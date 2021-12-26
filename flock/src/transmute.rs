// Copyright (c) 2020-present, UMD Database Group.
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! This module contains various utility functions.

use crate::datasource::DataSource;
use crate::encoding::Encoding;
use crate::error::{FlockError, Result};
use crate::runtime::payload::{DataFrame, Payload, Uuid};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::json;
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::flight_data_from_arrow_batch;
use arrow_flight::FlightData;
use arrow_flight::SchemaAsIpc;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::stream::StreamExt;
use rayon::prelude::*;
use serde_json::Value;
use std::io::BufReader;
use std::sync::Arc;

/// Combines small batches into larger batches for more efficient use of
/// vectorized processing by upstream operators
pub async fn coalesce_batches(
    input_partitions: Vec<Vec<RecordBatch>>,
    target_batch_size: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    // create physical plan
    let exec = MemoryExec::try_new(&input_partitions, input_partitions[0][0].schema(), None)?;
    let exec: Arc<dyn ExecutionPlan> =
        Arc::new(CoalesceBatchesExec::new(Arc::new(exec), target_batch_size));

    // execute and collect results
    let output_partition_count = exec.output_partitioning().partition_count();
    let mut output_partitions = Vec::with_capacity(output_partition_count);
    for i in 0..output_partition_count {
        // execute this *output* partition and collect all batches
        let mut stream = exec.execute(i).await?;
        let mut batches = vec![];
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }
        output_partitions.push(batches);
    }
    Ok(output_partitions)
}

/// Maps N input partitions to M output partitions based on a
/// partitioning scheme. No guarantees are made about the order of the
/// resulting partitions.
pub async fn repartition(
    input_partitions: Vec<Vec<RecordBatch>>,
    partitioning: Partitioning,
) -> Result<Vec<Vec<RecordBatch>>> {
    // create physical plan
    let exec = MemoryExec::try_new(&input_partitions, input_partitions[0][0].schema(), None)?;
    let exec = RepartitionExec::try_new(Arc::new(exec), partitioning)?;

    // execute and collect results
    let mut output_partitions = vec![];
    for i in 0..exec.partitioning().partition_count() {
        // execute this *output* partition and collect all batches
        let mut stream = exec.execute(i).await?;
        let mut batches = vec![];
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }
        output_partitions.push(batches);
    }
    Ok(output_partitions)
}

/// Deserialize `DataFrame` from cloud functions.
pub fn unmarshal(data: Vec<DataFrame>, encoding: Encoding) -> Vec<DataFrame> {
    match encoding {
        Encoding::Snappy | Encoding::Lz4 | Encoding::Zstd => data
            .par_iter()
            .map(|d| DataFrame {
                header: encoding.decompress(&d.header).unwrap(),
                body:   encoding.decompress(&d.body).unwrap(),
            })
            .collect(),
        Encoding::None => data,
        _ => unimplemented!(),
    }
}

/// Serialize the schema
pub fn schema_to_bytes(schema: SchemaRef) -> Vec<u8> {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();
    flight_data.data_header
}

/// Deserialize the schema
pub fn schema_from_bytes(bytes: &[u8]) -> Result<Arc<Schema>> {
    let schema = arrow::ipc::convert::schema_from_bytes(bytes).map_err(FlockError::Arrow)?;
    Ok(Arc::new(schema))
}

/// Convert incoming payload to record batches in Arrow format.
pub fn json_value_to_batch(event: Value) -> (Vec<RecordBatch>, Vec<RecordBatch>, Uuid) {
    let payload: Payload = serde_json::from_value(event).unwrap();
    payload.to_record_batch()
}

/// Convert record batches to payload for network transmission.
pub fn batch_to_json_value(batches: &[RecordBatch], uuid: Uuid, encoding: Encoding) -> Value {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let data_frames = batches
        .par_iter()
        .map(|b| {
            let (_, flight_data) = flight_data_from_arrow_batch(b, &options);
            if encoding != Encoding::None {
                DataFrame {
                    header: encoding.compress(&flight_data.data_header).unwrap(),
                    body:   encoding.compress(&flight_data.data_body).unwrap(),
                }
            } else {
                DataFrame {
                    header: flight_data.data_header,
                    body:   flight_data.data_body,
                }
            }
        })
        .collect();

    serde_json::to_value(&Payload {
        data: data_frames,
        schema: schema_to_bytes(batches[0].schema()),
        uuid,
        encoding,
        ..Default::default()
    })
    .unwrap()
}

/// Convert record batches to payload using the default encoding.
pub fn to_payload(
    batch1: &[RecordBatch],
    batch2: &[RecordBatch],
    uuid: Uuid,
    sync: bool,
) -> Payload {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let encoding = Encoding::default();
    let dataframe = |batches: &[RecordBatch]| -> Vec<DataFrame> {
        batches
            .par_iter()
            .map(|b| {
                let (_, flight_data) = flight_data_from_arrow_batch(b, &options);
                if encoding != Encoding::None {
                    DataFrame {
                        header: encoding.compress(&flight_data.data_header).unwrap(),
                        body:   encoding.compress(&flight_data.data_body).unwrap(),
                    }
                } else {
                    DataFrame {
                        header: flight_data.data_header,
                        body:   flight_data.data_body,
                    }
                }
            })
            .collect()
    };

    let mut payload = Payload {
        uuid,
        encoding: encoding.clone(),
        datasource: DataSource::Payload(sync),
        ..Default::default()
    };
    if !batch1.is_empty() {
        payload.data = dataframe(batch1);
        payload.schema = schema_to_bytes(batch1[0].schema());
    }
    if !batch2.is_empty() {
        payload.data2 = dataframe(batch2);
        payload.schema2 = schema_to_bytes(batch2[0].schema());
    }
    payload
}

/// Convert record batch to bytes for network transmission.
pub fn to_bytes(batch: &RecordBatch, uuid: Uuid, encoding: Encoding) -> bytes::Bytes {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let schema = schema_to_bytes(batch.schema());
    let (_, flight_data) = flight_data_from_arrow_batch(batch, &options);

    let data_frames = {
        if encoding != Encoding::None {
            DataFrame {
                header: encoding.compress(&flight_data.data_header).unwrap(),
                body:   encoding.compress(&flight_data.data_body).unwrap(),
            }
        } else {
            DataFrame {
                header: flight_data.data_header,
                body:   flight_data.data_body,
            }
        }
    };

    serde_json::to_vec(&Payload {
        data: vec![data_frames],
        schema,
        uuid,
        encoding,
        ..Default::default()
    })
    .unwrap()
    .into()
}

/// Converts events to record batches in Arrow format.
pub fn event_bytes_to_batch(
    events: &[u8],
    schema: SchemaRef,
    batch_size: usize,
) -> Vec<RecordBatch> {
    let mut reader = json::Reader::new(BufReader::new(events), schema, batch_size, None);
    let mut batches = vec![];
    while let Some(batch) = reader.next().unwrap() {
        batches.push(batch);
    }
    batches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FlockError;
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::expressions::col;
    use tokio::task::JoinHandle;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn create_vec_batches(schema: &Arc<Schema>, num_batches: usize) -> Vec<RecordBatch> {
        let batch = create_batch(schema);
        let mut vec = Vec::with_capacity(num_batches);
        for _ in 0..num_batches {
            vec.push(batch.clone());
        }
        vec
    }

    fn create_batch(schema: &Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_concat_batches() -> Result<()> {
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 10);
        let partitions = vec![partition];

        let output_partitions = coalesce_batches(partitions, 20).await?;
        assert_eq!(1, output_partitions.len());

        // input is 10 batches x 8 rows (80 rows)
        // expected output is batches of at least 20 rows (except for the final batch)
        let batches = &output_partitions[0];
        assert_eq!(4, batches.len());
        assert_eq!(24, batches[0].num_rows());
        assert_eq!(24, batches[1].num_rows());
        assert_eq!(24, batches[2].num_rows());
        assert_eq!(8, batches[3].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn one_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition];

        // repartition from 1 input to 4 output
        let output_partitions = repartition(partitions, Partitioning::RoundRobinBatch(4)).await?;

        assert_eq!(4, output_partitions.len());
        assert_eq!(13, output_partitions[0].len());
        assert_eq!(13, output_partitions[1].len());
        assert_eq!(12, output_partitions[2].len());
        assert_eq!(12, output_partitions[3].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_one_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 1 output
        let output_partitions = repartition(partitions, Partitioning::RoundRobinBatch(1)).await?;

        assert_eq!(1, output_partitions.len());
        assert_eq!(150, output_partitions[0].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 5 output
        let output_partitions = repartition(partitions, Partitioning::RoundRobinBatch(5)).await?;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_hash_partition() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        let output_partitions =
            repartition(partitions, Partitioning::Hash(vec![col("c0", &schema)?], 8)).await?;

        let total_rows: usize = output_partitions
            .iter()
            .map(|x| x.iter().map(|x| x.num_rows()).sum::<usize>())
            .sum();

        assert_eq!(8, output_partitions.len());
        assert_eq!(total_rows, 8 * 50 * 3);

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_round_robin_within_tokio_task() -> Result<()> {
        let join_handle: JoinHandle<Result<Vec<Vec<RecordBatch>>>> = tokio::spawn(async move {
            // define input partitions
            let schema = test_schema();
            let partition = create_vec_batches(&schema, 50);
            let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

            // repartition from 3 input to 5 output
            repartition(partitions, Partitioning::RoundRobinBatch(5)).await
        });

        let output_partitions = join_handle
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))??;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }
}
