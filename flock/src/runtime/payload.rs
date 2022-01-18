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

//! This module contains the [`Payload`] type, which is used to package up
//! the intermediate results and metadata into a single object that can be
//! passed to the next cloud function. [`Uuid`] represents a unique identifier
//! of the payload for a given query at the specified time.

use crate::datasource::DataSource;
use crate::encoding::Encoding;
use crate::transmute::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow_flight::utils::flight_data_to_arrow_batch;
use datafusion::arrow_flight::FlightData;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid as RandomId;

/// A helper struct for building uuids of payloads.
#[derive(Default, Debug, Clone)]
pub struct UuidBuilder {
    /// The identifier of the data fragment or the payload.
    pub tid: String,
    /// The data fragment index in the time window.
    pub pos: usize,
    /// The total number of data fragments in the time window.
    pub len: usize,
}

impl UuidBuilder {
    /// Returns a new UuidBuilder.
    pub fn new_with_ts(function_name: &str, timestamp: i64, len: usize) -> Self {
        let query_code = function_name.split('-').next().unwrap();
        Self {
            tid: format!(
                "{}-{}-{}",
                query_code,
                timestamp,
                RandomId::new_v4().as_u128()
            ),
            pos: 0,
            len,
        }
    }

    /// Returns a new UuidBuilder.
    pub fn new_with_ts_uuid(function_name: &str, timestamp: i64, uuid: u128, len: usize) -> Self {
        let query_code = function_name.split('-').next().unwrap();
        Self {
            tid: format!("{}-{}-{}", query_code, timestamp, uuid),
            pos: 0,
            len,
        }
    }

    /// Returns the next Uuid for the next payload.
    pub fn next_uuid(&mut self) -> Uuid {
        assert!(self.pos < self.len);

        let tid = self.tid.to_owned();
        let seq_num = self.pos;
        let seq_len = self.len;

        self.pos += 1;

        Uuid {
            tid,
            seq_num,
            seq_len,
        }
    }

    /// Returns the Uuid with a specific index.
    pub fn get(&self, i: usize) -> Uuid {
        assert!(i < self.len);

        let tid = self.tid.to_owned();
        let seq_num = i;
        let seq_len = self.len;

        Uuid {
            tid,
            seq_num,
            seq_len,
        }
    }
}

/// The unique identifier of the payload or the data fragment in the time
/// window.
#[derive(Default, Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Uuid {
    /// The identifier of the query triggered at the specific time.
    ///
    /// # Note
    /// * [`Uuid::tid`] is also used to pick the next function to execute using
    ///   consistent hashing.
    pub tid:     String,
    /// `seq_num` represents the position of the data fragment in the time
    /// window, starting from 0, which will be recorded in the
    /// `[WindowSession::bitmap]`.
    pub seq_num: usize,
    /// `seq_len` represents the total number of fragments after the data is
    /// fragmented into different payloads.
    pub seq_len: usize,
}

/// `DataFrame` is a wrapper of the Arrow Flight Data format for network
/// transmission.
#[derive(Default, Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DataFrame {
    /// Arrow Flight Data's header.
    #[serde(with = "serde_bytes")]
    pub header: Vec<u8>,
    /// Arrow Flight Data's body.
    #[serde(with = "serde_bytes")]
    pub body:   Vec<u8>,
}

/// `Payload` is the wire format of the function's payload passed between
/// cloud functions.
#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq)]
pub struct Payload {
    /// The record batches are encoded in the Arrow Flight Data format.
    pub data:         Vec<DataFrame>,
    /// The schema of the record batches in binary format.
    pub schema:       Vec<u8>,
    /// The record batches for the 2nd relation.
    pub data2:        Vec<DataFrame>,
    /// The schema of the record batches for the 2nd relation.
    pub schema2:      Vec<u8>,
    /// The UUID of the payload.
    pub uuid:         Uuid,
    /// The encoding and compression method.
    /// Note: using this value to guarantee the total size of payload doesn't
    /// exceed 256 KB due to the limitation of AWS Lambda's async invocation.
    pub encoding:     Encoding,
    /// Where the payload is coming from.
    pub datasource:   DataSource,
    /// The Nexmark query number for the benchmarking purposes.
    pub query_number: Option<usize>,
    /// The extra metadata for the payload.
    pub metadata:     Option<HashMap<String, String>>,
}

impl Payload {
    /// Convert incoming payload to record batch in Arrow.
    pub fn to_record_batch(self) -> (Vec<RecordBatch>, Vec<RecordBatch>, Uuid) {
        let record_batch = |df: Vec<DataFrame>, schema: Arc<Schema>| -> Vec<RecordBatch> {
            df.into_par_iter()
                .map(|d| {
                    flight_data_to_arrow_batch(
                        &FlightData {
                            data_body:         d.body,
                            data_header:       d.header,
                            app_metadata:      vec![],
                            flight_descriptor: None,
                        },
                        schema.clone(),
                        &[],
                    )
                    .unwrap()
                })
                .collect()
        };

        let mut res = (vec![], vec![], self.uuid.clone());
        if !self.data.is_empty() {
            let dataframe = unmarshal(self.data, self.encoding.clone());
            let schema = schema_from_bytes(&self.schema).unwrap();
            res.0 = record_batch(dataframe, schema);
        }
        if !self.data2.is_empty() {
            let dataframe = unmarshal(self.data2, self.encoding.clone());
            let schema = schema_from_bytes(&self.schema2).unwrap();
            res.1 = record_batch(dataframe, schema);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use datafusion::arrow::array::{Array, StructArray};
    use datafusion::arrow::csv;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::json;
    use datafusion::arrow_flight::utils::flight_data_from_arrow_batch;
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn uuid_builder() {
        let function_name = "SX72HzqFz1Qij4bP-00-01";
        let timestamp: i64 = 1;
        let uuid: u128 = 2;
        let payload_num = 10;

        let mut uuid_builder =
            UuidBuilder::new_with_ts_uuid(function_name, timestamp, uuid, payload_num);
        assert_eq!(
            format!("SX72HzqFz1Qij4bP-{}-{}", timestamp, uuid),
            uuid_builder.tid
        );

        for i in 0..payload_num {
            assert_eq!(
                uuid_builder.next_uuid(),
                Uuid {
                    tid:     format!("SX72HzqFz1Qij4bP-{}-{}", timestamp, uuid),
                    seq_num: i,
                    seq_len: payload_num,
                }
            );
        }
    }

    #[test]
    fn flight_data_compression_ratio_1() {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let records: &[u8] = include_str!("../tests/data/uk_cities_with_headers.csv").as_bytes();
        let mut reader = csv::Reader::new(records, Arc::new(schema), true, None, 1024, None, None);
        let batch = reader.next().unwrap().unwrap();
        let struct_array: StructArray = batch.clone().into();

        assert_eq!(37, struct_array.len());
        // return the total number of bytes of memory occupied by the buffers owned by
        // this array.
        println!(
            "buffer memory size: {}",
            struct_array.get_buffer_memory_size()
        );
        // return the total number of bytes of memory occupied physically by this array.
        println!(
            "physical memory size: {}",
            struct_array.get_array_memory_size()
        );

        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let (_, flight_data) = flight_data_from_arrow_batch(&batch, &options);
        let flight_data_size = flight_data.data_header.len() + flight_data.data_body.len();
        assert_eq!(1856, flight_data_size);
    }

    fn init_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tripduration", DataType::Utf8, false),
            Field::new("starttime", DataType::Utf8, false),
            Field::new("stoptime", DataType::Utf8, false),
            Field::new("start station id", DataType::Int32, false),
            Field::new("start station name", DataType::Utf8, false),
            Field::new("start station latitude", DataType::Float64, false),
            Field::new("start station longitude", DataType::Float64, false),
            Field::new("end station id", DataType::Int32, false),
            Field::new("end station name", DataType::Utf8, false),
            Field::new("end station latitude", DataType::Float64, false),
            Field::new("end station longitude", DataType::Float64, false),
            Field::new("bikeid", DataType::Int32, false),
            Field::new("usertype", DataType::Utf8, false),
            Field::new("birth year", DataType::Int32, false),
            Field::new("gender", DataType::Int8, false),
        ]));

        let batch_size = 21275;
        let records: &[u8] =
            include_str!("../tests/data/JC-202011-citibike-tripdata.csv").as_bytes();
        let mut reader = csv::Reader::new(records, schema, true, None, batch_size, None, None);

        let mut batches = vec![];
        while let Some(Ok(batch)) = reader.next() {
            batches.push(batch);
        }
        batches
    }

    #[tokio::test]
    async fn flight_data_compression_ratio_2() -> Result<()> {
        let batches = init_batches();

        // Arrow RecordBatch (in-memory)
        let size: usize = batches
            .par_iter()
            .map(|batch| {
                batch
                    .columns()
                    .par_iter()
                    .map(|a| a.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum();
        println!("Arrow RecordBatch data (in-memory): {}", size);

        let batches = coalesce_batches(vec![batches], size).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].len());

        let batch = &batches[0][0];

        // Option: Write the record batch out as JSON
        let buf = Vec::new();
        let mut writer = json::LineDelimitedWriter::new(buf);
        writer.write_batches(&[batch.clone()]).unwrap();
        writer.finish().unwrap();
        let buf = writer.into_inner();
        assert_eq!(9436023, buf.len());
        println!("Arrow RecordBatch data (Json writer): {}", buf.len());

        // Option: Arrow Struct Array
        let struct_array: StructArray = batch.clone().into();
        {
            assert_eq!(21275, struct_array.len());
            // return the total number of bytes of memory occupied by the buffers owned by
            // this array.
            println!(
                "buffer memory size: {}",
                struct_array.get_buffer_memory_size()
            );
            // return the total number of bytes of memory occupied physically by this array.
            println!(
                "physical memory size: {}",
                struct_array.get_array_memory_size()
            );
            println!(
                "Arrow Struct Array data: {}",
                struct_array.get_array_memory_size()
            );
        }

        // Option: Arrow Flight Data
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let (_, flight_data) = flight_data_from_arrow_batch(batch, &options);

        {
            let flight_data_size = flight_data.data_header.len() + flight_data.data_body.len();
            assert_eq!(3453248, flight_data_size);
            println!(
                "Raw Arrow Flight data: {}, encoding ratio: {:.3}",
                flight_data_size,
                size as f32 / flight_data_size as f32
            );
        }

        // Option: Compress Arrow Flight data
        {
            for en in [Encoding::Snappy, Encoding::Lz4, Encoding::Zstd].iter() {
                let now = Instant::now();
                let (en_header, en_body) = (
                    en.compress(&flight_data.data_header)?,
                    en.compress(&flight_data.data_body)?,
                );
                let en_flight_data_size = en_header.len() + en_body.len();
                println!("Compression time: {} ms", now.elapsed().as_millis());

                println!(
                    "Compressed Arrow Flight data: {}, type: {:?}, compression ratio: {:.3}",
                    en_flight_data_size,
                    en,
                    size as f32 / en_flight_data_size as f32
                );

                let now = Instant::now();
                let (de_header, de_body) = (en.decompress(&en_header)?, en.decompress(&en_body)?);
                println!("Decompression time: {} ms", now.elapsed().as_millis());

                assert_eq!(flight_data.data_header, de_header);
                assert_eq!(flight_data.data_body, de_body);
            }
        }

        Ok(())
    }

    // Convert record batch to payload for network transmission.
    fn to_vec(batches: &[RecordBatch], uuid: Uuid, encoding: Encoding) -> Vec<u8> {
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
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

        serde_json::to_vec(&Payload {
            data: data_frames,
            schema: schema_to_bytes(batches[0].schema()),
            uuid,
            encoding,
            ..Default::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn serde_payload() -> Result<()> {
        let batches = init_batches();
        let mut uuid_builder = UuidBuilder::new_with_ts("SX72HzqFz1Qij4bP-00-01", 1, 10);
        let uuid = uuid_builder.next_uuid();

        let now = Instant::now();
        let value = batch_to_json_value(&batches, uuid.clone(), Encoding::default());
        println!(
            "serde payload to value (with compression) - time: {} ms",
            now.elapsed().as_millis()
        );

        let payload1: Payload = serde_json::from_value(value.clone())?;
        let now = Instant::now();
        let (de_batches, _, de_uuid) = json_value_to_batch(value);
        println!(
            "serde value to batch (with decompression) - time: {} ms",
            now.elapsed().as_millis()
        );

        {
            assert_eq!(batches.len(), de_batches.len());
            assert_eq!(batches[0].schema(), de_batches[0].schema());
            assert_eq!(batches[0].num_columns(), de_batches[0].num_columns());
            assert_eq!(batches[0].num_rows(), de_batches[0].num_rows());
            assert_eq!(batches[0].columns(), de_batches[0].columns());
            assert_eq!(uuid, de_uuid);
        }

        let now = Instant::now();
        let bytes = to_vec(&batches, uuid, Encoding::default());
        println!(
            "serde payload to bytes (with compression) - time: {} ms, size: {} bytes",
            now.elapsed().as_millis(),
            bytes.len()
        );

        let payload2: Payload = serde_json::from_slice(&bytes)?;
        assert_eq!(payload1, payload2);

        let now = Instant::now();
        let bytes = Encoding::Zstd.compress(&bytes)?;
        println!(
            "compress json bytes - time: {} ms, size: {} bytes",
            now.elapsed().as_millis(),
            bytes.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn schema_ipc() -> Result<()> {
        let batches = init_batches();
        let schema1 = batches[0].schema();

        // serilization
        let bytes = schema_to_bytes(schema1.clone());

        // deserialization
        let schema2 = schema_from_bytes(&bytes)?;

        assert_eq!(schema1, schema2);

        Ok(())
    }

    #[tokio::test]
    async fn uuid() -> Result<()> {
        let mut uuid_builder =
            UuidBuilder::new_with_ts("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836", 1, 10);
        (0..10).for_each(|i| assert_eq!(uuid_builder.get(i).seq_num, i));

        let batches = init_batches();
        let bytes = to_bytes(&batches[0], uuid_builder.next_uuid(), Encoding::default());
        let value: Value = serde_json::from_slice(&bytes)?;
        let (de_batches, _, _) = json_value_to_batch(value);

        assert_eq!(batches[0].schema(), de_batches[0].schema());
        assert_eq!(batches[0].columns(), de_batches[0].columns());

        Ok(())
    }
}
