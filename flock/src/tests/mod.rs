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

//! Common unit test utility methods

use crate::datasource::DataSource;
use crate::runtime::plan;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::ExecutionPlan;
use fake::{Dummy, Fake};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

extern crate daggy;

/// The data record schema for unit tests.
#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct DataRecord {
    #[dummy(faker = "1..100")]
    pub c1: i64,
    #[dummy(faker = "1..100")]
    pub c2: i64,
    pub c3: String,
}

impl DataRecord {
    /// Return the schema of the data record.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Int64, false),
            Field::new("c3", DataType::Utf8, false),
        ]))
    }
}

/// Generate a random Kinesis event.
///
/// You can use an AWS Lambda function to process records in an Amazon Kinesis
/// data stream. A Kinesis data stream is a set of shards. Each shard contains a
/// sequence of data records. Before invoking the function, Lambda continues to
/// read records from the stream until it has gathered a full batch, or until
/// the batch window expires.
///
/// More details: <https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html>
///
/// # Arguments
///
/// * `num`: the number of records in the event.
///
/// # Return
///
/// A valid JSON [value](serde_json::Value) representing the Kinesis event, and
/// the schema of the data records.
///
/// # Example
///
/// Kinesis record event
///
/// ```json
/// {
///     "Records": [
///         {
///             "kinesis": {
///                 "kinesisSchemaVersion": "1.0",
///                 "partitionKey": "1",
///                 "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
///                 "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
///                 "approximateArrivalTimestamp": 1545084650.987
///             },
///             "eventSource": "aws:kinesis",
///             "eventVersion": "1.0",
///             "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
///             "eventName": "aws:kinesis:record",
///             "invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
///             "awsRegion": "us-east-2",
///             "eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/lambda-stream"
///         },
///         {
///             "kinesis": {
///                 "kinesisSchemaVersion": "1.0",
///                 "partitionKey": "1",
///                 "sequenceNumber": "49590338271490256608559692540925702759324208523137515618",
///                 "data": "VGhpcyBpcyBvbmx5IGEgdGVzdC4=",
///                 "approximateArrivalTimestamp": 1545084711.166
///             },
///             "eventSource": "aws:kinesis",
///             "eventVersion": "1.0",
///             "eventID": "shardId-000000000006:49590338271490256608559692540925702759324208523137515618",
///             "eventName": "aws:kinesis:record",
///             "invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
///             "awsRegion": "us-east-2",
///             "eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/lambda-stream"
///         }
///     ]
/// }
/// ```
pub fn random_kinesis_event(num: usize) -> (Value, SchemaRef) {
    (
        serde_json::to_value(kinesis::KinesisEvent {
            records: fake::vec![kinesis::KinesisEventRecord; num],
        })
        .unwrap(),
        DataRecord::schema(),
    )
}

/// Generate a random event for a given data source.
///
/// # Arguments
///
/// * `datasource`: A [data source](DataSource) type.
/// * `num`: the number of records in the event.
///
/// # Return
///
/// A valid JSON [value](serde_json::Value) representing the event, and
/// the schema of the data records in the event.
pub fn random_event(datasource: &DataSource, num: usize) -> (Value, SchemaRef) {
    match &datasource {
        DataSource::KinesisEvent(_) => random_kinesis_event(num),
        DataSource::KafkaEvent(_) => unimplemented!(),
        _ => unimplemented!(),
    }
}

/// Generate a random payload for unit tests.
///
/// # Arguments
///
/// * `rows`: the number of rows in each record batch (RecordBatch).
/// * `batch_nums`: the number of batches in each partition (vec![RecordBatch]).
/// * `partition_nums`: the number of partitions in each payload
///   (vec![vec![RecordBatch]).
///
/// # Return
///
/// A record batches that is formatted in vec![vec![RecordBatch]].
pub fn random_batches(
    rows: usize,
    batch_nums: usize,
    partition_nums: usize,
) -> Vec<Vec<RecordBatch>> {
    (0..partition_nums)
        .map(|_| {
            (0..batch_nums)
                .map(|_| {
                    RecordBatch::try_new(
                        DataRecord::schema(),
                        vec![
                            Arc::new(Int64Array::from(fake::vec![i64; rows])),
                            Arc::new(Int64Array::from(fake::vec![i64; rows])),
                            Arc::new(StringArray::from_iter_values(
                                fake::vec![String; rows].iter(),
                            )),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        })
        .collect()
}

/// Register a table
pub fn register_table(schema: &SchemaRef, table_name: &str) -> ExecutionContext {
    let mut ctx = ExecutionContext::new();

    // create empty batch to generate the execution plan
    let batch = RecordBatch::new_empty(schema.clone());
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();

    ctx
}

/// Generate a physical plan of a given query.
///
/// # Arguments
///
/// * `schema`: the data schema.
/// * `sql`: ANSI SQL statement.
/// * `table_name`: the table name the query works on.
///
/// # Return
///
/// `Arc<dyn ExecutionPlan>`: A physical execution plan.
pub async fn physical_plan(
    schema: &SchemaRef,
    sql: &str,
    table_name: &str,
) -> Arc<dyn ExecutionPlan> {
    let ctx = register_table(schema, table_name);
    plan::physical_plan(&ctx, sql).await.unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    async fn random_kinesis_data() -> Result<()> {
        for i in 1..5 {
            let (value, _) = random_kinesis_event(i);
            let event: aws_lambda_events::event::kinesis::KinesisEvent =
                serde_json::from_value(value)?;
            assert_eq!(event.records.len(), i);
        }
        Ok(())
    }

    #[tokio::test]
    async fn random_batches_data() -> Result<()> {
        let rows = 16;
        let batch_nums = 16;
        let partition_nums = 16;

        let batches = random_batches(rows, batch_nums, partition_nums);

        assert_eq!(partition_nums, batches.len());

        (0..partition_nums).for_each(|i| {
            assert_eq!(batches[i].len(), batch_nums);
            (0..batch_nums).for_each(|j| {
                assert_eq!(batches[i][j].num_rows(), rows);
            });
        });

        Ok(())
    }
}

pub mod kinesis;
pub mod nexmark;
