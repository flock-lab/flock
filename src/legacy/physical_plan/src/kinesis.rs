// Copyright (c) 2020 UMD Database Group. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::datatypes::{Schema, SchemaRef};
use arrow::json::reader::infer_json_schema;
use bytes::Buf;
use common::error::{Result, ServerlessCQError};
use futures::executor::block_on;
use rusoto_core::Region;
use rusoto_kinesis::{GetRecordsInput, GetShardIteratorInput, Kinesis, KinesisClient};
use std::io::{BufReader, Read};
use std::sync::Arc;

/// Execution plan for scanning a Kinesis stream
#[derive(Debug, Clone)]
pub struct KinesisStreamExec {
    /// The name of the Amazon Kinesis data stream
    name:       String,
    /// The shard ID of the Kinesis Data Streams shard to get the iterator for
    shard_id:   String,
    /// Schema after projection is applied
    schema:     SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
}

#[allow(unused_variables)]
#[allow(dead_code)]
impl KinesisStreamExec {
    /// Create a new Kinesis stream reader execution plan
    pub fn try_new(
        stream_name: &str,
        shard_id: Option<&str>,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let stream_shard_id = match shard_id {
            Some(id) => id,
            None => "shardId-000000000000",
        };

        let iter_input = GetShardIteratorInput {
            shard_id:                 stream_shard_id.to_string(),
            stream_name:              stream_name.to_string(),
            shard_iterator_type:      "TRIM_HORIZON".to_string(),
            starting_sequence_number: None,
            timestamp:                None,
        };

        // Creates a client backed by the default tokio event loop.
        let kinesis_client = KinesisClient::new(Region::default());
        // Gets an Amazon Kinesis shard iterator.
        let shard_iter = block_on(kinesis_client.get_shard_iterator(iter_input))
            .unwrap()
            .shard_iterator
            .unwrap();

        let records_input = GetRecordsInput {
            limit:          None,
            shard_iterator: shard_iter,
        };

        // Gets data records from a Kinesis data stream's shard.
        match block_on(kinesis_client.get_records(records_input)) {
            Ok(stream) => {
                let mut buffer = String::new();
                stream
                    .records
                    .last()
                    .unwrap()
                    .data
                    .reader()
                    .read_to_string(&mut buffer)
                    .unwrap();

                let schema =
                    infer_json_schema(&mut BufReader::new(buffer.as_bytes()), None).unwrap();

                Ok(Self::new(
                    stream_name.to_string(),
                    stream_shard_id.to_string(),
                    schema,
                    projection,
                    batch_size,
                ))
            }
            Err(e) => Err(ServerlessCQError::Plan(
                "No stream record found".to_string(),
            )),
        }
    }

    /// Create a new Kinesis stream execution plan with provided schema
    pub fn new(
        stream_name: String,
        shard_id: String,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        Self {
            name: stream_name,
            shard_id,
            schema: Arc::new(projected_schema),
            projection,
            batch_size,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
