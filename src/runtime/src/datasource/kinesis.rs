// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
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

//! Payload API for building and executing query plans in cloud function
//! services.

use aws_lambda_events::event::kinesis::KinesisEvent;

use arrow::datatypes::SchemaRef;
use arrow::json;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;

use rayon::prelude::*;

use std::io::BufReader;

/// Convert Kinesis event to record batch in Arrow.
pub fn to_batch(event: KinesisEvent) -> (RecordBatch, SchemaRef) {
    // infer schema based on the first record
    let record: &[u8] = &event.records[0].kinesis.data.0.clone();
    let mut reader = BufReader::new(record);
    let schema = infer_json_schema(&mut reader, Some(1)).unwrap();

    // get all data from Kinesis event
    let batch_size = event.records.len();
    let input: &[u8] = &event
        .records
        .into_par_iter()
        .flat_map(|r| r.kinesis.data.0)
        .collect::<Vec<u8>>();

    // transform data to record batch in Arrow
    reader = BufReader::with_capacity(input.len(), input);
    let mut json = json::Reader::new(reader, schema.clone(), batch_size, None);
    (json.next().unwrap().unwrap(), schema)
}
