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

//! Amazon Kinesis Data Streams is a managed service that scales elastically for
//! real-time processing of streaming big data.

use aws_lambda_events::event::kinesis::KinesisEvent;

use arrow::datatypes::SchemaRef;
use arrow::json::{self, reader::infer_json_schema};
use arrow::record_batch::RecordBatch;

use rayon::prelude::*;

use serde::{Deserialize, Serialize};
use std::io::BufReader;

/// A struct to manage all Kinesis info in cloud environment.
#[derive(Default, Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Kinesis {
    /// The shard ID of the Kinesis Data Streams shard to get the iterator for.
    pub shard_id:                 String,
    /// Determines how the shard iterator is used to start reading data records
    /// from the shard. The following are the valid Amazon Kinesis shard
    /// iterator types:
    /// - `SEQUENCE_NUMBER`: Start reading from the position denoted by a
    ///   specific sequence number, provided in the value
    ///   `StartingSequenceNumber`.
    /// - `AFTER_SEQUENCE_NUMBER`: Start reading right after the position
    ///   denoted by a specific sequence number, provided in the value
    ///   `StartingSequenceNumber`.
    /// - `TIMESTAMP`: Start reading from the position denoted by a specific
    ///   time stamp, provided in the value `Timestamp`.
    /// - `TRIM_HORIZON`: Start reading at the last untrimmed record in the
    ///   shard in the system, which is the oldest data record in the shard.
    /// - `LATEST`: Start reading just after the most recent record in the
    ///   shard, so that you always read the most recent data in the shard.
    pub shard_iterator_type:      String,
    /// The sequence number of the data record in the shard from which to start
    /// reading. Used with shard iterator type `AT_SEQUENCE_NUMBER` and
    /// `AFTER_SEQUENCE_NUMBER`.
    pub starting_sequence_number: Option<String>,
    /// The name of the Amazon Kinesis data stream.
    pub stream_name:              String,
    /// The time stamp of the data record from which to start reading. Used with
    /// shard iterator type `AT_TIMESTAMP`. A time stamp is the Unix epoch date
    /// with precision in milliseconds. For example,
    /// `2016-04-04T19:58:46.480-00:00` or `1459799926.480`. If a record with
    /// this exact time stamp does not exist, the iterator returned is for the
    /// next (later) record. If the time stamp is older than the current trim
    /// horizon, the iterator returned is for the oldest untrimmed data record
    /// (`TRIM_HORIZON`).
    pub timestamp:                Option<f64>,
}

/// Converts Kinesis event to record batch in Arrow.
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
