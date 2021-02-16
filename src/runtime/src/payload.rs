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

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch};
use arrow_flight::FlightData;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use text_io::scan;

/// A helper function to build UUIDs of a series of payloads for a given query.
#[derive(Default, Debug)]
pub struct UuidBuilder {
    /// The identifier of the query triggered at the specific time.
    pub tid: String,
    /// The sequence number which builder has assigned to the latest payload.
    pub pos: i64,
    /// The total sequence numbers after the data is fragmented to different
    /// payloads.
    pub len: i64,
}

impl UuidBuilder {
    /// Returns a new UuidBuilder.
    pub fn new(function_name: &str, len: i64) -> Self {
        let query_code: String;
        let plan_inedx: String;
        let timestamp: String;
        scan!(function_name.bytes() => "{}-{}-{}", query_code, plan_inedx, timestamp);
        Self {
            tid: format!("{}-{}", query_code, timestamp),
            pos: 0,
            len,
        }
    }

    /// Returns the next Uuid for the next payload.
    pub fn next(&mut self) -> Uuid {
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
}

/// Whether it is streaming or batch processing, each query has a unique
/// identifier to distinguish each other, so that the lambda function can
/// correctly separate and aggregate the results for distributed dataflow
/// computation.
#[derive(Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Uuid {
    /// The identifier of the query triggered at the specific time.
    ///
    /// # Note
    ///
    /// `tid` is also used as a random seed for choosing the next function
    /// (with concurrency = 1) through consistent hashing.
    pub tid:     String,
    /// The sequence number of the data contained in the payload.
    pub seq_num: i64,
    /// The total sequence numbers after the data is fragmented to different
    /// payloads.
    pub seq_len: i64,
}

/// `Payload` is the raw structure of the function's payload passed between
/// lambda functions. In AWS Lambda, it supports payload sizes up to 256KB for
/// async invocation. you can pass payloads in your query workflows, allowing
/// each lambda function to seamlessly perform related query operations.
#[derive(Debug, Deserialize, Serialize)]
pub struct Payload {
    /// Arrow Flight Data's header.
    #[serde(with = "serde_bytes")]
    header: Vec<u8>,
    /// Arrow Flight Data's body.
    #[serde(with = "serde_bytes")]
    body:   Vec<u8>,
    /// The subplan's schema.
    schema: Schema,
    /// The query's uuid.
    uuid:   Uuid,
}

impl Default for Payload {
    fn default() -> Payload {
        Self {
            header: vec![],
            body:   vec![],
            schema: Schema::empty(),
            uuid:   Uuid::default(),
        }
    }
}

impl Payload {
    /// Converts incoming payload to record batch in Arrow.
    pub fn to_batch(event: Value) -> (RecordBatch, Uuid) {
        let input: Payload = serde_json::from_value(event).unwrap();
        (
            flight_data_to_arrow_batch(
                &FlightData {
                    data_body:         input.body,
                    data_header:       input.header,
                    app_metadata:      vec![],
                    flight_descriptor: None,
                },
                Arc::new(input.schema),
                &[],
            )
            .unwrap(),
            input.uuid,
        )
    }

    /// Converts record batch to payload for network transmission.
    /// TODO: compression option
    pub fn from(batch: &RecordBatch, schema: SchemaRef, uuid: Uuid) -> Self {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let (_, flight_data) = flight_data_from_arrow_batch(batch, &options);
        Self {
            body: flight_data.data_body,
            header: flight_data.data_header,
            schema: (*schema).clone(),
            uuid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::Encoding;
    use arrow::array::{Array, StructArray};
    use arrow::csv;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::json;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn uuid_builder() {
        let function_name = "SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836";
        let payload_num = 10;

        let mut uuid_builder = UuidBuilder::new(function_name, payload_num);
        assert_eq!(
            "SX72HzqFz1Qij4bP-2021-01-28T19:27:50.298504836",
            uuid_builder.tid
        );

        for i in 0..payload_num {
            assert_eq!(
                uuid_builder.next(),
                Uuid {
                    tid:     "SX72HzqFz1Qij4bP-2021-01-28T19:27:50.298504836".to_owned(),
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

        let records: &[u8] = include_str!("datasource/data/uk_cities_with_headers.csv").as_bytes();
        let mut reader = csv::Reader::new(records, Arc::new(schema), true, None, 1024, None, None);
        let batch = reader.next().unwrap().unwrap();
        let struct_array: StructArray = batch.clone().into();

        assert_eq!(37, struct_array.len());
        // return the total number of bytes of memory occupied by the buffers owned by
        // this array.
        assert_eq!(2432, struct_array.get_buffer_memory_size());
        // return the total number of bytes of memory occupied physically by this array.
        assert_eq!(2768, struct_array.get_array_memory_size());

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let (_, flight_data) = flight_data_from_arrow_batch(&batch, &options);
        let flight_data_size = flight_data.data_header.len() + flight_data.data_body.len();
        assert_eq!(1856, flight_data_size);
    }

    #[test]
    fn flight_data_compression_ratio_2() {
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

        let records: &[u8] =
            include_str!("datasource/data/JC-202011-citibike-tripdata.csv").as_bytes();
        let mut reader = csv::Reader::new(records, schema, true, None, 21275, None, None);
        let batch = reader.next().unwrap().unwrap();

        // Option: Arrow RecordBatch
        let mut buf = Vec::new();
        {
            let mut writer = json::Writer::new(&mut buf);
            writer.write_batches(&[batch.clone()]).unwrap();
        }
        assert_eq!(9436023, buf.len());
        println!("Arrow RecordBatch data (Json writer): {}", buf.len());

        // Option: Arrow Struct Array
        let struct_array: StructArray = batch.clone().into();
        {
            assert_eq!(21275, struct_array.len());
            // return the total number of bytes of memory occupied by the buffers owned by
            // this array.
            assert_eq!(4659712, struct_array.get_buffer_memory_size());
            // return the total number of bytes of memory occupied physically by this array.
            assert_eq!(4661048, struct_array.get_array_memory_size());
            println!(
                "Arrow Struct Array data: {}",
                struct_array.get_array_memory_size()
            );
        }

        // Option: Arrow Flight Data
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let (_, flight_data) = flight_data_from_arrow_batch(&batch, &options);

        {
            let flight_data_size = flight_data.data_header.len() + flight_data.data_body.len();
            assert_eq!(3453248, flight_data_size);
            println!(
                "Raw Arrow Flight data: {}, encoding ratio: {:.3}",
                flight_data_size,
                buf.len() as f32 / flight_data_size as f32
            );
        }

        // Option: Compress Arrow Flight data
        {
            let now = Instant::now();
            for en in [Encoding::Snappy, Encoding::Lz4].iter() {
                let (en_header, en_body) = (
                    en.compress(&flight_data.data_header),
                    en.compress(&flight_data.data_body),
                );
                let en_flight_data_size = en_header.len() + en_body.len();
                println!("Compression time: {} ms", now.elapsed().as_millis());

                println!(
                    "Compressed Arrow Flight data: {}, type: {:?}, compression ratio: {:.3}",
                    en_flight_data_size,
                    en,
                    buf.len() as f32 / en_flight_data_size as f32
                );

                let now = Instant::now();
                let (de_header, de_body) = (en.decompress(&en_header), en.decompress(&en_body));
                println!("Decompression time: {} ms", now.elapsed().as_millis());

                assert_eq!(flight_data.data_header, de_header);
                assert_eq!(flight_data.data_body, de_body);
            }
        }
    }
}
