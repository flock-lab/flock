// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
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

use crate::encoding::Encoding;
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
    pub pos: usize,
    /// The total sequence numbers after the data is fragmented to different
    /// payloads.
    pub len: usize,
}

impl UuidBuilder {
    /// Returns a new UuidBuilder.
    pub fn new(function_name: &str, len: usize) -> Self {
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
#[derive(Default, Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Uuid {
    /// The identifier of the query triggered at the specific time.
    ///
    /// # Note
    ///
    /// `tid` is also used as a random seed for choosing the next function
    /// (with concurrency = 1) through consistent hashing.
    pub tid:     String,
    /// The sequence number of the data contained in the payload.
    pub seq_num: usize,
    /// The total sequence numbers after the data is fragmented to different
    /// payloads.
    pub seq_len: usize,
}

/// Arrow Flight Data format
#[derive(Default, Debug, Deserialize, Serialize)]
pub struct DataFrame {
    /// Arrow Flight Data's header.
    #[serde(with = "serde_bytes")]
    header: Vec<u8>,
    /// Arrow Flight Data's body.
    #[serde(with = "serde_bytes")]
    body:   Vec<u8>,
}

/// `Payload` is the raw structure of the function's payload passed between
/// lambda functions. In AWS Lambda, it supports payload sizes up to 256KB for
/// async invocation. You can pass payloads in your query workflows, allowing
/// each lambda function to seamlessly perform related query operations.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Payload {
    /// The data batches in the payload.
    #[serde(with = "serde_bytes")]
    pub data:     Vec<u8>,
    /// The subplan's schema.
    schema:       SchemaRef,
    /// The query's uuid.
    pub uuid:     Uuid,
    /// Compress `DataFrame` to guarantee the total size
    /// of payload doesn't exceed 256 KB.
    pub encoding: Encoding,
}

impl Default for Payload {
    fn default() -> Payload {
        Self {
            data:     vec![],
            schema:   Arc::new(Schema::empty()),
            uuid:     Uuid::default(),
            encoding: Encoding::default(),
        }
    }
}

impl Payload {
    /// Convert incoming payload to record batch in Arrow.
    pub fn to_batch(event: Value) -> (Vec<RecordBatch>, Uuid) {
        let payload: Payload = serde_json::from_value(event).unwrap();
        let uuid = payload.uuid.clone();
        let mut data_frames = unmarshal(&payload);
        let nums = data_frames.len();
        (
            (0..nums)
                .map(|_| {
                    let data = data_frames.pop().unwrap();
                    flight_data_to_arrow_batch(
                        &FlightData {
                            data_body:         data.body,
                            data_header:       data.header,
                            app_metadata:      vec![],
                            flight_descriptor: None,
                        },
                        payload.schema.clone(),
                        &[],
                    )
                    .unwrap()
                })
                .collect(),
            uuid,
        )
    }

    /// Convert record batch to payload for network transmission.
    pub fn to_value(batches: &[RecordBatch], uuid: Uuid) -> Value {
        let options = arrow::ipc::writer::IpcWriteOptions::default();

        let data_frames = (0..batches.len())
            .map(|i| {
                let (_, flight_data) = flight_data_from_arrow_batch(&batches[i], &options);
                DataFrame {
                    header: flight_data.data_header,
                    body:   flight_data.data_body,
                }
            })
            .collect();

        marshal2value(&data_frames, batches[0].schema(), uuid, Encoding::default())
    }

    /// Convert record batch to payload for network transmission.
    pub fn to_bytes(batches: &[RecordBatch], uuid: Uuid) -> bytes::Bytes {
        let options = arrow::ipc::writer::IpcWriteOptions::default();

        let data_frames = (0..batches.len())
            .map(|i| {
                let (_, flight_data) = flight_data_from_arrow_batch(&batches[i], &options);
                DataFrame {
                    header: flight_data.data_header,
                    body:   flight_data.data_body,
                }
            })
            .collect();

        marshal2bytes(&data_frames, batches[0].schema(), uuid, Encoding::default())
    }
}

/// Serialize `Payload` in cloud functions.
pub fn marshal2value(
    data: &Vec<DataFrame>,
    schema: SchemaRef,
    uuid: Uuid,
    encoding: Encoding,
) -> Value {
    match encoding {
        Encoding::Snappy | Encoding::Lz4 | Encoding::Zstd => {
            let encoded: Vec<u8> = serde_json::to_vec(&data).unwrap();
            serde_json::to_value(&Payload {
                data: encoding.compress(&encoded),
                schema,
                uuid,
                encoding,
            })
            .unwrap()
        }
        Encoding::None => serde_json::to_value(&Payload {
            data: serde_json::to_vec(&data).unwrap(),
            schema,
            uuid,
            encoding,
        })
        .unwrap(),
        _ => unimplemented!(),
    }
}

/// Serialize `Payload` in cloud functions.
pub fn marshal2bytes(
    data: &Vec<DataFrame>,
    schema: SchemaRef,
    uuid: Uuid,
    encoding: Encoding,
) -> bytes::Bytes {
    match encoding {
        Encoding::Snappy | Encoding::Lz4 | Encoding::Zstd => {
            let encoded: Vec<u8> = serde_json::to_vec(&data).unwrap();
            serde_json::to_vec(&Payload {
                data: encoding.compress(&encoded),
                schema,
                uuid,
                encoding,
            })
            .unwrap()
            .into()
        }
        Encoding::None => serde_json::to_vec(&Payload {
            data: serde_json::to_vec(&data).unwrap(),
            schema,
            uuid,
            encoding,
        })
        .unwrap()
        .into(),
        _ => unimplemented!(),
    }
}

/// Deserialize `DataFrame` from cloud functions.
pub fn unmarshal(payload: &Payload) -> Vec<DataFrame> {
    match payload.encoding {
        Encoding::Snappy | Encoding::Lz4 | Encoding::Zstd => {
            let encoded = payload.encoding.decompress(&payload.data);
            serde_json::from_slice(&encoded).unwrap()
        }
        Encoding::None => serde_json::from_slice(&payload.data).unwrap(),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use arrow::array::{Array, StructArray};
    use arrow::csv;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::json;
    use std::mem;
    use std::slice;
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

        let records: &[u8] = include_str!("../../test/data/uk_cities_with_headers.csv").as_bytes();
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

    fn init_batches() -> RecordBatch {
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
            include_str!("../../test/data/JC-202011-citibike-tripdata.csv").as_bytes();
        let mut reader = csv::Reader::new(records, schema, true, None, 21275, None, None);
        reader.next().unwrap().unwrap()
    }

    #[test]
    fn flight_data_compression_ratio_2() {
        let batch = init_batches();

        // Arrow RecordBatch (in-memory)
        let size: usize = batch
            .columns()
            .iter()
            .map(|a| a.get_array_memory_size())
            .sum();
        assert_eq!(4661248, size);
        println!("Arrow RecordBatch data (in-memory): {}", size);

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
                size as f32 / flight_data_size as f32
            );
        }

        // Option: Compress Arrow Flight data
        {
            for en in [Encoding::Snappy, Encoding::Lz4, Encoding::Zstd].iter() {
                let now = Instant::now();
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
                    size as f32 / en_flight_data_size as f32
                );

                let now = Instant::now();
                let (de_header, de_body) = (en.decompress(&en_header), en.decompress(&en_body));
                println!("Decompression time: {} ms", now.elapsed().as_millis());

                assert_eq!(flight_data.data_header, de_header);
                assert_eq!(flight_data.data_body, de_body);
            }
        }
    }

    #[tokio::test]
    async fn serde_payload() -> Result<()> {
        let batches = vec![init_batches()];
        let mut uuid_builder =
            UuidBuilder::new("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836", 10);
        let uuid = uuid_builder.next();

        let value = Payload::to_value(&batches, uuid.clone());
        let payload1: Payload = serde_json::from_value(value.clone())?;
        let (de_batches, de_uuid) = Payload::to_batch(value);
        {
            assert_eq!(batches.len(), de_batches.len());
            assert_eq!(batches[0].schema(), de_batches[0].schema());
            assert_eq!(batches[0].num_columns(), de_batches[0].num_columns());
            assert_eq!(batches[0].num_rows(), de_batches[0].num_rows());
            assert_eq!(batches[0].columns(), de_batches[0].columns());
            assert_eq!(uuid, de_uuid);
        }

        let bytes = Payload::to_bytes(&batches, uuid);
        let payload2: Payload = serde_json::from_slice(&bytes)?;
        assert_eq!(payload1, payload2);

        Ok(())
    }

    #[tokio::test]
    async fn transmute_data_frames() -> Result<()> {
        #[repr(packed)]
        pub struct DataFrameStruct {
            /// Arrow Flight Data's header.
            header: Vec<u8>,
            /// Arrow Flight Data's body.
            body:   Vec<u8>,
        }

        let batch = init_batches();
        let schema = batch.schema();
        let batches = vec![batch.clone(), batch.clone(), batch];
        let mut uuid_builder =
            UuidBuilder::new("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836", 10);
        let uuid = uuid_builder.next();

        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let data_frames = (0..batches.len())
            .map(|i| {
                let (_, flight_data) = flight_data_from_arrow_batch(&batches[i], &options);
                DataFrameStruct {
                    header: flight_data.data_header,
                    body:   flight_data.data_body,
                }
            })
            .collect::<Vec<DataFrameStruct>>();
        unsafe {
            println!(
                "transmute data - raw data: {}",
                data_frames[0].header.len() + data_frames[0].body.len(),
            );
        }

        let p: *const DataFrameStruct = &data_frames[0];
        let p: *const u8 = p as *const u8;
        let d: &[u8] = unsafe { slice::from_raw_parts(p, mem::size_of::<DataFrameStruct>()) };

        let (head, body, _tail) = unsafe { d.align_to::<DataFrameStruct>() };
        assert!(head.is_empty(), "Data was not aligned");
        let my_struct = &body[0];

        unsafe {
            assert_eq!(data_frames[0].header, (*my_struct).header);
            assert_eq!(data_frames[0].body, (*my_struct).body);
        }

        let encoding = Encoding::Zstd;
        // compress
        let now = Instant::now();
        let event = serde_json::to_value(&Payload {
            data: encoding.compress(&d),
            uuid: uuid.clone(),
            encoding: encoding.clone(),
            schema,
        })
        .unwrap();
        println!(
            "transmute data - compression time: {} us",
            now.elapsed().as_micros()
        );
        println!(
            "transmute data - compressed data: {}, type: {:?}",
            serde_json::to_vec(&event).unwrap().len(),
            encoding
        );

        // decompress
        let now = Instant::now();
        let payload: Payload = serde_json::from_value(event).unwrap();
        let de_uuid = payload.uuid.clone();
        let encoded = payload.encoding.decompress(&payload.data);

        let (head, body, _tail) = unsafe { encoded.align_to::<DataFrameStruct>() };
        println!(
            "transmute data - decompression time: {} us",
            now.elapsed().as_micros()
        );

        let de_struct = &body[0];
        assert!(head.is_empty(), "Data was not aligned");

        unsafe {
            assert_eq!(data_frames[0].header, (*de_struct).header);
            assert_eq!(data_frames[0].body, (*de_struct).body);
            assert_eq!(uuid, de_uuid);
            println!(
                "transmute data - decompress raw data: {}",
                (*de_struct).header.len() + (*de_struct).body.len(),
            );
        }

        Ok(())
    }
}
