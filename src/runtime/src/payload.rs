// Copyright 2020 UMD Database Group. All Rights Reserved.
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
use crate::error::{Result, SquirtleError};
use abomonation::{decode, encode};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::{
    flight_data_from_arrow_batch, flight_data_from_arrow_schema, flight_data_to_arrow_batch,
};
use arrow_flight::FlightData;
use rayon::prelude::*;
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

/// Whether it is streaming or batch processing, each query has a unique
/// identifier to distinguish each other, so that the lambda function can
/// correctly separate and aggregate the results for distributed dataflow
/// computation.
#[derive(Default, Debug, Clone, Abomonation, Deserialize, Serialize, PartialEq, Eq)]
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
#[derive(Default, Debug, Clone, Abomonation, Deserialize, Serialize, PartialEq, Eq)]
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
#[derive(Default, Debug, Abomonation, Deserialize, Serialize, PartialEq)]
pub struct Payload {
    /// The data batches in the payload.
    pub data:     Vec<DataFrame>,
    /// The subplan's schema.
    #[serde(with = "serde_bytes")]
    pub schema:   Vec<u8>,
    /// The query's uuid.
    pub uuid:     Uuid,
    /// Compress `DataFrame` to guarantee the total size
    /// of payload doesn't exceed 256 KB.
    pub encoding: Encoding,
}

impl Payload {
    /// Serialize the schema
    pub fn schema_to_bytes(schema: SchemaRef) -> Vec<u8> {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let flight_data = flight_data_from_arrow_schema(&schema, &options);
        flight_data.data_header
    }

    /// Deserialize the schema
    pub fn schema_from_bytes(bytes: &[u8]) -> Result<Arc<Schema>> {
        let schema =
            arrow::ipc::convert::schema_from_bytes(&bytes).map_err(SquirtleError::Arrow)?;
        Ok(Arc::new(schema))
    }

    /// Convert incoming payload to record batch in Arrow.
    pub fn to_batch(event: Value) -> (Vec<RecordBatch>, Uuid) {
        let payload: Payload = serde_json::from_value(event).unwrap();
        let uuid = payload.uuid.clone();
        let schema = payload.schema.clone();
        let data_frames = unmarshal(payload);
        (
            data_frames
                .into_par_iter()
                .map(|d| {
                    flight_data_to_arrow_batch(
                        &FlightData {
                            data_body:         d.body,
                            data_header:       d.header,
                            app_metadata:      vec![],
                            flight_descriptor: None,
                        },
                        Self::schema_from_bytes(&schema).unwrap(),
                        &[],
                    )
                    .unwrap()
                })
                .collect(),
            uuid,
        )
    }

    /// Convert record batch to payload for network transmission.
    pub fn to_value(batches: &[RecordBatch], uuid: Uuid, encoding: Encoding) -> Value {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let data_frames = batches
            .par_iter()
            .map(|b| {
                let (_, flight_data) = flight_data_from_arrow_batch(&b, &options);
                if encoding != Encoding::None {
                    DataFrame {
                        header: encoding.compress(&flight_data.data_header),
                        body:   encoding.compress(&flight_data.data_body),
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
            schema: Self::schema_to_bytes(batches[0].schema()),
            uuid,
            encoding,
        })
        .unwrap()
    }

    /// Convert record batch to payload for network transmission.
    pub fn to_vec(batches: &[RecordBatch], uuid: Uuid, encoding: Encoding) -> Vec<u8> {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let data_frames = batches
            .par_iter()
            .map(|b| {
                let (_, flight_data) = flight_data_from_arrow_batch(&b, &options);
                if encoding != Encoding::None {
                    DataFrame {
                        header: encoding.compress(&flight_data.data_header),
                        body:   encoding.compress(&flight_data.data_body),
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
            schema: Self::schema_to_bytes(batches[0].schema()),
            uuid,
            encoding,
        })
        .unwrap()
    }

    /// Convert record batch to bytes for network transmission.
    pub fn to_bytes(batch: &RecordBatch, uuid: Uuid, encoding: Encoding) -> bytes::Bytes {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema = Self::schema_to_bytes(batch.schema());
        let (_, flight_data) = flight_data_from_arrow_batch(&batch, &options);

        let data_frames = {
            if encoding != Encoding::None {
                DataFrame {
                    header: encoding.compress(&flight_data.data_header),
                    body:   encoding.compress(&flight_data.data_body),
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
        })
        .unwrap()
        .into()
    }
}

/// Deserialize `DataFrame` from cloud functions.
pub fn unmarshal(payload: Payload) -> Vec<DataFrame> {
    match payload.encoding {
        Encoding::Snappy | Encoding::Lz4 | Encoding::Zstd => payload
            .data
            .par_iter()
            .map(|d| DataFrame {
                header: payload.encoding.decompress(&d.header),
                body:   payload.encoding.decompress(&d.body),
            })
            .collect(),
        Encoding::None => payload.data,
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{Executor, LambdaExecutor};
    use arrow::array::{Array, StructArray};
    use arrow::csv;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::json;
    use bytes::BytesMut;
    use prost::Message;
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
        println!(
            "buffer memory size: {}",
            struct_array.get_buffer_memory_size()
        );
        // return the total number of bytes of memory occupied physically by this array.
        println!(
            "physical memory size: {}",
            struct_array.get_array_memory_size()
        );

        let options = arrow::ipc::writer::IpcWriteOptions::default();
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
            include_str!("../../test/data/JC-202011-citibike-tripdata.csv").as_bytes();
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

        let batches = LambdaExecutor::coalesce_batches(vec![batches], size).await?;
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

        Ok(())
    }

    #[tokio::test]
    async fn serde_payload() -> Result<()> {
        let batches = init_batches();
        let mut uuid_builder =
            UuidBuilder::new("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836", 10);
        let uuid = uuid_builder.next();

        let now = Instant::now();
        let value = Payload::to_value(&batches, uuid.clone(), Encoding::default());
        println!(
            "serde payload to value (with compression) - time: {} ms",
            now.elapsed().as_millis()
        );

        let payload1: Payload = serde_json::from_value(value.clone())?;
        let now = Instant::now();
        let (de_batches, de_uuid) = Payload::to_batch(value);
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
        let bytes = Payload::to_vec(&batches, uuid, Encoding::default());
        println!(
            "serde payload to bytes (with compression) - time: {} ms, size: {} bytes",
            now.elapsed().as_millis(),
            bytes.len()
        );

        let payload2: Payload = serde_json::from_slice(&bytes)?;
        assert_eq!(payload1, payload2);

        let now = Instant::now();
        let bytes = Encoding::Zstd.compress(&bytes);
        println!(
            "serde Json bytes - time: {} ms, size: {} bytes",
            now.elapsed().as_millis(),
            bytes.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn abomonation_data_frames() -> Result<()> {
        let options = arrow::ipc::writer::IpcWriteOptions::default();

        for encoding in [
            Encoding::Snappy,
            Encoding::Lz4,
            Encoding::Zstd,
            Encoding::None,
        ]
        .iter()
        {
            println!("-----------------{:?}-----------------", encoding);
            let batches = init_batches();
            let schema = batches[0].schema().clone();

            let now = Instant::now();

            // compress
            let data_frames = batches
                .into_par_iter()
                .map(|batch| {
                    let (_, flight_data) = flight_data_from_arrow_batch(&batch, &options);
                    DataFrame {
                        header: flight_data.data_header,
                        body:   flight_data.data_body,
                    }
                })
                .collect::<Vec<DataFrame>>();

            println!(
                "abomonation data - raw data: {}",
                data_frames[0].header.len() + data_frames[0].body.len(),
            );

            let uuid =
                UuidBuilder::new("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836", 10).next();

            let payload = Payload {
                data: data_frames,
                schema: Payload::schema_to_bytes(schema.clone()),
                uuid,
                encoding: encoding.clone(),
            };

            let mut bytes = Vec::new();
            unsafe {
                encode(&payload, &mut bytes)?;
            }
            let event: bytes::Bytes = encoding.compress(&bytes).into();
            println!(
                "abomonation data - compression time: {} ms",
                now.elapsed().as_millis()
            );
            println!(
                "abomonation data - compressed data: {}, type: {:?}",
                event.len(),
                encoding
            );

            // decompress
            let now = Instant::now();
            let mut encoded = encoding.decompress(&event);
            if let Some((result, remaining)) = unsafe { decode::<Payload>(&mut encoded) } {
                println!(
                    "abomonation data - decompression time: {} ms",
                    now.elapsed().as_millis()
                );
                assert!(remaining.is_empty());
                println!(
                    "abomonation data - decompressed data: {}",
                    result.data[0].header.len() + result.data[0].body.len(),
                );

                assert_eq!(payload.schema, result.schema);
                assert_eq!(payload.data, result.data);
                assert_eq!(payload.uuid, result.uuid);
            }
            println!("--------------------------------------");
        }

        Ok(())
    }

    #[tokio::test]
    async fn prost_data_frames() -> Result<()> {
        let batches = init_batches();

        // compress
        let now = Instant::now();
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let data_frames = (0..batches.len())
            .map(|i| {
                let (_, flight_data) = flight_data_from_arrow_batch(&batches[i], &options);
                flight_data
            })
            .collect::<Vec<FlightData>>();

        println!("prost data - raw data: {}", data_frames[0].encoded_len(),);

        let encoding = Encoding::Zstd;

        // compress
        let mut buffer = BytesMut::with_capacity(data_frames[0].encoded_len());
        data_frames[0].encode(&mut buffer).unwrap();
        let event: bytes::Bytes = encoding.compress(&buffer).into();
        println!(
            "prost data - compression time: {} ms",
            now.elapsed().as_millis()
        );
        println!(
            "prost data - compressed data: {}, type: {:?}",
            event.len(),
            encoding
        );

        // decompress
        let now = Instant::now();
        let de_flights_data = FlightData::decode(&encoding.decompress(&event)[..]).unwrap();
        println!(
            "prost data - decompression time: {} ms",
            now.elapsed().as_millis()
        );
        println!(
            "prost data - decompressed data: {}",
            de_flights_data.encoded_len(),
        );

        assert_eq!(de_flights_data, data_frames[0]);

        Ok(())
    }

    #[tokio::test]
    async fn schema_ipc() -> Result<()> {
        let batches = init_batches();
        let schema1 = batches[0].schema();

        // serilization
        let bytes = Payload::schema_to_bytes(schema1.clone());

        // deserialization
        let schema2 = Payload::schema_from_bytes(&bytes)?;

        assert_eq!(schema1, schema2);

        Ok(())
    }

    #[tokio::test]
    async fn uuid() -> Result<()> {
        let mut uuid_builder =
            UuidBuilder::new("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836", 10);
        (0..10).for_each(|i| assert_eq!(uuid_builder.get(i).seq_num, i));

        let batches = init_batches();
        let bytes = Payload::to_bytes(&batches[0], uuid_builder.next(), Encoding::default());
        let value: Value = serde_json::from_slice(&bytes)?;
        let (de_batches, _) = Payload::to_batch(value);

        assert_eq!(batches[0].schema(), de_batches[0].schema());
        assert_eq!(batches[0].columns(), de_batches[0].columns());

        Ok(())
    }
}
