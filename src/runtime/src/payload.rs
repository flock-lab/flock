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

/// Whether it is streaming or batch processing, each query has a unique
/// identifier to distinguish each other, so that the lambda function can
/// correctly separate and aggregate the results for distributed dataflow
/// computation.
#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Uuid {
    /// The identifier of the query triggered at the specific time.
    ///
    /// Note: `tid` is also used as a random seed for choosing the next function
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
