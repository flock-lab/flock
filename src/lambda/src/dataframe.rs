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

//! DataFrame API for building and executing query plans in cloud function
//! services.

use aws_lambda_events::event::kinesis::KinesisEvent;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::json;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;

use arrow_flight::utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch};
use arrow_flight::FlightData;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use rayon::prelude::*;

use std::io::BufReader;
use std::sync::Arc;

/// A data source is the location where data that is being used originates from.
pub enum DataSource {
    /// Amazon Kinesis Data Streams (KDS) is a massively scalable and durable
    /// real-time data streaming service.
    KinesisEvent,
    /// Apache Kafka is a community distributed event streaming platform capable
    /// of handling trillions of events a day.
    KafkaEvent,
    /// Amazon Simple Queue Service (SQS) is a fully managed message queuing
    /// service that enables you to decouple and scale microservices,
    /// distributed systems, and serverless applications. SQS eliminates the
    /// complexity and overhead associated with managing and operating message
    /// oriented middleware, and empowers developers to focus on differentiating
    /// work. Using SQS, you can send, store, and receive messages between
    /// software components at any volume, without losing messages or requiring
    /// other services to be available.
    SqsEvent,
    /// Amazon Simple Notification Service (Amazon SNS) is a fully managed
    /// messaging service for both application-to-application (A2A) and
    /// application-to-person (A2P) communication.
    SnsEvent,
    /// The AWS IoT Button is a programmable button based on the Amazon Dash
    /// Button hardware. This simple Wi-Fi device is easy to configure and
    /// designed for developers to get started with AWS IoT Core, AWS Lambda,
    /// Amazon DynamoDB, Amazon SNS, and many other Amazon Web Services without
    /// writing device-specific code.
    IoTButtonEvent,
}

/// Convert Kinesis event to record batch in Arrow.
pub fn from_kinesis_to_batch(event: KinesisEvent) -> RecordBatch {
    // infer schema based on the first record
    let record: &[u8] = &event.records[0].kinesis.data.0.clone();
    let mut reader = BufReader::new(record);
    let schema = infer_json_schema(&mut reader, Some(1)).unwrap();

    // get all data from Kinesis event
    let input: &[u8] = &event
        .records
        .into_par_iter()
        .flat_map(|r| r.kinesis.data.0)
        .collect::<Vec<u8>>();

    // transform data to record batch in Arrow
    reader = BufReader::with_capacity(input.len(), input);
    let mut json = json::Reader::new(reader, schema, input.len(), None);
    json.next().unwrap().unwrap()
}

/// DataFrame is the deserialization format of the payload passed between lambda
/// functions.
#[derive(Debug, Deserialize, Serialize)]
pub struct DataFrame {
    #[serde(with = "serde_bytes")]
    header: Vec<u8>,
    #[serde(with = "serde_bytes")]
    body:   Vec<u8>,
    schema: Schema,
}

impl DataFrame {
    /// Convert incoming dataframe to record batch in Arrow.
    pub fn to_batch(event: Value) -> RecordBatch {
        let input: DataFrame = serde_json::from_value(event).unwrap();
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
        .unwrap()
    }

    /// Convert record batch to dataframe for network transmission.
    pub fn from(batch: &RecordBatch, schema: SchemaRef) -> Self {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let (_, flight_data) = flight_data_from_arrow_batch(batch, &options);
        Self {
            body:   flight_data.data_body,
            header: flight_data.data_header,
            schema: (*schema).clone(),
        }
    }
}
