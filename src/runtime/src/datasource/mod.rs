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

//! A data source is the location where data that is being used originates from.

use kafka::KafkaSource;
use kinesis::KinesisSource;
use nexmark::NexMarkSource;
use serde::{Deserialize, Serialize};

/// A Data Source for either stream processing or batch processing.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum DataSource {
    /// Amazon Kinesis Data Streams (KDS) is a massively scalable and durable
    /// real-time data streaming service.
    KinesisEvent(KinesisSource),
    /// Apache Kafka is a community distributed event streaming platform capable
    /// of handling trillions of events a day.
    KafkaEvent(KafkaSource),
    /// Nexmark is a suite of pipelines inspired by the continuous data stream
    /// queries, which includes multiple queries over a three entities model
    /// representing on online auction system.
    /// We use Nexmark benchmark to measure the performance of our system.
    NexMarkEvent(NexMarkSource),
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
    /// Lambda function invocation payload (request and response)
    /// - 6 MB (synchronous)
    /// - 256 KB (asynchronous)
    /// <https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html>
    Payload,
    /// Data source for unit tests.
    Json,
    /// Unknown data source.
    UnknownEvent,
}

impl Default for DataSource {
    fn default() -> Self {
        DataSource::NexMarkEvent(NexMarkSource::default())
    }
}

impl DataSource {
    /// Return Kinesis type with default settings.
    pub fn kinesis() -> Self {
        DataSource::default()
    }
}

pub mod kafka;
pub mod kinesis;
pub mod nexmark;
