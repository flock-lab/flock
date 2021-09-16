// Copyright (c) 2020 UMD Database Group. All Rights Reserved.
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
// Only bring in dependencies for the repl when the cli feature is enabled.

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
        DataSource::KinesisEvent(KinesisSource::default())
    }
}

pub mod kafka;
pub mod kinesis;
pub mod nexmark;
