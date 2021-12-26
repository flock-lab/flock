// Copyright (c) 2020-present, UMD Database Group.
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

//! A data source is the location where data that is being used originates from.

use self::kafka::KafkaSource;
use self::kinesis::KinesisSource;
use self::nexmark::NEXMarkSource;
use self::ysb::YSBSource;
use crate::error::Result;
use crate::runtime::payload::{Payload, Uuid};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A relation's data in Arrow record batches.
pub type RelationPartitions = Arc<Vec<Vec<RecordBatch>>>;
/// To determine the function type to be called: sync or async.
pub type FastAggregate = bool;

/// A streaming data source trait.
pub trait DataStream {
    /// Select events from the stream and transform them into a payload.
    ///
    /// This function is called by the runtime to get the next epoch of events.
    /// The events will be transformed into a single payload and returned.
    fn select_event_to_payload(
        &self,
        time: usize,
        generator: usize,
        query_number: Option<usize>,
        uuid: Uuid,
        sync: bool,
    ) -> Result<Payload>;

    /// Select events from the stream and transform them into record batches.
    ///
    /// This function will partition the event to multiple record batches,
    /// therefore the number of record batches will be equal to the number
    /// of partitions, which is equal to the number of payloads.
    fn select_event_to_batches(
        &self,
        time: usize,
        generator: usize,
        query_number: Option<usize>,
        sync: bool,
    ) -> Result<(RelationPartitions, RelationPartitions)>;
}

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
    NEXMarkEvent(NEXMarkSource),
    /// The Yahoo Streaming Benchmark is a well-known benchmark used in industry
    /// to evaluate streaming systems.
    YSBEvent(YSBSource),
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
    /// - 6 MB (synchronous) if FAST_AGGREGATE is true
    /// - 256 KB (asynchronous) if FAST_AGGREGATE is false
    /// <https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html>
    Payload(FastAggregate),
    /// Data source for unit tests.
    Json,
    /// AWS S3 for baseline benchmark.
    S3(NEXMarkSource),
    /// Unknown data source.
    UnknownEvent,
}

impl Default for DataSource {
    fn default() -> Self {
        DataSource::UnknownEvent
    }
}

impl DataSource {
    /// Return Kinesis type with default settings.
    pub fn kinesis() -> Self {
        DataSource::KinesisEvent(KinesisSource::default())
    }
}

pub mod config;
pub mod epoch;
pub mod kafka;
pub mod kinesis;
pub mod nexmark;
pub mod ysb;
