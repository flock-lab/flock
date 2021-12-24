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

//! The operations for managing an Amazon MSK cluster. Apache Kafka is an
//! open-source stream-processing software platform developed by the Apache
//! Software Foundation, written in Scala and Java. The project aims to provide
//! a unified, high-throughput, low-latency platform for handling real-time data
//! feeds.

use aws_lambda_events::event::kafka::KafkaEvent;

use arrow::json::{self, reader::infer_json_schema};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::query::StreamWindow;
use arrow::datatypes::Schema;
use rayon::prelude::*;
use rusoto_lambda::CreateEventSourceMappingRequest;
use serde::{Deserialize, Serialize};
use std::io::BufReader;
use std::sync::Arc;

/// A struct to manage all KafKa info in cloud environment.
#[derive(Default, Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct KafkaSource {
    /// The window type.
    pub window:       StreamWindow,
    /// The name of the cluster.
    pub cluster_name: String,
    /// The Amazon Resource Name (ARN) of the cluster.
    pub cluster_arn:  Option<String>,
    /// The name of the Kafka topic.
    pub topics:       Option<Vec<String>>,
}

impl KafkaSource {
    /// Fetches data records from KafKa.
    pub fn fetch_data(&self) -> Result<RecordBatch> {
        unimplemented!();
    }
}

/// Creates event source mapping for KafKa.
pub async fn create_event_source_mapping_request(
    function_name: &str,
    window_in_seconds: i64,
    cluster_arn: &Option<String>,
    topics: &Option<Vec<String>>,
) -> Result<CreateEventSourceMappingRequest> {
    Ok(CreateEventSourceMappingRequest {
        // The maximum number of items to retrieve in a single batch.
        // Amazon KafKa - Default 100. Max 10,000.
        batch_size: Some(10000),
        // If true, the event source mapping is active. Set to false to pause polling and
        // invocation.
        enabled: Some(true),
        // The Amazon Resource Name (ARN) of the event source.
        // Amazon Managed Streaming for Apache Kafka - The ARN of the cluster.
        event_source_arn: cluster_arn.clone(),
        // The name of the Lambda function.
        function_name: function_name.to_owned(),
        // The maximum amount of time to gather records before invoking the function, in seconds.
        maximum_batching_window_in_seconds: Some(300),
        // The number of batches to process from each shard concurrently.
        // The parallelization factor can be scaled up to 10.
        // <https://aws.amazon.com/blogs/compute/new-aws-lambda-scaling-controls-for-kinesis-and-dynamodb-event-sources>
        parallelization_factor: Some(4),
        // The position in a stream from which to start reading. Required for Amazon Kinesis, Amazon
        // DynamoDB, and Amazon MSK Streams sources.
        starting_position: Some("LATEST".to_owned()),
        // The duration of a processing window in seconds. The range is between 1 second up to 15
        // minutes.
        tumbling_window_in_seconds: Some(window_in_seconds),
        // The name of the Kafka topic.
        topics: topics.clone(),
        ..CreateEventSourceMappingRequest::default()
    })
}

/// Converts KafKa event to record batch in Arrow.
pub fn to_batch(event: KafkaEvent) -> Vec<RecordBatch> {
    let mut input = vec![];
    let mut schema = Schema::empty();

    // get all data from KafKa event
    for (i, records) in event.records.values().enumerate() {
        if i == 0 {
            assert!(!records.is_empty());
            // infer schema based on the first record
            let record = base64::decode(records[0].value.as_ref().unwrap()).unwrap();
            schema = infer_json_schema(&mut BufReader::new(&record[..]), Some(1)).unwrap();
        }

        input.append(
            &mut records
                .into_par_iter()
                .flat_map(|r| {
                    base64::decode(r.value.as_ref().unwrap())
                        .unwrap()
                        .into_iter()
                        .chain(vec![10].into_iter())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
        );
    }

    // transform data to record batch in Arrow
    let batch_size = 1024;
    let mut reader = json::Reader::new(
        BufReader::with_capacity(input.len(), &input[..]),
        Arc::new(schema),
        batch_size,
        None,
    );

    let mut batches = vec![];
    while let Some(batch) = reader.next().unwrap() {
        batches.push(batch);
    }
    batches
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::util::pretty;

    #[test]
    #[ignore]
    fn example_kafka_event() -> Result<()> {
        let data = include_bytes!("../../../test/data/example-kafka-event.json");
        let parsed: KafkaEvent = serde_json::from_slice(data)?;
        let output: String = serde_json::to_string(&parsed)?;
        let reparsed: KafkaEvent = serde_json::from_slice(output.as_bytes())?;
        assert_eq!(parsed, reparsed);
        let mut batches = vec![];
        for (_, records) in parsed.records.iter() {
            batches.append(
                &mut records
                    .into_par_iter()
                    .flat_map(|r| base64::decode(r.value.as_ref().unwrap()).unwrap())
                    .collect::<Vec<u8>>(),
            );
        }
        assert_eq!(
            r#"{"cust_id":123,"month":9,"amount_paid":456.78}"#,
            std::str::from_utf8(&batches).unwrap()
        );

        pretty::print_batches(&to_batch(parsed))?;

        Ok(())
    }
}
