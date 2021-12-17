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

//! This module provides different data sinks for the Flock runtime to write
//! data to.

use crate::config::FLOCK_CONF;
use crate::encoding::Encoding;
use crate::error::{FlockError, Result};
use crate::payload::DataFrame;
use crate::transform::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::flight_data_from_arrow_batch;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::FlightData;
use lazy_static::lazy_static;
use rayon::prelude::*;
use rusoto_core::{ByteStream, Region};
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use rusoto_sqs::{
    CreateQueueRequest, GetQueueUrlRequest, ReceiveMessageRequest, SendMessageRequest, Sqs,
    SqsClient,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use uuid::Uuid;

lazy_static! {
    static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["flock"]["s3_bucket"].to_string();
    static ref FLOCK_S3_CLIENT: S3Client = S3Client::new(Region::default());
    static ref FLOCK_SQS_CLIENT: SqsClient = SqsClient::new(Region::default());
}

/// Flock writes messages to the different data sinks.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum DataSinkType {
    /// No data sink.
    Empty    = 0,
    /// Write to AWS S3.
    S3       = 1,
    /// Write to AWS DynamoDB.
    DynamoDB = 2,
    /// Write to AWS SQS.
    SQS      = 3,
}

impl DataSinkType {
    /// Convert the user input to the corresponding data sink type.
    pub fn new(data_sink: usize) -> Result<DataSinkType> {
        match data_sink {
            0 => Ok(DataSinkType::Empty),
            1 => Ok(DataSinkType::S3),
            2 => Ok(DataSinkType::DynamoDB),
            3 => Ok(DataSinkType::SQS),
            _ => Err(FlockError::DataSink(format!(
                "Unknown data sink type: {}",
                data_sink
            ))),
        }
    }
}

/// The data sink interface.
#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq)]
pub struct DataSink {
    /// The record batches are encoded in the Arrow Flight Data format.
    pub data:          Vec<DataFrame>,
    /// The schema of the record batches in binary format.
    pub schema:        Vec<u8>,
    /// The encoding and compression method.
    pub encoding:      Encoding,
    /// The last actor in the dag that wrote to the data sink.
    /// Client can use this to fetch the logs for AWS WatchLogs.
    pub function_name: String,
}

impl DataSink {
    /// Create a new data sink from record batches.
    pub fn new(function_name: String, batches: Vec<RecordBatch>, encoding: Encoding) -> Self {
        let schema = schema_to_bytes(batches[0].schema());
        let data = batches
            .par_iter()
            .map(|b| {
                let (_, flight_data) = flight_data_from_arrow_batch(
                    b,
                    &arrow::ipc::writer::IpcWriteOptions::default(),
                );
                if encoding != Encoding::None {
                    DataFrame {
                        header: encoding.compress(&flight_data.data_header).unwrap(),
                        body:   encoding.compress(&flight_data.data_body).unwrap(),
                    }
                } else {
                    DataFrame {
                        header: flight_data.data_header,
                        body:   flight_data.data_body,
                    }
                }
            })
            .collect();

        Self {
            data,
            schema,
            encoding,
            function_name,
        }
    }

    /// Write the record batches to the data sink.
    pub async fn write(&self, data_sink: DataSinkType) -> Result<Value> {
        match data_sink {
            DataSinkType::Empty => {}
            DataSinkType::SQS => {
                self.write_to_sqs().await?;
            }
            DataSinkType::S3 => {
                self.write_to_s3().await?;
            }
            _ => unimplemented!(),
        }
        Ok(json!({"name": self.function_name.clone(), "sink_type": data_sink, "status": "success"}))
    }

    /// Read the record batches from the data sink.
    pub async fn read(function_name: String, data_sink: DataSinkType) -> Result<DataSink> {
        match data_sink {
            DataSinkType::Empty => Ok(DataSink {
                function_name,
                ..Default::default()
            }),
            DataSinkType::SQS => DataSink::read_from_sqs(function_name).await,
            DataSinkType::S3 => DataSink::read_from_s3(function_name).await,
            _ => unimplemented!(),
        }
    }

    /// Convert the data sink to record batches.
    pub fn to_record_batch(self) -> Result<(String, Vec<RecordBatch>)> {
        let record_batch = |df: Vec<DataFrame>, schema: Arc<Schema>| -> Vec<RecordBatch> {
            df.into_par_iter()
                .map(|d| {
                    flight_data_to_arrow_batch(
                        &FlightData {
                            data_body:         d.body,
                            data_header:       d.header,
                            app_metadata:      vec![],
                            flight_descriptor: None,
                        },
                        schema.clone(),
                        &[],
                    )
                    .unwrap()
                })
                .collect()
        };

        let dataframe = unmarshal(self.data, self.encoding.clone());
        Ok((
            self.function_name,
            record_batch(dataframe, schema_from_bytes(&self.schema)?),
        ))
    }

    async fn write_to_sqs(&self) -> Result<()> {
        // The name of the new queue. The following limits apply to this name:
        // A queue name can have up to 80 characters.
        // Valid values: alphanumeric characters, hyphens (-), and underscores (_).
        // A FIFO queue name must end with the .fifo suffix.
        let queue_name = self.function_name.split('-').next().unwrap();

        let mut attrs = HashMap::new();
        // The length of time, in seconds, for which Amazon SQS retains a message.
        attrs.insert("MessageRetentionPeriod".to_string(), "3600".to_string());
        // Designates a queue as FIFO.
        // For more information, see FIFO queue logic in the Amazon Simple Queue Service
        // Developer Guide.
        attrs.insert("FifoQueue".to_string(), "true".to_string());

        let queue_url = FLOCK_SQS_CLIENT
            .create_queue(CreateQueueRequest {
                queue_name: format!("{}.fifo", queue_name),
                attributes: Some(attrs),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?
            .queue_url
            .expect("queue_url not found");

        FLOCK_SQS_CLIENT
            .send_message(SendMessageRequest {
                queue_url,
                message_body: serde_json::to_string(&self).unwrap(),
                message_group_id: Some(queue_name.to_string()),
                message_deduplication_id: Some(format!("{}", Uuid::new_v4())),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;

        Ok(())
    }

    async fn write_to_s3(&self) -> Result<()> {
        let s3_key = self.function_name.split('-').next().unwrap();
        FLOCK_S3_CLIENT
            .put_object(PutObjectRequest {
                bucket: FLOCK_S3_BUCKET.clone(),
                key: s3_key.to_string(),
                body: Some(ByteStream::from(serde_json::to_vec(&self)?)),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;

        Ok(())
    }

    async fn read_from_sqs(function_name: String) -> Result<DataSink> {
        let queue_name = function_name.split('-').next().unwrap();
        let queue_url = FLOCK_SQS_CLIENT
            .get_queue_url(GetQueueUrlRequest {
                queue_name: format!("{}.fifo", queue_name),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?
            .queue_url
            .expect("Queue URL not found");

        let messages = FLOCK_SQS_CLIENT
            .receive_message(ReceiveMessageRequest {
                queue_url: queue_url.to_string(),
                max_number_of_messages: Some(1),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?
            .messages
            .expect("Messages not found");

        assert!(messages.len() <= 1);

        serde_json::from_str(messages[0].body.as_ref().expect("Message body not found"))
            .map_err(|e| FlockError::AWS(e.to_string()))
    }

    async fn read_from_s3(function_name: String) -> Result<DataSink> {
        let s3_key = function_name.split('-').next().unwrap();
        let body = FLOCK_S3_CLIENT
            .get_object(GetObjectRequest {
                bucket: FLOCK_S3_BUCKET.clone(),
                key: s3_key.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?
            .body
            .take()
            .expect("body is empty");

        Ok(tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            body.into_blocking_read().read_to_end(&mut buf).unwrap();
            serde_json::from_slice(&buf).unwrap()
        })
        .await
        .expect("failed to load plan from S3"))
    }
}
