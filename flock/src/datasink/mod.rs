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

use crate::encoding::Encoding;
use crate::error::{FlockError, Result};
use crate::runtime::payload::DataFrame;
use crate::services::*;
use crate::transmute::*;
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow_flight::utils::flight_data_from_arrow_batch;
use datafusion::arrow_flight::utils::flight_data_to_arrow_batch;
use datafusion::arrow_flight::FlightData;
use datafusion::execution::context::ExecutionContext;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::CsvReadOptions;
use rayon::prelude::*;
use rusoto_core::ByteStream;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3};
use rusoto_sqs::{
    CreateQueueRequest, GetQueueUrlRequest, ReceiveMessageRequest, SendMessageRequest, Sqs,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};
use uuid::Uuid;

/// Flock data format for data sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSinkFormat {
    /// CSV format.
    CSV,
    /// JSON format.
    JSON,
    /// Parquet format.
    Parquet,
    /// Binary format.
    /// This is the default format.
    SerdeBinary,
}

impl Default for DataSinkFormat {
    fn default() -> Self {
        DataSinkFormat::SerdeBinary
    }
}

/// Flock writes messages to the different data sinks.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum DataSinkType {
    /// No data sink.
    Blackhole,
    /// Write to AWS S3.
    S3,
    /// Write to AWS DynamoDB.
    DynamoDB,
    /// Write to AWS SQS.
    SQS,
    /// Write to AWS EFS.
    EFS,
}

impl Default for DataSinkType {
    fn default() -> Self {
        DataSinkType::Blackhole
    }
}

impl DataSinkType {
    /// Convert the user input to the corresponding data sink type.
    pub fn new(data_sink: &str) -> Result<DataSinkType> {
        match data_sink {
            "blackhole" => Ok(DataSinkType::Blackhole),
            "s3" => Ok(DataSinkType::S3),
            "dynamodb" => Ok(DataSinkType::DynamoDB),
            "sqs" => Ok(DataSinkType::SQS),
            "efs" => Ok(DataSinkType::EFS),
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
    /// The record batch to write.
    #[serde(skip)]
    pub record_batches: Box<Vec<RecordBatch>>,
    /// The record batches are encoded in the Arrow Flight Data format.
    pub encoded_data:   Vec<DataFrame>,
    /// The schema of the record batches in binary format.
    pub schema:         Vec<u8>,
    /// The encoding and compression method.
    pub encoding:       Encoding,
    /// The last actor in the dag that wrote to the data sink.
    /// Client can use this to fetch the logs for AWS WatchLogs.
    pub function_name:  String,
}

impl DataSink {
    /// Create a new data sink from record batches.
    pub fn new(
        function_name: String,
        record_batches: Vec<RecordBatch>,
        encoding: Encoding,
    ) -> Self {
        Self {
            record_batches: Box::new(record_batches),
            encoding,
            function_name,
            ..Default::default()
        }
    }

    /// Write the record batches to the data sink.
    pub async fn write(
        &mut self,
        sink_type: DataSinkType,
        sink_format: DataSinkFormat,
    ) -> Result<Value> {
        match sink_type {
            DataSinkType::Blackhole => {}
            DataSinkType::SQS => {
                self.write_to_sqs().await?;
            }
            DataSinkType::S3 => {
                self.write_to_s3().await?;
            }
            DataSinkType::EFS => {
                self.write_to_efs(sink_format).await?;
            }
            _ => unimplemented!(),
        }
        Ok(json!({"name": self.function_name.clone(), "sink_type": sink_type, "status": "success"}))
    }

    /// Read the record batches from the data sink.
    pub async fn read(
        function_name: String,
        sink_type: DataSinkType,
        sink_format: DataSinkFormat,
    ) -> Result<DataSink> {
        match sink_type {
            DataSinkType::Blackhole => Ok(DataSink {
                function_name,
                ..Default::default()
            }),
            DataSinkType::SQS => DataSink::read_from_sqs(function_name).await,
            DataSinkType::S3 => DataSink::read_from_s3(function_name).await,
            DataSinkType::EFS => DataSink::read_from_efs(function_name, sink_format).await,
            _ => unimplemented!(),
        }
    }

    /// This is an internal function that is used to decode `self.encoded_data`
    /// to `self.record_batches` for future use.
    fn decode_record_batches(&mut self) -> Result<()> {
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

        if self.record_batches.is_empty() {
            let dataframe = unmarshal(self.encoded_data.clone(), self.encoding.clone());
            self.record_batches =
                Box::new(record_batch(dataframe, schema_from_bytes(&self.schema)?));
        }

        Ok(())
    }

    /// This is an internal function that is used to encode the
    /// `self.record_batches` to the `self.encoded_data` for the data sink.
    fn encode_record_batches(&mut self) {
        self.schema = schema_to_bytes(self.record_batches[0].schema());
        self.encoded_data = self
            .record_batches
            .par_iter()
            .map(|b| {
                let (_, flight_data) = flight_data_from_arrow_batch(
                    b,
                    &datafusion::arrow::ipc::writer::IpcWriteOptions::default(),
                );
                if self.encoding != Encoding::None {
                    DataFrame {
                        header: self.encoding.compress(&flight_data.data_header).unwrap(),
                        body:   self.encoding.compress(&flight_data.data_body).unwrap(),
                    }
                } else {
                    DataFrame {
                        header: flight_data.data_header,
                        body:   flight_data.data_body,
                    }
                }
            })
            .collect();
    }

    async fn write_to_sqs(&mut self) -> Result<()> {
        self.encode_record_batches();
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

    async fn write_to_s3(&mut self) -> Result<()> {
        self.encode_record_batches();

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

    async fn write_to_efs(&mut self, sink_format: DataSinkFormat) -> Result<()> {
        let fs_path = Path::new(&*FLOCK_EFS_MOUNT_PATH).join(self.function_name.clone());
        let mut tasks = vec![];

        match sink_format {
            DataSinkFormat::CSV => {
                for (i, batch) in self.record_batches.iter().enumerate() {
                    let filename = format!("part-{}.csv", i);
                    let file = std::fs::File::create(fs_path.join(&filename))?;
                    let mut writer = csv::Writer::new(file);
                    let data = batch.clone();
                    let handle: JoinHandle<Result<()>> =
                        task::spawn(async move { writer.write(&data).map_err(FlockError::from) });
                    tasks.push(handle);
                }
            }
            DataSinkFormat::Parquet => {
                let schema = self.record_batches[0].schema();
                for (i, batch) in self.record_batches.iter().enumerate() {
                    let filename = format!("part-{}.parquet", i);
                    let file = std::fs::File::create(fs_path.join(&filename))?;
                    let mut writer =
                        ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), None)?;
                    let data = batch.clone();
                    let handle: JoinHandle<Result<()>> = task::spawn(async move {
                        writer.write(&data)?;
                        writer.close().map_err(FlockError::from).map(|_| ())
                    });
                    tasks.push(handle);
                }
            }
            _ => unimplemented!(),
        }
        futures::future::join_all(tasks).await;
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

        let mut data: DataSink =
            serde_json::from_str(messages[0].body.as_ref().expect("Message body not found"))
                .map_err(|e| FlockError::AWS(e.to_string()))?;

        data.decode_record_batches()?;

        Ok(data)
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

        let mut data: DataSink = tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            body.into_blocking_read().read_to_end(&mut buf).unwrap();
            serde_json::from_slice(&buf).unwrap()
        })
        .await
        .expect("failed to load plan from S3");

        data.decode_record_batches()?;

        Ok(data)
    }

    async fn read_from_efs(function_name: String, sink_format: DataSinkFormat) -> Result<DataSink> {
        let fs_path = Path::new(&*FLOCK_EFS_MOUNT_PATH).join(function_name.clone());
        let ctx = Box::new(ExecutionContext::new());

        let mut tasks = vec![];
        match sink_format {
            DataSinkFormat::CSV => {
                let options = CsvReadOptions::new()
                    .has_header(true)
                    .schema_infer_max_records(100);
                for entry in std::fs::read_dir(fs_path)? {
                    let entry = entry?;
                    if entry.file_name().to_str().unwrap().ends_with(".csv") {
                        let mut context = ctx.clone();
                        let task: JoinHandle<Result<Vec<RecordBatch>>> = task::spawn(async move {
                            let df = context
                                .read_csv(entry.path().to_str().unwrap(), options)
                                .await
                                .unwrap();
                            df.collect().await.map_err(FlockError::from)
                        });
                        tasks.push(task);
                    }
                }
            }
            DataSinkFormat::Parquet => {
                for entry in std::fs::read_dir(fs_path)? {
                    let entry = entry?;
                    if entry.file_name().to_str().unwrap().ends_with(".parquet") {
                        let mut context = ctx.clone();
                        let task: JoinHandle<Result<Vec<RecordBatch>>> = task::spawn(async move {
                            let df = context
                                .read_parquet(entry.path().to_str().unwrap())
                                .await
                                .unwrap();
                            df.collect().await.map_err(FlockError::from)
                        });
                        tasks.push(task);
                    }
                }
            }
            _ => unimplemented!(),
        }

        let mut records = vec![];
        for task in tasks {
            records.push(task.await.map_err(|e| FlockError::AWS(e.to_string()))??);
        }

        Ok(DataSink {
            function_name: function_name.clone(),
            record_batches: Box::new(records.into_iter().flatten().collect()),
            ..Default::default()
        })
    }
}
