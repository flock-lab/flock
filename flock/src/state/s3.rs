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

//! Use S3 state backend to manage the state of the execution engine.

use super::StateBackend;
use crate::aws::s3;
use crate::error::{FlockError, Result};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow_flight::utils::flight_data_from_arrow_batch;
use serde::{Deserialize, Serialize};
use std::any::Any;

/// S3StateBackend is a state backend that stores query states in Amazon S3.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct S3StateBackend {}

#[async_trait]
#[typetag::serde(name = "s3_state_backend")]
impl StateBackend for S3StateBackend {
    fn name(&self) -> String {
        "S3StateBackend".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    async fn write(&self, bucket: &str, key: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if !batches.is_empty() {
            s3::create_bucket_if_missing(bucket).await?;
            let batch =
                RecordBatch::concat(&batches[0].schema(), &batches).map_err(FlockError::Arrow)?;
            let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
            let (_, flight_data) = flight_data_from_arrow_batch(&batch, &options);
            s3::put_object(bucket, key, flight_data.data_body).await?;
        }
        Ok(())
    }
}

impl S3StateBackend {
    /// Creates a new S3StateBackend.
    pub fn new() -> Self {
        Self {}
    }
}
