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
use crate::error::Result;
use crate::runtime::arena::Bitmap;
use crate::runtime::payload::Payload;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;
use tokio::task::JoinHandle;

/// S3StateBackend is a state backend that stores query states in Amazon S3.
///
/// S3 bucket name is the qid of the function payload:
///
/// | query code | timestamp  | random string |
///
/// S3 key composed of the following parts:
///
/// | plan index | shuffle id | sequence id   |
///
/// Note: Parts of component are derived from cloud function name. The cloud
/// function name has three parts: | query code | plan index | group index |.
/// `query code` is the hash digest of the SQL query. `plan index` is the stage
/// index of the query DAG. `group index` is the current index of the function
/// group.
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

    async fn write(&self, bucket: String, key: String, payload_bytes: Vec<u8>) -> Result<()> {
        s3::create_bucket_if_missing(&bucket).await?;
        s3::put_object(&bucket, &key, payload_bytes).await
    }

    async fn read(&self, bucket: String, keys: Vec<String>) -> Result<Vec<Payload>> {
        let tasks = keys
            .into_iter()
            .map(|key| {
                let b = bucket.clone();
                tokio::spawn(async move {
                    Ok(serde_json::from_slice(&s3::get_object(&b, &key).await?)?)
                })
            })
            .collect::<Vec<JoinHandle<Result<Payload>>>>();

        Ok(futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(|r| r.unwrap().unwrap())
            .collect())
    }
}

impl S3StateBackend {
    /// Creates a new S3StateBackend.
    pub fn new() -> Self {
        Self {}
    }

    /// Read S3 keys from a bucket with a prefix.
    ///
    /// This function can be used to monitor the progress of checkpointing, and
    /// can also be used for early aggregation, if the payload has not reached
    /// the current function through the function invocation. If the total
    /// number of keys returned equals to the total number of payloads, then
    /// the checkpoint is complete.
    ///
    /// # Arguments
    /// * `bucket` - The S3 bucket to store the checkpoint.
    /// * `prefix` - The S3 key prefix to store each data partition.
    ///
    /// # Returns
    /// A vector of S3 keys in usize format.
    pub async fn read_s3_keys(&self, bucket: &str, prefix: &str) -> Result<Vec<usize>> {
        Ok(s3::get_matched_keys(bucket, prefix)
            .await?
            .into_iter()
            .map(|key| {
                let mut key_parts = key.split('/');
                key_parts.next(); // skip the plan index
                key_parts.next(); // skip the shuffle id
                key_parts.next().unwrap().parse::<usize>().unwrap() // get the sequence id
            })
            .collect())
    }

    /// Counts the number of S3 keys in a bucket with a prefix.
    ///
    /// # Arguments
    /// * `bucket` - The S3 bucket to store the checkpoint.
    /// * `prefix` - The S3 key prefix to store each data partition.
    ///
    /// # Returns
    /// The number of S3 keys.
    pub async fn get_s3_key_num(&self, bucket: &str, prefix: &str) -> Result<usize> {
        Ok(s3::get_matched_keys(bucket, prefix).await?.len())
    }

    /// Returns the latest checkpointed keys.
    ///
    /// # Arguments
    /// * `bucket` - The S3 bucket to store the checkpoint.
    /// * `prefix` - The S3 key prefix to store data partitions.
    /// * `old_keys` - The keys that have been checkpointed before.
    ///
    /// # Returns
    /// * The difference between the latest checkpointed keys and the old keys.
    pub async fn new_s3_keys(
        &self,
        bucket: &str,
        prefix: &str,
        old_keys: &Bitmap,
    ) -> Result<Vec<String>> {
        Ok(self
            .read_s3_keys(bucket, prefix)
            .await?
            .into_iter()
            .filter(|seq_num| !old_keys.is_set(*seq_num))
            .map(|seq_num| format!("{:02}/{:02}", prefix, seq_num))
            .collect())
    }
}
