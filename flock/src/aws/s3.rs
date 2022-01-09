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

//! This crate contains all wrapped functions of the AWS S3 service.

use crate::configs::*;
use crate::error::{FlockError, Result};
use rusoto_core::ByteStream;
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3};
use std::io::Read;

/// Puts an object to AWS S3 if the object does not exist. If the object exists,
/// it isn't modified.
///
/// # Arguments
/// * `bucket` - The name of the bucket to put the object in.
/// * `key` - The key of the object to put.
/// * `body` - The body of the object to put.
pub async fn put_object_if_missing(bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
    if let Some(0) = FLOCK_S3_CLIENT
        .list_objects_v2(ListObjectsV2Request {
            bucket: bucket.to_owned(),
            prefix: Some(key.to_owned()),
            max_keys: Some(1),
            ..Default::default()
        })
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?
        .key_count
    {
        FLOCK_S3_CLIENT
            .put_object(PutObjectRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                body: Some(ByteStream::from(body)),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;
    }
    Ok(())
}

/// Puts an object to AWS S3. If the object exists, it is overwritten.
///
/// # Arguments
/// * `bucket` - The name of the bucket to put the object in.
/// * `key` - The key of the object to put.
/// * `body` - The body of the object to put.
pub async fn put_object(bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
    FLOCK_S3_CLIENT
        .put_object(PutObjectRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            body: Some(ByteStream::from(body)),
            ..Default::default()
        })
        .await
        .map_err(|e| FlockError::AWS(e.to_string()))
        .map(|_| ())
}

/// Gets object from AWS S3.
/// If the object does not exist, it returns an empty body.
///
/// # Arguments
/// * `bucket` - The name of the bucket to get the object from.
/// * `key` - The key of the object to get.
///
/// # Returns
/// The body of the object.
pub async fn get_object(bucket: &str, key: &str) -> Result<Vec<u8>> {
    let body = FLOCK_S3_CLIENT
        .get_object(GetObjectRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
        .body
        .take()
        .expect("body is empty");

    Ok(tokio::task::spawn_blocking(move || {
        let mut buf = Vec::new();
        body.into_blocking_read().read_to_end(&mut buf).unwrap();
        buf
    })
    .await
    .expect("failed to load plan from S3"))
}
