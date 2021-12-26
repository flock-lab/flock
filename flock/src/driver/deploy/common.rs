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

//! This module contains various utility functions.

use crate::config::FLOCK_CONF;
use crate::driver::deploy::config as DeployConfig;
use crate::driver::logwatch::tail::{self, fetch_logs, AWSResponse};
use crate::error::{FlockError, Result};
use crate::runtime::context::ExecutionContext;
use bytes::Bytes;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use rusoto_core::Region;
use rusoto_lambda::{
    CreateFunctionRequest, FunctionCode, GetFunctionRequest, InvocationRequest, InvocationResponse,
    Lambda, LambdaClient, PutFunctionConcurrencyRequest, UpdateFunctionCodeRequest,
};
use rusoto_logs::CloudWatchLogsClient;
use rusoto_s3::S3Client;
use std::time::Duration;

lazy_static! {
    /// AWS Lambda function async invocation.
    pub static ref FLOCK_LAMBDA_ASYNC_CALL: String = "Event".to_string();
    /// AWS Lambda function sync invocation.
    pub static ref FLOCK_LAMBDA_SYNC_CALL: String = "RequestResponse".to_string();
    /// AWS Lambda function maximum error retry.
    pub static ref FLOCK_LAMBDA_MAX_RETRIES: usize = FLOCK_CONF["lambda"]["max_invoke_retries"].parse::<usize>().unwrap();

    /// Flock sync invocation granularity.
    pub static ref FLOCK_SYNC_GRANULE_SIZE: usize = FLOCK_CONF["lambda"]["sync_granule"].parse::<usize>().unwrap();
    /// Flock async invocation granularity.
    pub static ref FLOCK_ASYNC_GRANULE_SIZE: usize = FLOCK_CONF["lambda"]["async_granule"].parse::<usize>().unwrap();

    /// Flock s3 key prefix.
    pub static ref FLOCK_S3_KEY: String = FLOCK_CONF["flock"]["s3_key"].to_string();
    /// Flock S3 bucket name.
    pub static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["flock"]["s3_bucket"].to_string();

    /// Flock's AWS S3 client.
    pub static ref FLOCK_S3_CLIENT: S3Client = S3Client::new(Region::default());
    static ref FLOCK_LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
    static ref FLOCK_WATCHLOGS_CLIENT: CloudWatchLogsClient = CloudWatchLogsClient::new(Region::default());
}

/// Set the lambda function's concurrency.
/// <https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html>
pub async fn set_lambda_concurrency(function_name: String, concurrency: i64) -> Result<()> {
    let request = PutFunctionConcurrencyRequest {
        function_name,
        reserved_concurrent_executions: concurrency,
    };
    let concurrency = FLOCK_LAMBDA_CLIENT
        .put_function_concurrency(request)
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?;
    assert_ne!(concurrency.reserved_concurrent_executions, Some(0));
    Ok(())
}

/// Fetch the lambda function's latest log.
pub async fn fetch_aws_watchlogs(group: &str, mtime: std::time::Duration) -> Result<()> {
    let mut logged = false;
    let timeout = parse_duration("1min").unwrap();
    let sleep_for = parse_duration("5s").ok();
    let mut token: Option<String> = None;
    let mut req = tail::create_filter_request(group, mtime, None, token);
    loop {
        if logged {
            break;
        }

        match fetch_logs(&FLOCK_WATCHLOGS_CLIENT, req, timeout)
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?
        {
            AWSResponse::Token(x) => {
                info!("Got a Token response");
                logged = true;
                token = Some(x);
                req = tail::create_filter_request(group, mtime, None, token);
            }
            AWSResponse::LastLog(t) => match sleep_for {
                Some(x) => {
                    info!("Got a lastlog response");
                    token = None;
                    req = tail::create_filter_from_timestamp(group, t, None, token);
                    info!("Waiting {:?} before requesting logs again...", x);
                    tokio::time::sleep(x).await;
                }
                None => break,
            },
        };
    }

    Ok(())
}

/// Invoke the lambda function with the given payload.
///
/// # Arguments
/// * `function_name` - The name of the lambda function.
/// * `payload` - The payload to be passed to the lambda function.
/// * `invocation_type` - The invocation type of the lambda function.
///   - `Event` - Asynchronous invocation.
///   - `RequestResponse` - Synchronous invocation.
///
/// # Returns
/// The result of the invocation.
pub async fn invoke_lambda_function(
    function_name: String,
    payload: Option<Bytes>,
    invocation_type: String,
) -> Result<InvocationResponse> {
    let request = InvocationRequest {
        function_name,
        payload,
        invocation_type: Some(invocation_type.clone()),
        ..Default::default()
    };

    if invocation_type == *FLOCK_LAMBDA_ASYNC_CALL {
        let response = FLOCK_LAMBDA_CLIENT
            .invoke(request)
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;
        Ok(response)
    } else {
        // Error retries and exponential backoff in AWS Lambda
        let mut retries = 0;
        loop {
            let response = FLOCK_LAMBDA_CLIENT
                .invoke(request.clone())
                .await
                .map_err(|e| FlockError::AWS(e.to_string()))?;
            if response.function_error.is_none() {
                return Ok(response);
            }
            if retries > 0 {
                if response.payload.is_some() {
                    info!(
                        "Function invocation error: {:?}",
                        serde_json::from_slice::<serde_json::Value>(&response.payload.unwrap())
                    );
                }
                info!("Retrying invocation...");
            }

            tokio::time::sleep(Duration::from_millis(2_u64.pow(retries) * 100)).await;

            retries += 1;

            if retries as usize > *FLOCK_LAMBDA_MAX_RETRIES {
                return Err(FlockError::AWS(format!(
                    "Sync invocation failed after {} retries",
                    *FLOCK_LAMBDA_MAX_RETRIES
                )));
            }
        }
    }
}

/// Creates a single lambda function using bootstrap.zip in Amazon S3.
pub async fn create_lambda_function(
    ctx: &ExecutionContext,
    memory_size: Option<i64>,
    debug: bool,
) -> Result<String> {
    let func_name = ctx.name.clone();
    if FLOCK_LAMBDA_CLIENT
        .get_function(GetFunctionRequest {
            function_name: ctx.name.clone(),
            ..Default::default()
        })
        .await
        .is_ok()
    {
        let conf = FLOCK_LAMBDA_CLIENT
            .update_function_code(UpdateFunctionCodeRequest {
                function_name: func_name.clone(),
                s3_bucket: Some(FLOCK_S3_BUCKET.clone()),
                s3_key: Some(FLOCK_S3_KEY.clone()),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?;
        conf.function_name
            .ok_or_else(|| FlockError::Internal("No function name!".to_string()))
    } else {
        let conf = FLOCK_LAMBDA_CLIENT
            .create_function(CreateFunctionRequest {
                code: FunctionCode {
                    s3_bucket: Some(FLOCK_S3_BUCKET.clone()),
                    s3_key: Some(FLOCK_S3_KEY.clone()),
                    ..Default::default()
                },
                function_name: func_name.clone(),
                handler: DeployConfig::handler(),
                role: DeployConfig::role().await,
                runtime: DeployConfig::runtime(),
                memory_size,
                environment: DeployConfig::environment(ctx, debug),
                timeout: Some(900),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?;
        conf.function_name
            .ok_or_else(|| FlockError::Internal("No function name!".to_string()))
    }
}
