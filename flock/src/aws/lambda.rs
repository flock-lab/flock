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

//! This crate contains all wrapped functions of the AWS Lambda services.

use crate::configs::*;
use crate::error::{FlockError, Result};
use crate::runtime::context::ExecutionContext;
use bytes::Bytes;
use log::info;
use rusoto_lambda::{
    CreateFunctionRequest, GetFunctionRequest, InvocationRequest, InvocationResponse, Lambda,
    PutFunctionConcurrencyRequest, UpdateFunctionCodeRequest,
};
use std::time::Duration;

/// Sets the lambda function's concurrency.
///
/// # Arguments
/// * `function_name` - The name of the lambda function.
/// * `concurrency` - The concurrency of the lambda function.
///
/// <https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html>
pub async fn set_concurrency(function_name: &str, concurrency: i64) -> Result<()> {
    let request = PutFunctionConcurrencyRequest {
        function_name:                  function_name.to_owned(),
        reserved_concurrent_executions: concurrency,
    };
    let concurrency = FLOCK_LAMBDA_CLIENT
        .put_function_concurrency(request)
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?;
    assert_ne!(concurrency.reserved_concurrent_executions, Some(0));
    Ok(())
}

/// Invokes the lambda function with the given payload.
///
/// # Arguments
/// * `function_name` - The name of the lambda function.
/// * `payload` - The payload to be passed to the lambda function.
/// * `invocation_type` - The invocation type of the lambda function.
///   - `Event`: Asynchronous invocation.
///   - `RequestResponse`: Synchronous invocation.
///
/// # Returns
/// The result of the invocation.
pub async fn invoke_function(
    function_name: &str,
    invocation_type: &str,
    payload: Option<Bytes>,
) -> Result<InvocationResponse> {
    let request = InvocationRequest {
        function_name: function_name.to_owned(),
        invocation_type: Some(invocation_type.to_owned()),
        payload,
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
            match FLOCK_LAMBDA_CLIENT
                .invoke(request.clone())
                .await
                .map_err(|e| FlockError::AWS(e.to_string()))
            {
                Ok(response) => {
                    if response.function_error.is_none() {
                        return Ok(response);
                    } else {
                        info!(
                            "Function execution error: {}, details: {:?}",
                            response.function_error.unwrap(),
                            serde_json::from_slice::<serde_json::Value>(&response.payload.unwrap())
                        );
                    }
                }
                Err(e) => {
                    info!("Function invocation error: {}", e);
                }
            }

            info!("Retrying {} function invocation...", function_name);
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

/// Creates a single lambda function.
///
/// # Arguments
/// * `ctx` - The execution context.
/// * `memory_size` - The memory size of the lambda function.
/// * `architecture` - The architecture of the lambda function.
///
/// # Returns
/// The name of the created lambda function.
pub async fn create_function(
    ctx: &ExecutionContext,
    memory_size: i64,
    architecture: &str,
) -> Result<String> {
    let func_name = ctx.name.clone();
    let flock_s3_key = if architecture == "x86_64" {
        FLOCK_S3_X86_64_KEY.clone()
    } else {
        FLOCK_S3_ARM_64_KEY.clone()
    };

    let mut conf = AwsLambdaConfig::try_new().await?;
    conf.set_memory_size(memory_size);
    conf.set_function_spec(ctx);
    conf.set_architectures(vec![architecture.to_string()]);
    conf.set_code(&flock_s3_key);

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
                architectures: conf.architectures,
                function_name: func_name.clone(),
                s3_bucket: Some(FLOCK_S3_BUCKET.clone()),
                s3_key: Some(flock_s3_key),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;
        conf.function_name
            .ok_or_else(|| FlockError::AWS("No function name!".to_string()))
    } else {
        let resp = FLOCK_LAMBDA_CLIENT
            .create_function(CreateFunctionRequest {
                architectures: conf.architectures,
                function_name: conf.function_name,
                code: conf.code,
                handler: conf.handler,
                runtime: conf.runtime,
                role: conf.role,
                vpc_config: conf.vpc_config,
                environment: conf.environment,
                timeout: conf.timeout,
                memory_size: conf.memory_size,
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;

        resp.function_name
            .ok_or_else(|| FlockError::AWS("No function name!".to_string()))
    }
}
