// Copyright (c) 2021 UMD Database Group. All Rights Reserved.
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

//! This is a playground for testing the system design ideas of Flock.

use driver::deploy::lambda;
use lazy_static::lazy_static;
use log::info;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{
    CreateFunctionRequest, DeleteFunctionRequest, FunctionCode, GetFunctionRequest,
    InvocationRequest, InvocationResponse, Lambda, LambdaClient, PutFunctionConcurrencyRequest,
};
use serde_json::json;
use serde_json::Value;
use structopt::StructOpt;

lazy_static! {
    static ref LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::UsEast1);
}

#[derive(Debug, StructOpt)]
struct PlaygroundOpt {
    /// Operation type
    #[structopt(short = "op", long = "ops_type", default_value = "scatter_gather_ops")]
    ops_type: String,

    /// Number of events generated
    #[structopt(short = "e", long = "events", default_value = "1000")]
    events: usize,

    /// Concurrency of the lambda function
    #[structopt(short = "c", long = "concurrency", default_value = "8")]
    concurrency: usize,
}

async fn create_function(func_name: &str) -> Result<String> {
    if LAMBDA_CLIENT
        .get_function(GetFunctionRequest {
            function_name: String::from(func_name),
            ..Default::default()
        })
        .await
        .is_ok()
    {
        // To avoid obsolete code on S3, remove the previous lambda function.
        LAMBDA_CLIENT
            .delete_function(DeleteFunctionRequest {
                function_name: String::from(func_name),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?;
    }

    match LAMBDA_CLIENT
        .create_function(CreateFunctionRequest {
            code: FunctionCode {
                s3_bucket:         Some("umd-flock".to_string()),
                s3_key:            Some(String::from(func_name)),
                s3_object_version: None,
                zip_file:          None,
                image_uri:         None,
            },
            function_name: String::from(func_name),
            handler: lambda::handler(),
            role: lambda::role().await,
            runtime: lambda::runtime(),
            ..Default::default()
        })
        .await
    {
        Ok(config) => {
            return config.function_name.ok_or_else(|| {
                FlockError::Internal("Unable to find lambda function arn.".to_string())
            })
        }
        Err(err) => {
            return Err(FlockError::Internal(format!(
                "Failed to create lambda function: {}",
                err
            )))
        }
    }
}

/// As traffic increases, Lambda increases the number of concurrent executions
/// of your functions. When a function is first invoked, the Lambda service
/// creates an instance of the function and runs the handler method to process
/// the event. After completion, the function remains available for a period of
/// time to process subsequent events. If other events arrive while the function
/// is busy, Lambda creates more instances of the function to handle these
/// requests concurrently.
async fn set_function_concurrency(func_name: &str, concurrency: usize) -> Result<()> {
    let concurrency = LAMBDA_CLIENT
        .put_function_concurrency(PutFunctionConcurrencyRequest {
            function_name:                  String::from(func_name),
            reserved_concurrent_executions: concurrency as i64,
        })
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?;
    assert_ne!(concurrency.reserved_concurrent_executions, Some(0));
    Ok(())
}

async fn invoke_function(func_name: &'static str, num_events: usize) -> Result<()> {
    let tasks = (0..num_events)
        .map(|i| {
            tokio::spawn(async move {
                // To invoke a function asynchronously, set InvocationType to Event.
                let response = match LAMBDA_CLIENT
                    .invoke(InvocationRequest {
                        function_name: func_name.to_string(),
                        payload: Some(serde_json::to_vec(&json!({ "val": i + 1 }))?.into()),
                        invocation_type: Some("Event".to_string()),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(response) => Ok(response),
                    Err(err) => Err(FlockError::Execution(format!(
                        "Lambda function execution failure: {}",
                        err
                    ))),
                };
                Ok(response?)
            })
        })
        // this collect *is needed* so that the join below can switch between tasks.
        .collect::<Vec<tokio::task::JoinHandle<Result<InvocationResponse>>>>();

    for task in tasks {
        // The HTTP status code is in the 200 range for a successful request.
        // - For the RequestResponse invocation type, this status code is 200.
        // - For the Event invocation type, this status code is 202.
        // - For the DryRun invocation type, the status code is 204.
        let response = task.await.expect("Lambda function execution failed.")?;
        match response.status_code {
            Some(200) => {
                info!(
                    "{:?}",
                    serde_json::from_slice::<Value>(&response.payload.ok_or_else(|| {
                        FlockError::Internal(
                            "Failed to parse the payload of the function response.".to_string(),
                        )
                    })?)?
                );
            }
            Some(202) => {
                info!(" [OK] Received status from async lambda function.");
            }
            _ => {
                panic!("Incorrect Lambda invocation!");
            }
        }
    }

    Ok(())
}

async fn scatter_gather_ops(num_events: usize, concurrency: usize) -> Result<()> {
    let func_names = vec!["flock_pg_scatter", "flock_pg_gather"];
    create_function(&func_names[0]).await?;
    create_function(&func_names[1]).await?;
    set_function_concurrency(&func_names[0], concurrency).await?;
    set_function_concurrency(&func_names[1], 1).await?;
    // invoke the first function in the scatter-gather pattern
    invoke_function(&func_names[0], num_events).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = PlaygroundOpt::from_args();
    println!("Playground with the following options: {:?}", opt);
    assert!(opt.events > 0);
    if opt.ops_type == "scatter_gather_ops" {
        scatter_gather_ops(opt.events, opt.concurrency).await?;
    } else {
        println!("Unknown operation type: {}", opt.ops_type);
    }
    Ok(())
}
