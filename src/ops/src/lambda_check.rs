// Copyright (c) 2020 UMD Database Group. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate rusoto_core;
extern crate rusoto_lambda;

use bytes::Bytes;
use hyper::body;
use hyper::Client;
use hyper_tls::HttpsConnector;
use lambda::{handler_fn, Context};
use rusoto_core::{Region, RusotoError};
use rusoto_lambda::{
    CreateFunctionRequest, FunctionCode, FunctionCodeLocation, GetFunctionConfigurationRequest,
    GetFunctionRequest, InvocationRequest, Lambda, LambdaClient,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Read;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[derive(Debug, Serialize, Deserialize)]
struct ProjectionOutputMsg {
    stream_name: String,
    data: Vec<ProjectionOutputRecord>,
    join_args: JoinArgs,

    key: String,
    is_last: bool,
    batch_num: i32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
// One record
struct ProjectionOutputRecord {
    result_cols: HashMap<String, String>,
    join_cols: HashMap<String, String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct JoinArgs {
    join_method: String, // "0": Nested loop; "1": Hash join
    join_type: String,   // Inner, Left ...
    left_stream: String,
    left_attr: String,
    right_stream: String,
    right_attr: String,
    op: String, // "=", ">", "<"
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let input: ProjectionOutputMsg = serde_json::from_value(event.clone()).unwrap();
    let clone_func_name = "join_distributed";

    println!("key: {}", input.key);
    let client = LambdaClient::new(Region::UsEast1);
    // let request = ListFunctionsRequest::default();

    let func_name: String = format!("{}_{}", clone_func_name, input.key);
    // let func_name: String = "join_scheduler".to_string();

    let req = GetFunctionConfigurationRequest {
        function_name: func_name.clone(),
        ..GetFunctionConfigurationRequest::default()
    };
    let mut func_arn: String = "".to_string();
    // let re = client.get_function_configuration(req);
    match client.get_function_configuration(req).await {
        Ok(func) => {
            // Find function, invoke.
            println!("{:#?}", func);
            func_arn = func.function_arn.unwrap();
            let request = InvocationRequest {
                function_name: func_name.clone(),
                invocation_type: Some("Event".to_string()),
                payload: Some(json!(event).to_string().into()),
                ..InvocationRequest::default()
            };
            let result = client.invoke(request).await;
            println!("result: {:#?}", result);
        }
        Err(e) => {
            // Doesn't exist, create one.
            println!("{:#?}", e);
            let req = GetFunctionRequest {
                function_name: clone_func_name.to_string(),
                ..GetFunctionRequest::default()
            };
            let res = client.get_function(req).await;

            // Download code
            match res {
                Ok(resp) => {
                    // println!("code info: {:?}", resp.configuration);
                    if let Some(code_location) = resp.code {
                        let url = code_location.location.unwrap();
                        println!("code loacation: {:?}", url);
                        let https = HttpsConnector::new();
                        let codeClient = Client::builder().build::<_, hyper::Body>(https);

                        let res = codeClient.get(url.parse()?).await?;
                        println!("code res: {:?}", res);
                        let body_bytes = body::to_bytes(res.into_body()).await?;
                        println!("code body_bytes: {:?}", body_bytes.len());

                        // Upload new function
                        let func_code = FunctionCode {
                            zip_file: Some(body_bytes),
                            ..FunctionCode::default()
                        };
                        let config = resp.configuration.unwrap();
                        let req = CreateFunctionRequest {
                            code: func_code,
                            function_name: func_name.clone(),
                            handler: config.handler.unwrap(),
                            role: config.role.unwrap(),
                            runtime: config.runtime.unwrap(),
                            ..CreateFunctionRequest::default()
                        };
                        let createresp = client.create_function(req).await.unwrap();
                        println!(
                            "New function arn: {}",
                            createresp.function_arn.clone().unwrap()
                        );
                        func_arn = createresp.function_arn.unwrap();

                        // Send data to new function
                        let request = InvocationRequest {
                            function_name: func_name.clone(),
                            invocation_type: Some("Event".to_string()),
                            payload: Some(json!(event).to_string().into()),
                            ..InvocationRequest::default()
                        };
                        let result = client.invoke(request).await;
                        // println!("result: {:#?}", result);
                    }
                }
                Err(e) => {
                    println!("{:#?}", e);
                }
            }
        }
    }
    Ok(json!({ "msg": func_arn }))
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::fs::File;

//     #[tokio::test]
//     async fn stream1() {
//         let event: Value =
//
// serde_json::from_slice(include_bytes!("../test_output/proj_output_0.json"))
//                 .expect("invalid kinesis event");
//         handler(event, Context::default()).await.ok().unwrap();
//     }
// }
