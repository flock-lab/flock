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

#[macro_use]
extern crate lazy_static;
use lambda::{handler_fn, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

lazy_static! {
    static ref STREAM_1: Mutex<VecDeque<Value>> = Mutex::new(VecDeque::new());
}

lazy_static! {
    static ref STREAM_2: Mutex<VecDeque<Value>> = Mutex::new(VecDeque::new());
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct JoinSchedulerOutput {
    end: bool,
    stream_data: HashMap<String, Value>,
    join_args: JoinArgs,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let stream_name = event["stream_name"].as_str().unwrap();
    let join_args: JoinArgs = serde_json::from_value(event["join_args"].clone()).unwrap();

    let mut res = JoinSchedulerOutput {
        end: false,
        stream_data: HashMap::new(),
        join_args: join_args,
    };

    match stream_name {
        "stream1" => {
            if STREAM_2.lock().unwrap().len() == 0 {
                STREAM_1.lock().unwrap().push_back(event);
                println!("\nStream 2 is empty\n");
                // CheckStream();

                return Ok(json!(JoinSchedulerOutput {
                    end: true,
                    ..Default::default()
                }));
            }

            let batch = STREAM_2.lock().unwrap().pop_front().unwrap();
            res.stream_data
                .insert("stream2".to_string(), batch["message"].clone());
            res.stream_data
                .insert("stream1".to_string(), event["message"].clone());
        }
        "stream2" => {
            // Another stream is not ready to join
            if STREAM_1.lock().unwrap().len() == 0 {
                STREAM_2.lock().unwrap().push_back(event);
                println!("\nStream 1 is empty\n");
                // CheckStream();

                return Ok(json!(JoinSchedulerOutput {
                    end: true,
                    ..Default::default()
                }));
            }

            let batch = STREAM_1.lock().unwrap().pop_front().unwrap();
            res.stream_data
                .insert("stream1".to_string(), batch["message"].clone());
            res.stream_data
                .insert("stream2".to_string(), event["message"].clone());
        }
        _ => {}
    }

    // CheckStream();
    Ok(json!(res))
}

fn CheckStream() {
    println!("In stream 1:\n");
    for v in STREAM_1.lock().unwrap().iter() {
        println!("{:?}", v);
    }
    println!("In stream 2:\n");
    for v in STREAM_2.lock().unwrap().iter() {
        println!("{:?}", v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda::Context;
    use serde::{Deserialize, Serialize};
    use std::fs::File;
    #[derive(Debug, Default, Serialize, Deserialize)]
    // One record
    struct ProjectionOutputRecord {
        result_cols: HashMap<String, String>,
        join_cols: HashMap<String, String>,
        // stream_name: String,
        // join_args: JoinArgs,
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct ProjectionOutputMsg {
        stream_name: String,
        message: Vec<ProjectionOutputRecord>,
        join_args: JoinArgs,
    }
    #[tokio::test]
    async fn stream_1() {
        let event = serde_json::from_slice(include_bytes!("../proj_output_stream1.json")).unwrap();
        println!("event:{}\n", event);

        let res = handler(event, Context::default()).await.ok().unwrap();
        println!("res: {}", res);
    }
    #[tokio::test]
    async fn stream_2() {
        let event = serde_json::from_slice(include_bytes!("../proj_output_stream2.json")).unwrap();
        println!("event:{}\n", event);

        let res = handler(event, Context::default()).await.ok().unwrap();
        println!("res: {}", res);
    }
}
