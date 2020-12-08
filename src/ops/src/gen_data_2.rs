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

use aws_lambda_events::event::kinesis::KinesisEvent;
use lambda::{handler_fn, Context};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rusoto_core::Region;
use rusoto_stepfunctions::{StartExecutionInput, StepFunctions, StepFunctionsClient};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::from_utf8;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;
#[derive(Debug, Default, Serialize, Deserialize)]
struct Input {
    input: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProjectionInput {
    projection:  Vec<String>,
    stream_name: String,
    data:        Vec<String>, // [{}, {}, ...]
    join_cols:   Vec<String>, // join on ...
    join_args:   JoinArgs,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct JoinArgs {
    join_method:  String, // "0": Nested loop; "1": Hash join
    join_type:    String, // Inner, Left ...
    left_stream:  String,
    left_attr:    String,
    right_stream: String,
    right_attr:   String,
    op:           String, // "=", ">", "<"
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn handler(event: KinesisEvent, _: Context) -> Result<Value, Error> {
    let mut res: Vec<String> = Vec::new();
    for record in event.records {
        let a = from_utf8(&record.kinesis.data.0)
            .map(|s| s.to_owned())
            .unwrap_or_else(|err| format!("expected utf8 data: {}", err));
        res.push(a);
    }
    let jargs = JoinArgs {
        join_method:  "0".to_string(),     // "0": Nested loop; "1": Hash join
        join_type:    "Inner".to_string(), // Inner, Left ...
        left_stream:  "stream1".to_string(),
        left_attr:    "attr_1".to_string(),
        right_stream: "stream2".to_string(),
        right_attr:   "attr_1".to_string(),
        op:           "=".to_string(), // "=", ">", "<"
    };

    let proj: Vec<String> = vec!["attr_1", "attr_5", "attr_6", "attr_7"]
        .into_iter()
        .map(|s| s.to_owned())
        .collect();
    let proj_input = ProjectionInput {
        projection:  proj,
        stream_name: "stream2".to_string(),
        data:        res,                        // [{}, {}, ...]
        join_cols:   vec!["attr_1".to_string()], // join on ...
        join_args:   jargs,
    };

    let rand_string: String = thread_rng().sample_iter(&Alphanumeric).take(50).collect();

    // println!("{}", rand_string);
    let request = StartExecutionInput {
        input:             Some(json!(proj_input).to_string()),
        name:              Some(rand_string),
        state_machine_arn: "arn:aws:states:us-east-1:942368842860:stateMachine:InnerJoin2Streams"
            .to_string(),
    };
    let client = StepFunctionsClient::new(Region::UsEast1);
    client.start_execution(request).await.unwrap();
    Ok(json!(proj_input))
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::fs::File;

//     #[tokio::test]
//     async fn handler_handles() {
//         let event: KinesisEvent =
// serde_json::from_slice(include_bytes!("./example-event.json"))             
// .expect("invalid kinesis event");         let data = handler(event,
// Context::default()).await.ok().unwrap();

//         // assert!(handler(event, Context::default()).await.is_ok());
//         println!("{:?}", data);
//         // serde_json::to_writer(&File::create("gen_data1_output.json").ok().
//         // unwrap(), &data);
//     }
// }
