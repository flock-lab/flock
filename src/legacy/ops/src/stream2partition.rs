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
use aws_lambda_events::event::kinesis::KinesisEvent;
use chrono::{DateTime, Utc};
use lambda::{handler_fn, Context};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rusoto_core::Region;
use rusoto_stepfunctions::{StartExecutionInput, StepFunctions, StepFunctionsClient};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::File;
use std::str::from_utf8;
use std::sync::Mutex;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

lazy_static! {
    static ref BATCH_NUM: Mutex<i32> = Mutex::new(0);
}

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

    is_last:   bool, // if this is the last batch of the key
    key:       String,
    batch_num: i32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct JoinArgs {
    join_method:  String, // "0": Nested loop; "1": Hash join
    join_type:    String, // Inner, Left ...
    left_stream:  String,
    left_attr:    String,
    right_stream: String,
    right_attr:   String,
    op:           String, // "=", ">", "<"
}

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    attr_1: String,
    attr_2: String,
    attr_3: String,
    attr_4: String,
    attr_5: String,
    attr_6: String,
    attr_7: String,
    attr_8: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn handler(event: KinesisEvent, _: Context) -> Result<(), Error> {
    let mut batch_num = BATCH_NUM.lock().unwrap();
    *batch_num += 1;
    let my_batch = *batch_num;
    println!("my batch: {}", my_batch);

    let SIZE_LIMIT = 230 * 1024;

    let join_key = "attr_1";
    let mut partition: HashMap<String, Vec<String>> = HashMap::new(); // key partition

    let mut rec_num = 0;
    let mut total_size = 0;
    for record in event.records {
        let a = from_utf8(&record.kinesis.data.0)
            .map(|s| s.to_owned())
            .unwrap_or_else(|err| format!("expected utf8 data: {}", err));

        total_size += a.len();
        let record: Value = serde_json::from_str(&a)?;
        // println!(
        //     "string length: {}, size of string: {}",
        //     a.chars().count(),
        //     &a.len()
        // );
        rec_num += 1;

        partition
            .entry(record[join_key].to_string())
            .or_insert_with(Vec::new)
            .push(record.to_string());
        // println!("{:?}", record[join_key]);
        // break;
    }
    println!("rec num: {}, total size: {}", rec_num, total_size);
    let mut output_n = 0;

    // let mut proj_input: ProjectionInput;
    // Send data in batches, batch size cannot exceed 250kb
    for (key, recs) in &partition {
        let mut batch_size = 0;
        let mut res: Vec<String> = Vec::new();

        for r in recs.iter() {
            if batch_size + r.len() > SIZE_LIMIT {
                let data = res.clone();
                let proj_input = get_proj_input(data, key.clone(), false, my_batch);

                // // For local testing
                // serde_json::to_writer(
                //     &File::create(format!("./test_output/stream1_output_{}.json", output_n))
                //         .ok()
                //         .unwrap(),
                //     &proj_input,
                // );
                // output_n += 1;

                // invoke_step_function(proj_input);
                println!(
                    "stream name: {}, key: {}, batch_size: {}, islast: {}",
                    proj_input.stream_name, key, batch_size, proj_input.is_last
                );

                batch_size = 0;
                res.clear();
            }
            res.push(r.clone());
            batch_size += r.len();
        }
        let proj_input = get_proj_input(res, key.clone(), true, my_batch);

        // // For local testing
        // serde_json::to_writer(
        //     &File::create(format!("./test_output/stream1_output_{}.json", output_n))
        //         .ok()
        //         .unwrap(),
        //     &proj_input,
        // );
        // output_n += 1;

        println!(
            "stream name: {}, key: {}, batch_size: {}, islast: {}",
            proj_input.stream_name, key, batch_size, proj_input.is_last
        );
        // println!("Partitioned data: {:?}", proj_input);
        // invoke_step_function(proj_input);
    }
    Ok(())
}

fn get_proj_input(data: Vec<String>, key: String, islast: bool, batch: i32) -> ProjectionInput {
    let proj: Vec<String> = vec!["attr_1", "attr_5", "attr_6", "attr_7"]
        .into_iter()
        .map(|s| s.to_owned())
        .collect();
    let jargs = JoinArgs {
        join_method:  "0".to_string(),     // "0": Nested loop; "1": Hash join
        join_type:    "Inner".to_string(), // Inner, Left ...
        left_stream:  "stream1".to_string(),
        left_attr:    "attr_1".to_string(),
        right_stream: "stream2".to_string(),
        right_attr:   "attr_1".to_string(),
        op:           "=".to_string(), // "=", ">", "<"
    };
    ProjectionInput {
        projection: proj,
        stream_name: "stream2".to_string(),
        data,                                  // [{}, {}, ...]
        join_cols: vec!["attr_1".to_string()], // join on ...
        join_args: jargs,
        is_last: islast,
        key,
        batch_num: batch,
    }
}

async fn invoke_step_function(input: ProjectionInput) {
    let now: DateTime<Utc> = Utc::now();
    let rand_string: String = now.to_rfc3339();

    // println!("{}", rand_string);
    let request = StartExecutionInput {
        input:             Some(json!(input).to_string()),
        name:              Some(rand_string),
        state_machine_arn: "arn:aws:states:us-east-1:942368842860:stateMachine:DistrJoin2Streams"
            .to_string(),
    };
    let client = StepFunctionsClient::new(Region::UsEast1);
    client.start_execution(request).await.unwrap();
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::fs::File;

//     #[tokio::test]
//     async fn stream1() {
//         let event: KinesisEvent =
//
// serde_json::from_slice(include_bytes!("../test_output/events.json"))
//                 .expect("invalid kinesis event");
//         handler(event, Context::default()).await.ok().unwrap();
//     }
// }
