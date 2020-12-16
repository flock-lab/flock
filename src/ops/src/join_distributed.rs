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

extern crate rusoto_core;
extern crate rusoto_kinesis;

use bytes::Bytes;
use lambda::{handler_fn, Context};
use rusoto_core::{Region, RusotoError};
use rusoto_kinesis::{Kinesis, KinesisClient, PutRecordError, PutRecordInput};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

lazy_static! {
    static ref STREAM1_BATCHES: Mutex<HashMap<i32, VecDeque<ProjectionOutputMsg>>> =
        Mutex::new(HashMap::new());
}
lazy_static! {
    static ref STREAM2_BATCHES: Mutex<HashMap<i32, VecDeque<ProjectionOutputMsg>>> =
        Mutex::new(HashMap::new());
}
lazy_static! {
    static ref BATCH_COMPLETENESS: Mutex<HashMap<i32, bool>> = Mutex::new(HashMap::new());
}

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

#[derive(Debug, Default, Serialize, Deserialize)]
struct testMag {
    msg: String,
}

// #[derive(Debug, Default, Serialize, Deserialize)]
// // One record
// struct JoinRecord {
//     result_cols: HashMap<String, String>,
//     join_cols: HashMap<String, String>,
// }
#[derive(Debug, Default, Serialize, Deserialize)]
struct JoinOutput {
    results: Vec<HashMap<String, String>>,
}

fn generate_record(
    left_record: &ProjectionOutputRecord,
    right_record: &ProjectionOutputRecord,
    join_args: &JoinArgs,
) -> HashMap<String, String> {
    let mut record_hashmap: HashMap<String, String> = HashMap::new();

    for (k, v) in left_record.result_cols.iter() {
        record_hashmap.entry(k.clone()).or_insert_with(|| v.clone());
    }
    for (k, v) in right_record.result_cols.iter() {
        if record_hashmap.contains_key(k) {
            let new_k: String = join_args.right_stream.clone() + "." + k;
            record_hashmap.insert(new_k, v.clone());
        } else {
            record_hashmap.insert(k.clone(), v.clone());
        }
    }
    record_hashmap
}

fn join(
    join_args: &JoinArgs,
    left_data: &[ProjectionOutputRecord],
    right_data: &[ProjectionOutputRecord],
) -> Vec<HashMap<String, String>> {
    let mut join_output: Vec<HashMap<String, String>> = Vec::new();
    match join_args.join_type.as_str() {
        "Inner" => match join_args.op.as_str() {
            "=" => {
                for left_record in left_data.iter() {
                    for right_record in right_data.iter() {
                        if left_record.join_cols[&join_args.left_attr]
                            == right_record.join_cols[&join_args.right_attr]
                        {
                            let record_hashmap =
                                generate_record(left_record, right_record, &join_args);
                            join_output.push(record_hashmap);
                        }
                    }
                }
                join_output
            }
            ">" => {
                for left_record in left_data.iter() {
                    for right_record in right_data.iter() {
                        if left_record.join_cols[&join_args.left_attr]
                            > right_record.join_cols[&join_args.right_attr]
                        {
                            let record_hashmap =
                                generate_record(left_record, right_record, &join_args);
                            join_output.push(record_hashmap);
                        }
                    }
                }
                join_output
            }
            "<" => {
                for left_record in left_data.iter() {
                    for right_record in right_data.iter() {
                        if left_record.join_cols[&join_args.left_attr]
                            < right_record.join_cols[&join_args.right_attr]
                        {
                            let record_hashmap =
                                generate_record(left_record, right_record, &join_args);
                            join_output.push(record_hashmap);
                        }
                    }
                }
                join_output
            }
            ">=" => {
                for left_record in left_data.iter() {
                    for right_record in right_data.iter() {
                        if left_record.join_cols[&join_args.left_attr]
                            >= right_record.join_cols[&join_args.right_attr]
                        {
                            let record_hashmap =
                                generate_record(left_record, right_record, &join_args);
                            join_output.push(record_hashmap);
                        }
                    }
                }
                join_output
            }
            "<=" => {
                for left_record in left_data.iter() {
                    for right_record in right_data.iter() {
                        if left_record.join_cols[&join_args.left_attr]
                            <= right_record.join_cols[&join_args.right_attr]
                        {
                            let record_hashmap =
                                generate_record(left_record, right_record, &join_args);
                            join_output.push(record_hashmap);
                        }
                    }
                }
                join_output
            }
            _ => join_output,
        },
        "Left" => join_output,
        "Right" => join_output,
        "Full" => join_output,
        _ => join_output,
    }
}

async fn handler(event: Value, _: Context) -> Result<(), Error> {
    let input: ProjectionOutputMsg = serde_json::from_value(event.clone()).unwrap();
    let mut join_output: Vec<HashMap<String, String>> = Vec::new();
    let batch_num = input.batch_num;
    println!(
        "batch num: {:?}, stream: {:?}, last batch? {:?}",
        batch_num, input.stream_name, input.is_last
    );

    BATCH_COMPLETENESS
        .lock()
        .unwrap()
        .entry(input.batch_num)
        .or_insert(false);
    STREAM1_BATCHES
        .lock()
        .unwrap()
        .entry(input.batch_num)
        // .or_insert(VecDeque::new());
        .or_insert_with(VecDeque::new);
    STREAM2_BATCHES
        .lock()
        .unwrap()
        .entry(input.batch_num)
        // .or_insert(VecDeque::new());
        .or_insert_with(VecDeque::new);

    if input.stream_name == "stream1" {
        // Join
        for right in STREAM2_BATCHES.lock().unwrap()[&input.batch_num].iter() {
            let mut new_rec = join(&input.join_args, &input.data, &right.data);
            join_output.append(&mut new_rec);
            // println!("1 join with 2, join output len: {}",
            // join_output.len());
        }
        // Put in deque?
        let batch_complete = BATCH_COMPLETENESS.lock().unwrap()[&input.batch_num];
        println!("in stream1, batch complete:{:}", batch_complete);
        if input.is_last {
            if batch_complete {
                STREAM1_BATCHES.lock().unwrap().remove(&input.batch_num);
                STREAM2_BATCHES.lock().unwrap().remove(&input.batch_num);
                BATCH_COMPLETENESS.lock().unwrap().remove(&input.batch_num);
            } else {
                BATCH_COMPLETENESS
                    .lock()
                    .unwrap()
                    .entry(input.batch_num)
                    .and_modify(|e| *e = true)
                    .or_insert(true);
                STREAM1_BATCHES
                    .lock()
                    .unwrap()
                    .entry(input.batch_num)
                    .or_insert_with(VecDeque::new)
                    // .or_insert(VecDeque::new())
                    .push_back(input);
            }
        } else if !batch_complete {
            STREAM1_BATCHES
                .lock()
                .unwrap()
                .entry(input.batch_num)
                // .or_insert(VecDeque::new())
                .or_insert_with(VecDeque::new)
                .push_back(input);
        }
    } else {
        // Join
        for left in STREAM1_BATCHES.lock().unwrap()[&input.batch_num].iter() {
            let mut new_rec = join(&input.join_args, &left.data, &input.data);
            join_output.append(&mut new_rec);
            // println!(
            //     "2 join with 1, new_rec: {}, join output len: {} ",
            //     new_rec.len(),
            //     join_output.len()
            // );
            // println!("left data: {:?}", left.data);
        }
        let batch_complete = BATCH_COMPLETENESS.lock().unwrap()[&input.batch_num];
        println!("in stream2, batch complete:{:}", batch_complete);
        // Put in deque?
        if input.is_last {
            if batch_complete {
                STREAM1_BATCHES.lock().unwrap().remove(&input.batch_num);
                STREAM2_BATCHES.lock().unwrap().remove(&input.batch_num);
                BATCH_COMPLETENESS.lock().unwrap().remove(&input.batch_num);
            } else {
                BATCH_COMPLETENESS
                    .lock()
                    .unwrap()
                    .entry(input.batch_num)
                    .and_modify(|e| *e = true)
                    .or_insert(true);
                STREAM2_BATCHES
                    .lock()
                    .unwrap()
                    .entry(input.batch_num)
                    .or_insert_with(VecDeque::new)
                    // .or_insert(VecDeque::new())
                    .push_back(input);
            }
        } else if !batch_complete {
            STREAM2_BATCHES
                .lock()
                .unwrap()
                .entry(input.batch_num)
                // .or_insert(VecDeque::new())
                .or_insert_with(VecDeque::new)
                .push_back(input);
        }
    }

    // let batch_num = input.batch_num;
    // let len1 = STREAM1_BATCHES.lock().unwrap()[&batch_num.clone()].len();
    // let len2 = STREAM2_BATCHES.lock().unwrap()[&batch_num.clone()].len();

    let res = JoinOutput {
        results: join_output,
    };

    // let res = testMag {
    //     msg: "test".to_string(),
    // };
    // println!("{:}", json!(res));
    // Write to Kinesis Data streams
    let kinesis_client = KinesisClient::new(Region::UsEast1);
    let input_record = PutRecordInput {
        // data: serde_json::to_vec(res),
        data: Bytes::copy_from_slice(json!(res).to_string().as_bytes()),
        partition_key: "lambda_join".to_string(),
        stream_name: "joinResults".to_string(),
        ..PutRecordInput::default()
    };
    let result = kinesis_client
        .put_record(input_record.clone())
        .await
        .unwrap();
    println!("Put record result: {:?}", result);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use lambda::Context;
//     // use serde::{Deserialize, Serialize};
//     use std::fs::File;
//     #[tokio::test]
//     async fn join_test() {
//         let event =
//
// serde_json::from_slice(include_bytes!("../test_output/proj_output_0.json")).
// unwrap();         // println!("event:{}\n", event);

//         let res = handler(event, Context::default()).await.ok().unwrap();
//     }
// }
