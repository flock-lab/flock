// Copyright 2020 UMD Database Group. All Rights Reserved.
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

use lambda::{handler_fn, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[derive(Debug, Default, Serialize, Deserialize)]
// One record
struct JoinRecord {
    result_cols: HashMap<String, String>,
    join_cols:   HashMap<String, String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct JoinArgs {
    join_method:  String, // "0": Nested loop; "1": Hash join
    join_type:    String, // Inner, Left, Right, Full
    left_stream:  String,
    left_attr:    String,
    right_stream: String,
    right_attr:   String,
    op:           String, // "=", ">", "<"
}
#[derive(Debug, Default, Serialize, Deserialize)]
struct JoinOutput {
    results: Vec<HashMap<String, String>>,
}

fn generate_record(
    left_record: &JoinRecord,
    right_record: &JoinRecord,
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

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let end: bool = serde_json::from_value(event["end"].clone()).unwrap();
    if end {
        return Ok(json!(""));
    }
    let mut join_output: Vec<HashMap<String, String>> = Vec::new();

    let join_args: JoinArgs = serde_json::from_value(event["join_args"].clone()).unwrap();
    let stream_data: HashMap<String, Value> =
        serde_json::from_value(event["data"].clone()).unwrap();

    let left_data: Vec<JoinRecord> =
        serde_json::from_value(stream_data[&join_args.left_stream].clone()).unwrap();
    let right_data: Vec<JoinRecord> =
        serde_json::from_value(stream_data[&join_args.right_stream].clone()).unwrap();
    // Join
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
                return Ok(json!(join_output));
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
                return Ok(json!(join_output));
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
                return Ok(json!(join_output));
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
                return Ok(json!(join_output));
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
                return Ok(json!(join_output));
            }
            _ => {}
        },
        "Left" => {}
        "Right" => {}
        "Full" => {}
        _ => {}
    }
    Ok(json!(join_output))
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
// serde_json::from_slice(include_bytes!("../sche_output2.json")).unwrap();
//         println!("event:{}\n", event);

//         let res = handler(event, Context::default()).await.ok().unwrap();
//         println!("res: {}", res);
//         //
//         serde_json::to_writer(&File::create("nested_join_output.json").ok().
// unwrap(), &res);     }
// }
