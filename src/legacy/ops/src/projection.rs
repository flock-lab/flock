// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
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
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::mem;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

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
#[derive(Debug, Default, Serialize, Deserialize)]
// One record
struct ProjectionOutputRecord {
    result_cols: HashMap<String, String>,
    join_cols:   HashMap<String, String>,
}
#[derive(Debug, Serialize, Deserialize)]
struct ProjectionOutputMsg {
    stream_name: String,
    data:        Vec<ProjectionOutputRecord>,
    join_args:   JoinArgs,

    key:       String,
    is_last:   bool,
    batch_num: i32,
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

// async fn handler(input: ProjectionInput, _: Context) -> Result<Value, Error>
// {
async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let input: ProjectionInput = serde_json::from_value(event).unwrap();
    let mut re: Vec<ProjectionOutputRecord> = Vec::new();

    for record in input.data {
        let mut output = ProjectionOutputRecord {
            result_cols: HashMap::new(),
            join_cols:   HashMap::new(),
            // stream_name: input.stream_name.clone(),
        };

        let record_json: Value = serde_json::from_str(&record)?;
        let obj: Map<String, Value> = record_json.as_object().unwrap().clone();

        for col in &input.projection {
            match obj[col].clone() {
                serde_json::Value::String(v) => {
                    output.result_cols.entry(col.clone()).or_insert(v);
                }
                serde_json::Value::Bool(v) => {
                    output
                        .result_cols
                        .entry(col.clone())
                        .or_insert_with(|| v.to_string());
                }
                serde_json::Value::Number(v) => {
                    output
                        .result_cols
                        .entry(col.clone())
                        .or_insert_with(|| v.to_string());
                }
                _ => {}
            }
        }
        for col in &input.join_cols {
            match obj[col].clone() {
                serde_json::Value::String(v) => {
                    output.join_cols.entry(col.clone()).or_insert(v);
                }
                serde_json::Value::Bool(v) => {
                    output
                        .join_cols
                        .entry(col.clone())
                        .or_insert_with(|| v.to_string());
                }
                serde_json::Value::Number(v) => {
                    output
                        .join_cols
                        .entry(col.clone())
                        .or_insert_with(|| v.to_string());
                }
                _ => {}
            }
        }
        re.push(output);
    }
    // Ok(re)
    // let re: ProjectionOutputMsg = { message: re };
    let res = json!(ProjectionOutputMsg {
        data:        re,
        stream_name: input.stream_name.clone(),
        join_args:   input.join_args,
        key:         input.key,
        is_last:     input.is_last,
        batch_num:   input.batch_num,
    });
    println!("res size: {}", mem::size_of_val(&res));
    Ok(res)
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
//     use rand::distributions::Alphanumeric;
//     use rand::{thread_rng, Rng};
//     use serde::{Deserialize, Serialize};
//     use std::fs::File;
//     #[tokio::test]
//     async fn join_test() {
//         let n: i32 = 1;
//         let input_file_name =
// format!("../test_output/stream1_output_{}.json", n);         let event =
//
// serde_json::from_slice(include_bytes!("../test_output/stream1_output_1.json"
// )).unwrap();         // println!("event:{}\n", event);

//         let res = handler(event, Context::default()).await.ok().unwrap();
//         // println!("res: {}", res);
//         //
//         serde_json::to_writer(
//             &File::create(format!("./test_output/proj_output_{}.json", n))
//                 .ok()
//                 .unwrap(),
//             &res,
//         );
//     }
// }
