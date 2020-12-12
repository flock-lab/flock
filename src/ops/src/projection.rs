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
    message:     Vec<ProjectionOutputRecord>,
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

async fn handler(input: ProjectionInput, _: Context) -> Result<Value, Error> {
    // async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    // let input: ProjectionInput = serde_json::from_value(event.clone()).unwrap();
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
        message:     re,
        stream_name: input.stream_name.clone(),
        join_args:   input.join_args,
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
//     use serde::{Deserialize, Serialize};
//     use std::fs::File;

//     fn generate_stream_data(
//         start_record: usize,
//         num_records: usize,
//         prj_cols: Vec<&str>,
//         join_cols: Vec<&str>,
//         stream_name: String,
//     ) -> Result<ProjectionInput, Error> {
//         // let prj_cols = vec!["attr_1", "attr_2", "attr_3", "attr_4"];
//         let join_args = JoinArgs {
//             join_method:  "0".to_string(),     // "0": Nested loop; "1": Hash
// join             join_type:    "Inner".to_string(), // Inner, Left ...
//             left_stream:  "stream1".to_string(),
//             left_attr:    "attr_1".to_string(),
//             right_stream: "stream2".to_string(),
//             right_attr:   "attr_1".to_string(),
//             op:           "=".to_string(), // "=", ">", "<"
//         };
//         let projections: Vec<String> = prj_cols.iter().map(|s|
// s.to_string()).collect();

//         // let join_cols = vec!["attr_1"];
//         let joins: Vec<String> = join_cols.iter().map(|s|
// s.to_string()).collect();

//         let mut data: Vec<String> = Vec::new();
//         for i in start_record..num_records {
//             let i_str: String = i.to_string();
//             let record = Event {
//                 attr_1: "my_attr_1_".to_string() + &i_str,
//                 attr_2: "my_attr_2_".to_string() + &i_str,
//                 attr_3: "my_attr_3_".to_string() + &i_str,
//                 attr_4: "my_attr_4_".to_string() + &i_str,
//                 attr_5: "my_attr_5_".to_string() + &i_str,
//                 attr_6: "my_attr_6_".to_string() + &i_str,
//                 attr_7: "my_attr_7_".to_string() + &i_str,
//                 attr_8: "my_attr_8_".to_string() + &i_str,
//             };
//             // println!("Event:\n{:?}", record);

//             let j = serde_json::to_string(&record);
//             match j {
//                 Ok(rec_j) => {
//                     println!("Event:\n{:?}", rec_j);
//                     data.push(rec_j);
//                 }
//                 _ => {}
//             }
//         }
//         Ok(ProjectionInput {
//             projection:  projections,
//             stream_name: stream_name,
//             data:        data,
//             join_cols:   joins,
//             join_args:   join_args,
//         })
//     }

//     #[tokio::test]
//     async fn test_lambda_handler() {
//         let proj_input = generate_stream_data(
//             0,
//             5,
//             vec!["attr_1", "attr_2", "attr_3", "attr_4"],
//             vec!["attr_1"],
//             "stream1".to_string(),
//         )
//         .ok()
//         .unwrap();
//         serde_json::to_writer(
//             &File::create("proj_input_stream1.json").ok().unwrap(),
//             &proj_input,
//         );
//         // Check the result is ok
//         let re = handler(proj_input, Context::default()).await.ok().unwrap();

//         serde_json::to_writer(&File::create("proj_output_stream1.json").ok().
// unwrap(), &re);         println!("re:\n{:#?}", re);
//     }
//     #[tokio::test]
//     async fn two_streams() {
//         // Stream 1
//         let proj_input = generate_stream_data(
//             0,
//             5,
//             vec!["attr_1", "attr_2", "attr_3", "attr_4"],
//             vec!["attr_1"],
//             "stream1".to_string(),
//         )
//         .ok()
//         .unwrap();
//         serde_json::to_writer(
//             &File::create("proj_input_stream1.json").ok().unwrap(),
//             &proj_input,
//         );
//         // Check the result is ok
//         let re = handler(proj_input, Context::default()).await.ok().unwrap();

//         serde_json::to_writer(&File::create("proj_output_stream1.json").ok().
// unwrap(), &re);         println!("re:\n{:#?}", re);

//         // Stream 2
//         let proj_input2 = generate_stream_data(
//             3,
//             7,
//             vec!["attr_1", "attr_5", "attr_6", "attr_7"],
//             vec!["attr_1"],
//             "stream2".to_string(),
//         )
//         .ok()
//         .unwrap();
//         serde_json::to_writer(
//             &File::create("proj_input_stream2.json").ok().unwrap(),
//             &proj_input2,
//         );
//         // Check the result is ok
//         let re = handler(proj_input2,
// Context::default()).await.ok().unwrap();

//         serde_json::to_writer(&File::create("proj_output_stream2.json").ok().
// unwrap(), &re);         println!("re:\n{:#?}", re);
//     }
// }
