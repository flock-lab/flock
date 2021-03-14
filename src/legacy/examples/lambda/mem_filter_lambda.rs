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

use aws_lambda_events::event::kinesis::KinesisEvent;

use datafusion::physical_plan::{common, ExecutionPlan};

use arrow::util::pretty;
use lambda::{handler_fn, Context};
use serde_json::Value;

use std::sync::Once;

use runtime::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

/// Initialize the lambda function once and only once.
static INIT: Once = Once::new();

/// Empty Plan before initializing the cloud environment.
static mut PLAN: LambdaPlan = LambdaPlan::None;

async fn handler(event: KinesisEvent, _: Context) -> Result<Value> {
    let (schema, plan) = init_plan!(INIT, PLAN);

    let record_batch = kinesis::to_batch(event);
    let result = exec_plan!(plan, vec![record_batch]);
    pretty::print_batches(&result)?;

    Ok(Payload::from(&result[0], schema, Uuid::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn filter_test() {
        let plan_key = "PLAN_JSON";
        let plan_val = r#"
        {
            "execution_plan":"filter_exec",
            "predicate":{
            "physical_expr":"binary_expr",
            "left":{
                "physical_expr":"column",
                "name":"c2"
            },
            "op":"Lt",
            "right":{
                "physical_expr":"cast_expr",
                "expr":{
                    "physical_expr":"literal",
                    "value":{
                        "Int64":99
                    }
                },
                "cast_type":"Float64"
            }
            },
            "input":{
            "execution_plan":"memory_exec",
            "schema":{
                "fields":[
                    {
                        "name":"c1",
                        "data_type":"Int64",
                        "nullable":false,
                        "dict_id":0,
                        "dict_is_ordered":false
                    },
                    {
                        "name":"c2",
                        "data_type":"Float64",
                        "nullable":false,
                        "dict_id":0,
                        "dict_is_ordered":false
                    },
                    {
                        "name":"c3",
                        "data_type":"Utf8",
                        "nullable":false,
                        "dict_id":0,
                        "dict_is_ordered":false
                    }
                ],
                "metadata":{

                }
            },
            "projection":[
                0,
                1,
                2
            ]
            }
        }
        "#;
        std::env::set_var(plan_key, plan_val);

        let data = include_str!("example-kinesis-event.json");
        let event: KinesisEvent = serde_json::from_str(data).unwrap();
        handler(event, Context::default()).await.ok().unwrap();
    }
}
