// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
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

use datafusion::physical_plan::projection::ProjectionExec;

use datafusion::physical_plan::{common, ExecutionPlan, LambdaExecPlan};

use arrow::util::pretty;
use lambda::{handler_fn, Context};
use serde_json::Value;

use std::sync::Once;

use scq_lambda::dataframe::DataFrame;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

/// JSON representation of the physical plan.
const PLAN_JSON: &str = r#"
{
    "expr":[
       [
          {
             "physical_expr":"column",
             "name":"MAX(c1)"
          },
          "MAX(c1)"
       ],
       [
          {
             "physical_expr":"column",
             "name":"MIN(c2)"
          },
          "MIN(c2)"
       ],
       [
          {
             "physical_expr":"column",
             "name":"c3"
          },
          "c3"
       ]
    ],
    "schema":{
       "fields":[
          {
             "name":"MAX(c1)",
             "data_type":"Int64",
             "nullable":true,
             "dict_id":0,
             "dict_is_ordered":false
          },
          {
             "name":"MIN(c2)",
             "data_type":"Float64",
             "nullable":true,
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
    "input":{
       "execution_plan":"memory_exec",
       "schema":{
          "fields":[
             {
                "name":"MAX(c1)",
                "data_type":"Int64",
                "nullable":true,
                "dict_id":0,
                "dict_is_ordered":false
             },
             {
                "name":"MIN(c2)",
                "data_type":"Float64",
                "nullable":true,
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
       "projection":null
    }
}
"#;

static mut PLAN: Option<ProjectionExec> = None;
static INIT: Once = Once::new();

/// Performs an initialization routine once and only once.
macro_rules! init_plan {
    () => {{
        unsafe {
            INIT.call_once(|| {
                PLAN = Some(serde_json::from_str(&PLAN_JSON).unwrap());
            });

            match &PLAN {
                Some(plan) => plan.schema().clone(),
                None => panic!("Unexpected plan!"),
            }
        }
    }};
}

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let schema = init_plan!();
    let record_batch = DataFrame::to_batch(event);

    unsafe {
        match &mut PLAN {
            Some(plan) => {
                // Plan Execution
                plan.feed_batches(vec![vec![record_batch]]);
                let it = plan.execute(0).await?;
                let result = common::collect(it).await?;
                pretty::print_batches(&result)?;

                // RecordBatch to DataFrame
                let datafame = DataFrame::from(&result[0], schema);
                Ok(serde_json::to_value(&datafame)?)
            }
            None => panic!("Unexpected plan!"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn projection_test() {
        let data = r#"
            {"header":[16,0,0,0,12,0,26,0,24,0,23,0,4,0,8,0,12,0,0,0,32,0,0,0,80,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,10,0,24,0,12,0,8,0,4,0,10,0,0,0,76,0,0,0,16,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,16,0,0,0,0,0,0,0,24,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,32,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,40,0,0,0,0,0,0,0,16,0,0,0,0,0,0,0,56,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,64,0,0,0,0,0,0,0,16,0,0,0,0,0,0,0],"body":[255,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,2,0,0,0,0,0,0,0,98,97,0,0,0,0,0,0,255,0,0,0,0,0,0,0,101,0,0,0,0,0,0,0,100,0,0,0,0,0,0,0,255,0,0,0,0,0,0,0,154,153,153,153,153,25,88,64,102,102,102,102,102,6,87,64],"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}}
        "#;
        let event: Value = serde_json::from_str(data).unwrap();
        handler(event, Context::default()).await.ok().unwrap();
    }
}
