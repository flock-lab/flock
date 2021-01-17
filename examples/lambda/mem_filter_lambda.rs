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

use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{common, ExecutionPlan, LambdaExecPlan};

use arrow::json;
use arrow::json::reader::infer_json_schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;

use lambda::{handler_fn, Context};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::io::BufReader;
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

static mut PLAN: Option<FilterExec> = None;
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

/// Streaming data sources
#[derive(Debug, Deserialize, Serialize)]
pub struct DataSource {
    data: String,
}

impl DataSource {
    /// Convert streaming data source to record batch in Arrow.
    pub fn record_batch(event: Value) -> RecordBatch {
        let input: DataSource = serde_json::from_value(event).unwrap();
        let mut reader = BufReader::new(input.data.as_bytes());
        let inferred_schema = infer_json_schema(&mut reader, Some(1)).unwrap();
        let mut json = json::Reader::new(reader, inferred_schema, 1024, None);
        json.next().unwrap().unwrap()
    }
}

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let schema = init_plan!();
    let record_batch = DataSource::record_batch(event);

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
    async fn filter_test() {
        let data = r#"{"data": "{\"c1\": 90, \"c2\": 92.1, \"c3\": \"a\"}\n{\"c1\": 100, \"c2\": 93.2, \"c3\": \"a\"}\n{\"c1\": 91, \"c2\": 95.3, \"c3\": \"a\"}\n{\"c1\": 101, \"c2\": 96.4, \"c3\": \"b\"}\n{\"c1\": 92, \"c2\": 98.5, \"c3\": \"b\"}\n{\"c1\": 102, \"c2\": 99.6, \"c3\": \"b\"}\n{\"c1\": 93, \"c2\": 100.7, \"c3\": \"c\"}\n{\"c1\": 103, \"c2\": 101.8, \"c3\": \"c\"}"}"#;
        let event: Value = serde_json::from_str(data).unwrap();

        handler(event, Context::default()).await.ok().unwrap();
    }
}
