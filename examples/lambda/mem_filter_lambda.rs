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

use arrow::datatypes::Schema;
use arrow::json;
use arrow::record_batch::RecordBatch;

use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

use arrow::util::pretty;
use lambda::{handler_fn, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

use std::io::BufReader;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[derive(Debug, Deserialize, Serialize)]
struct Data {
    data: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FlightDataRef {
    #[serde(with = "serde_bytes")]
    pub data_header: std::vec::Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub data_body:   std::vec::Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    // Construct BatchRecord
    let input: Data = serde_json::from_value(event).unwrap();
    let schema_str = r#"{"fields":[{"name":"c1","data_type":"Int64","nullable":false,"dict_id":0,
    "dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":false,"dict_id":0,
    "dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,
    "dict_is_ordered":false}]}"#;
    let schema: Arc<Schema> = serde_json::from_str(schema_str).unwrap();

    let data_str = input.data;
    let reader = BufReader::new(data_str.as_bytes());
    let mut json = json::Reader::new(reader, schema.clone(), 1024, None);
    let record_batch: RecordBatch = json.next().unwrap().unwrap();
    // println!("batch:\n{:?}", record_batch);

    // Construct MemoryExec
    let partitions: Vec<Vec<RecordBatch>> = vec![vec![record_batch.clone()]];
    let mem_plan = MemoryExec::try_new(&partitions, schema.clone(), None)?;

    // Construct FilterExec
    let plan_json =  "{\"predicate\":{\"physical_expr\":\"binary_expr\",\"left\":{\"physical_expr\":\"column\",\"name\":\"c2\"},\"op\":\"Lt\",\"right\":{\"physical_expr\":\"cast_expr\",\"expr\":{\"physical_expr\":\"literal\",\"value\":{\"Int64\":99}},\"cast_type\":\"Float64\"}},\"input\":{\"execution_plan\":\"dummy_exec\"}}";
    let dummy_plan: FilterExec = serde_json::from_str(&plan_json).unwrap();
    // println!("dummy plan:\n{:?}", dummy_plan);
    
    let plan = dummy_plan.try_new_from_plan(Arc::new(mem_plan)).unwrap().execute(0).await?;

    let result = collect(plan).await?;
    pretty::print_batches(&result)?;

    // RecordBatch to FlightData
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let flight_data = &arrow_flight::utils::flight_data_from_arrow_batch(&result[0], &options)[0];

    let flight_data_ref = FlightDataRef {
        data_header: flight_data.data_header.clone(),
        data_body:   flight_data.data_body.clone(),
    };
    let data_str = serde_json::to_string(&flight_data_ref).unwrap();

    Ok(json!({ "data": data_str, "schema": schema_str.to_string()}))
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
