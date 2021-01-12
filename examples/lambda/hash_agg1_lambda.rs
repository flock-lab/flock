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

use arrow::datatypes::Schema;
use arrow_flight::FlightData;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::LambdaExecPlan;
use datafusion::physical_plan::{collect, ExecutionPlan};
use lambda::{handler_fn, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

use arrow::util::pretty;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[derive(Debug, Deserialize, Serialize)]
struct Data {
    data:   String,
    schema: String,
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
    // Convert FlightData to BatchRecord
    let input: Data = serde_json::from_value(event).unwrap();

    let fake_flight_data: FlightDataRef = serde_json::from_str(&input.data).unwrap();
    let prev_schema: Schema = serde_json::from_str(&input.schema).unwrap();

    let flight_data_de = FlightData {
        flight_descriptor: None,
        app_metadata:      vec![],
        data_header:       fake_flight_data.data_header,
        data_body:         fake_flight_data.data_body,
    };
    let arrow_batch = arrow_flight::utils::flight_data_to_arrow_batch(
        &flight_data_de,
        Arc::new(prev_schema.clone()),
        &[],
    )
    .unwrap();

    // Construct HashAggregateExec with MemoryExec
    let plan_json = r#"{"mode":"Partial","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)[max]","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)[min]","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":null},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)[max]","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)[min]","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}}"#;
    let mut hash_agg_exec: HashAggregateExec = serde_json::from_str(&plan_json).unwrap();

    hash_agg_exec.feed_batches(vec![vec![arrow_batch]]);
    let output_schema = hash_agg_exec.schema();
    let result = collect(Arc::new(hash_agg_exec)).await?;
    pretty::print_batches(&result)?;

    // RecordBatch to FlightData
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let (_, flight_data) = arrow_flight::utils::flight_data_from_arrow_batch(&result[0], &options);

    let flight_data_ref = FlightDataRef {
        data_header: flight_data.data_header,
        data_body:   flight_data.data_body,
    };
    let data_str = serde_json::to_string(&flight_data_ref).unwrap();

    Ok(json!({ "data": data_str, "schema": serde_json::to_string(&output_schema).unwrap()}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn agg1_test() {
        let data = r#"{
            "data": "{\"data_header\":[16,0,0,0,12,0,26,0,24,0,23,0,4,0,8,0,12,0,0,0,32,0,0,0,136,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,10,0,24,0,12,0,8,0,4,0,10,0,0,0,76,0,0,0,16,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,40,0,0,0,0,0,0,0,48,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,56,0,0,0,0,0,0,0,40,0,0,0,0,0,0,0,96,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,104,0,0,0,0,0,0,0,24,0,0,0,0,0,0,0,128,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0],\"data_body\":[255,0,0,0,0,0,0,0,90,0,0,0,0,0,0,0,100,0,0,0,0,0,0,0,91,0,0,0,0,0,0,0,101,0,0,0,0,0,0,0,92,0,0,0,0,0,0,0,255,0,0,0,0,0,0,0,102,102,102,102,102,6,87,64,205,204,204,204,204,76,87,64,51,51,51,51,51,211,87,64,154,153,153,153,153,25,88,64,0,0,0,0,0,160,88,64,255,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,2,0,0,0,3,0,0,0,4,0,0,0,5,0,0,0,97,97,97,98,98,0,0,0]}",
            "schema": "{\"fields\":[{\"name\":\"c1\",\"data_type\":\"Int64\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false},{\"name\":\"c2\",\"data_type\":\"Float64\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false},{\"name\":\"c3\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false}]}"
        }"#;
        let event: Value = serde_json::from_str(data).unwrap();
        handler(event, Context::default()).await.ok().unwrap();
    }
}
