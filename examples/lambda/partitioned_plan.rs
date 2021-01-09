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

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::Schema;
    use arrow::json;
    use arrow::record_batch::RecordBatch;

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext;

    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::ExecutionPlan;

    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    use std::io::BufReader;

    #[tokio::test]
    async fn simple_avg() -> Result<(), Error> {
        #[derive(Debug, Deserialize, Serialize)]
        struct Data {
            data: String,
        }

        let input = r#"{"data": "{\"c1\": 90, \"c2\": 92.1, \"c3\": \"a\"}\n{\"c1\": 100, \"c2\": 93.2, \"c3\": \"a\"}\n{\"c1\": 91, \"c2\": 95.3, \"c3\": \"a\"}\n{\"c1\": 101, \"c2\": 96.4, \"c3\": \"b\"}\n{\"c1\": 92, \"c2\": 98.5, \"c3\": \"b\"}\n{\"c1\": 102, \"c2\": 99.6, \"c3\": \"b\"}\n{\"c1\": 93, \"c2\": 100.7, \"c3\": \"c\"}\n{\"c1\": 103, \"c2\": 101.8, \"c3\": \"c\"}"}"#;
        let input: Data = serde_json::from_str(input).unwrap();

        let schema = r#"{"fields":[
            {"name":"c1","data_type":"Int64","nullable":false,"dict_id":0,"dict_is_ordered":false},
            {"name":"c2","data_type":"Float64","nullable":false,"dict_id":0,"dict_is_ordered":false},
            {"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false}]
        }"#;
        let schema: Arc<Schema> = serde_json::from_str(schema).unwrap();

        let reader = BufReader::new(input.data.as_bytes());
        let mut json = json::Reader::new(reader, schema.clone(), 1024, None);
        let record_batch: RecordBatch = json.next().unwrap().unwrap();
        let partitions = vec![vec![record_batch]];

        let mut ctx = ExecutionContext::new();

        let provider = MemTable::try_new(schema, partitions)?;
        ctx.register_table("aggregate_test_100", Box::new(provider));

        let sql = "SELECT MAX(c1), MIN(c2), c3 FROM aggregate_test_100 WHERE c2 < 99 GROUP BY c3";
        let logical_plan = ctx.create_logical_plan(&sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;

        let serialized = serde_json::to_string(&physical_plan).unwrap();
        // println!("{}", serde_json::to_string_pretty(&physical_plan).unwrap());

        assert_eq!(
            r#"{"execution_plan":"projection_exec","expr":[[{"physical_expr":"column","name":"MAX(c1)"},"MAX(c1)"],[{"physical_expr":"column","name":"MIN(c2)"},"MIN(c2)"],[{"physical_expr":"column","name":"c3"},"c3"]],"schema":{"fields":[{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"input":{"execution_plan":"hash_aggregate_exec","mode":"Final","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"hash_aggregate_exec","mode":"Partial","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"coalesce_batches_exec","input":{"execution_plan":"filter_exec","predicate":{"physical_expr":"binary_expr","left":{"physical_expr":"column","name":"c2"},"op":"Lt","right":{"physical_expr":"cast_expr","expr":{"physical_expr":"literal","value":{"Int64":99}},"cast_type":"Float64"}},"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c1","data_type":"Int64","nullable":false,"dict_id":0,"dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":false,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":[0,1,2]}},"target_batch_size":16384},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)[max]","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)[min]","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}}}"#,
            serialized
        );

        let physical_plan: Arc<dyn ExecutionPlan> = serde_json::from_str(&serialized).unwrap();
        let projection_exec = match physical_plan.as_any().downcast_ref::<ProjectionExec>() {
            Some(projection_exec) => projection_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = projection_exec.children();
        let hash_agg_2_exec = match input[0].as_any().downcast_ref::<HashAggregateExec>() {
            Some(hash_agg_2_exec) => hash_agg_2_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = hash_agg_2_exec.children();
        let hash_agg_1_exec = match input[0].as_any().downcast_ref::<HashAggregateExec>() {
            Some(hash_agg_1_exec) => hash_agg_1_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = hash_agg_1_exec.children();
        let coalesce_exec = match input[0].as_any().downcast_ref::<CoalesceBatchesExec>() {
            Some(coalesce_exec) => coalesce_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = coalesce_exec.children();
        let filter_exec = match input[0].as_any().downcast_ref::<FilterExec>() {
            Some(filter_exec) => filter_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = filter_exec.children();
        let memory_exec = match input[0].as_any().downcast_ref::<MemoryExec>() {
            Some(memory_exec) => memory_exec,
            None => panic!("Plan mismatch Error"),
        };

        assert_eq!("MemoryExec { partitions: [], schema: Schema { fields: [Field { name: \"c1\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false }, Field { name: \"c2\", data_type: Float64, nullable: false, dict_id: 0, dict_is_ordered: false }, Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false }], metadata: {} }, projection: Some([0, 1, 2]) }", format!("{:?}", memory_exec));

        let filter = filter_exec.new_orphan();
        assert_eq!("FilterExec { predicate: BinaryExpr { left: Column { name: \"c2\" }, op: Lt, right: CastExpr { expr: Literal { value: Int64(99) }, cast_type: Float64 } }, input: DummyExec }", format!("{:?}", filter));

        let coalesce = coalesce_exec.new_orphan();
        assert_eq!(
            "CoalesceBatchesExec { input: DummyExec, target_batch_size: 16384 }",
            format!("{:?}", coalesce)
        );

        let hash_agg_1 = hash_agg_1_exec.new_orphan();
        assert_eq!("HashAggregateExec { mode: Partial, group_expr: [(Column { name: \"c3\" }, \"c3\")], aggr_expr: [Max { name: \"MAX(c1)\", data_type: Int64, nullable: true, expr: Column { name: \"c1\" } }, Min { name: \"MIN(c2)\", data_type: Float64, nullable: true, expr: Column { name: \"c2\" } }], input: DummyExec, schema: Schema { fields: [Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false }, Field { name: \"MAX(c1)[max]\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false }, Field { name: \"MIN(c2)[min]\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false }], metadata: {} } }", format!("{:?}", hash_agg_1));

        let hash_agg_2 = hash_agg_2_exec.new_orphan();
        assert_eq!("HashAggregateExec { mode: Final, group_expr: [(Column { name: \"c3\" }, \"c3\")], aggr_expr: [Max { name: \"MAX(c1)\", data_type: Int64, nullable: true, expr: Column { name: \"c1\" } }, Min { name: \"MIN(c2)\", data_type: Float64, nullable: true, expr: Column { name: \"c2\" } }], input: DummyExec, schema: Schema { fields: [Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false }, Field { name: \"MAX(c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false }, Field { name: \"MIN(c2)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false }], metadata: {} } }", format!("{:?}", hash_agg_2));

        let projection = projection_exec.new_orphan();
        assert_eq!("ProjectionExec { expr: [(Column { name: \"MAX(c1)\" }, \"MAX(c1)\"), (Column { name: \"MIN(c2)\" }, \"MIN(c2)\"), (Column { name: \"c3\" }, \"c3\")], schema: Schema { fields: [Field { name: \"MAX(c1)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false }, Field { name: \"MIN(c2)\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false }, Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false }], metadata: {} }, input: DummyExec }", format!("{:?}", projection));

        Ok(())
    }
}
