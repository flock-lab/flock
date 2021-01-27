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

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate daggy;
    use daggy::NodeIndex;

    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use aws_lambda_events::event::kinesis::KinesisEvent;

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext;

    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::ExecutionPlan;

    use driver::funcgen::dag::{DagNode, LambdaDag};
    use runtime::datasource::kinesis;
    use std::sync::Arc;

    #[tokio::test]
    async fn simple_query() -> Result<(), Error> {
        let input = include_str!("example-kinesis-event.json");
        let input: KinesisEvent = serde_json::from_str(input).unwrap();

        let (record_batch, schema) = kinesis::to_batch(input);

        let partitions = vec![vec![record_batch]];

        let mut ctx = ExecutionContext::new();

        let provider = MemTable::try_new(schema, partitions)?;
        ctx.register_table("aggregate_test_100", Box::new(provider));

        let sql = "SELECT MAX(c1), MIN(c2), c3 FROM aggregate_test_100 WHERE c2 < 99 GROUP BY c3";
        let logical_plan = ctx.create_logical_plan(&sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;

        let serialized = serde_json::to_string(&physical_plan).unwrap();

        assert_eq!(
            r#"{"execution_plan":"projection_exec","expr":[[{"physical_expr":"column","name":"MAX(c1)"},"MAX(c1)"],[{"physical_expr":"column","name":"MIN(c2)"},"MIN(c2)"],[{"physical_expr":"column","name":"c3"},"c3"]],"schema":{"fields":[{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"input":{"execution_plan":"hash_aggregate_exec","mode":"Final","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"hash_aggregate_exec","mode":"Partial","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"coalesce_batches_exec","input":{"execution_plan":"filter_exec","predicate":{"physical_expr":"binary_expr","left":{"physical_expr":"column","name":"c2"},"op":"Lt","right":{"physical_expr":"cast_expr","expr":{"physical_expr":"literal","value":{"Int64":99}},"cast_type":"Float64"}},"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c1","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":[0,1,2]}},"target_batch_size":16384},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)[max]","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)[min]","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}}}"#,
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

        let memory = Arc::new(memory_exec.clone()) as Arc<dyn ExecutionPlan>;
        assert_eq!(
            r#"{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c1","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":[0,1,2]}"#,
            serde_json::to_string(&memory).unwrap()
        );

        let filter = filter_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert_eq!(
            r#"{"execution_plan":"filter_exec","predicate":{"physical_expr":"binary_expr","left":{"physical_expr":"column","name":"c2"},"op":"Lt","right":{"physical_expr":"cast_expr","expr":{"physical_expr":"literal","value":{"Int64":99}},"cast_type":"Float64"}},"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c1","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":[0,1,2]}}"#,
            serde_json::to_string(&filter).unwrap()
        );

        let coalesce = coalesce_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert_eq!(
            r#"{"execution_plan":"coalesce_batches_exec","input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c1","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":null},"target_batch_size":16384}"#,
            serde_json::to_string(&coalesce).unwrap()
        );

        let hash_agg_1 = hash_agg_1_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert_eq!(
            r#"{"execution_plan":"hash_aggregate_exec","mode":"Partial","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)[max]","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)[min]","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":null},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)[max]","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)[min]","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}}"#,
            serde_json::to_string(&hash_agg_1).unwrap()
        );

        let hash_agg_2 = hash_agg_2_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert_eq!(
            r#"{"execution_plan":"hash_aggregate_exec","mode":"Final","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":null},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}}"#,
            serde_json::to_string(&hash_agg_2).unwrap()
        );

        let projection = projection_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert_eq!(
            r#"{"execution_plan":"projection_exec","expr":[[{"physical_expr":"column","name":"MAX(c1)"},"MAX(c1)"],[{"physical_expr":"column","name":"MIN(c2)"},"MIN(c2)"],[{"physical_expr":"column","name":"c3"},"c3"]],"schema":{"fields":[{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":null}}"#,
            serde_json::to_string(&projection).unwrap()
        );

        // Construct a DAG for the physical plan partition
        let mut lambda_dag = LambdaDag::new();

        let node = DagNode::from(format!("{}", serde_json::to_value(&hash_agg_2).unwrap()));
        let p = lambda_dag.add_node(node);

        let node = DagNode::from(format!("{}", serde_json::to_value(&hash_agg_1).unwrap()));
        let h = lambda_dag.add_child(p, node);

        let node = DagNode::from(format!("{}", serde_json::to_value(&coalesce).unwrap()));
        let c = lambda_dag.add_child(h, node);

        let node = DagNode::from(format!("{}", serde_json::to_value(&filter).unwrap()));
        let f = lambda_dag.add_child(c, node);

        let node = DagNode::from(format!("{}", serde_json::to_value(&memory).unwrap()));
        let m = lambda_dag.add_child(f, node);

        assert!(lambda_dag
            .get_node(p)
            .unwrap()
            .contains("hash_aggregate_exec"));

        // Forward traversal from root
        let plans = lambda_dag.get_sub_plans(p);
        let mut iter = plans.iter();

        assert!(iter.next().unwrap().0.contains("hash_aggregate_exec"));
        assert!(iter.next().unwrap().0.contains("coalesce_batches_exec"));
        assert!(iter.next().unwrap().0.contains("filter_exec"));
        assert!(iter.next().unwrap().0.contains("memory_exec"));

        // Backward traversal from leaf
        let plans = lambda_dag.get_depended_plans(m);
        let mut iter = plans.iter();

        assert!(iter.next().unwrap().0.contains("filter_exec"));
        assert!(iter.next().unwrap().0.contains("coalesce_batches_exec"));
        assert!(iter.next().unwrap().0.contains("hash_aggregate_exec"));
        assert!(iter.next().unwrap().0.contains("hash_aggregate_exec"));

        Ok(())
    }

    #[tokio::test]
    async fn partition_json() {
        let plan = r#"{"execution_plan":"projection_exec","expr":[[{"physical_expr":"column","name":"MAX(c1)"},"MAX(c1)"],[{"physical_expr":"column","name":"MIN(c2)"},"MIN(c2)"],[{"physical_expr":"column","name":"c3"},"c3"]],"schema":{"fields":[{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"input":{"execution_plan":"hash_aggregate_exec","mode":"Final","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"hash_aggregate_exec","mode":"Partial","group_expr":[[{"physical_expr":"column","name":"c3"},"c3"]],"aggr_expr":[{"aggregate_expr":"max","name":"MAX(c1)","data_type":"Int64","nullable":true,"expr":{"physical_expr":"column","name":"c1"}},{"aggregate_expr":"min","name":"MIN(c2)","data_type":"Float64","nullable":true,"expr":{"physical_expr":"column","name":"c2"}}],"input":{"execution_plan":"coalesce_batches_exec","input":{"execution_plan":"filter_exec","predicate":{"physical_expr":"binary_expr","left":{"physical_expr":"column","name":"c2"},"op":"Lt","right":{"physical_expr":"cast_expr","expr":{"physical_expr":"literal","value":{"Int64":99}},"cast_type":"Float64"}},"input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c1","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":[0,1,2]}},"target_batch_size":16384},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)[max]","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)[min]","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}},"schema":{"fields":[{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MAX(c1)","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"MIN(c2)","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}}}}"#;

        let plan: Arc<dyn ExecutionPlan> = serde_json::from_str(&plan).unwrap();

        let dag = &mut LambdaDag::from(&plan);
        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan.contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));
    }

    // Mem -> Proj
    #[tokio::test]
    async fn simple_select() {
        let sql = concat!("SELECT c1 FROM test_table");
        quick_init(&sql);
    }

    #[tokio::test]
    async fn select_alias() {
        let sql = concat!("SELECT c1 as col_1 FROM test_table");
        quick_init(&sql);
    }

    #[tokio::test]
    async fn cast() {
        let sql = concat!("SELECT CAST(c2 AS int) FROM test_table");
        quick_init(&sql);
    }
    #[tokio::test]
    async fn math() {
        let sql = concat!("SELECT c1+c2 FROM test_table");
        quick_init(&sql);
    }

    #[tokio::test]
    async fn math_sqrt() {
        let sql = concat!("SELECT c1>=c2 FROM test_table");
        quick_init(&sql);
    }

    // Filter
    // Memory -> Filter -> CoalesceBatches -> Projection
    #[tokio::test]
    async fn filter_query() {
        let sql = concat!("SELECT c1, c2 FROM test_table WHERE c2 < 99");
        quick_init(&sql);
    }

    // Mem -> Filter -> Coalesce
    #[tokio::test]
    async fn filter_select_all() {
        let sql = concat!("SELECT * FROM test_table WHERE c2 < 99");
        quick_init(&sql);
    }

    // Aggregate
    // Mem -> HashAgg -> HashAgg
    #[tokio::test]
    async fn aggregate_query_no_group_by_count_distinct_wide() {
        let sql = concat!("SELECT COUNT(DISTINCT c1) FROM test_table");
        let dag = &mut quick_init(&sql);

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));
    }

    #[tokio::test]
    async fn aggregate_query_no_group_by() {
        let sql = concat!("SELECT MIN(c1), AVG(c4), COUNT(c3) FROM test_table");
        quick_init(&sql);
    }

    // Aggregate + Group By
    // Mem -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn aggregate_query_group_by() {
        let sql = concat!(
            "SELECT MIN(c1), AVG(c4), COUNT(c3) as c3_count ",
            "FROM test_table ",
            "GROUP BY c3"
        );
        let dag = &mut quick_init(&sql);

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));
    }

    // Sort
    // Mem -> Project -> Sort
    #[tokio::test]
    async fn sort() {
        let sql = concat!("SELECT c1, c2, c3 ", "FROM test_table ", "ORDER BY c1 ");
        quick_init(&sql);
    }

    // Sort limit
    // Mem -> Project -> Sort -> GlobalLimit
    #[tokio::test]
    async fn sort_and_limit_by_int() {
        let sql = concat!(
            "SELECT c1, c2, c3 ",
            "FROM test_table ",
            "ORDER BY c1 ",
            "LIMIT 4"
        );
        quick_init(&sql);
    }

    // Agg + Filter
    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn aggregate_query_filter() {
        let sql = concat!(
            "SELECT MIN(c1), AVG(c4), COUNT(c3) as c3_count ",
            "FROM test_table ",
            "WHERE c2 < 99"
        );
        let dag = &mut quick_init(&sql);

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan.contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));
    }

    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn agg_query2() {
        let sql = concat!(
            "SELECT MAX(c1), MIN(c2), c3 ",
            "FROM test_table ",
            "WHERE c2 < 101 AND c1 > 91 ",
            "GROUP BY c3"
        );
        let dag = &mut quick_init(&sql);

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan.contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));
    }

    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj -> Sort ->
    // GlobalLimit
    #[tokio::test]
    async fn filter_agg_sort() {
        let sql = concat!(
            "SELECT MAX(c1), MIN(c2), c3 ",
            "FROM test_table ",
            "WHERE c2 < 101 ",
            "GROUP BY c3 ",
            "ORDER BY c3 ",
            "LIMIT 3"
        );
        let dag = &mut quick_init(&sql);

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"global_limit_exec"#));
        assert!(subplan.contains(r#"execution_plan":"sort_exec"#));
        assert!(subplan.contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan.contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));
    }

    fn quick_init(sql: &str) -> LambdaDag {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Float64, false),
            Field::new("c3", DataType::Utf8, false),
            Field::new("c4", DataType::UInt64, false),
            Field::new("c5", DataType::Utf8, false),
            Field::new("neg", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![90, 90, 91, 101, 92, 102, 93, 103])),
                Arc::new(Float64Array::from(vec![
                    92.1, 93.2, 95.3, 96.4, 98.5, 99.6, 100.7, 101.8,
                ])),
                Arc::new(StringArray::from(vec![
                    "a", "a", "d", "b", "b", "d", "c", "c",
                ])),
                Arc::new(UInt64Array::from(vec![33, 1, 54, 33, 12, 75, 2, 87])),
                Arc::new(StringArray::from(vec![
                    "rapport",
                    "pedantic",
                    "mimesis",
                    "haptic",
                    "baksheesh",
                    "amok",
                    "devious",
                    "c",
                ])),
                Arc::new(Int64Array::from(vec![
                    -90, -90, -91, -101, -92, -102, -93, -103,
                ])),
            ],
        )
        .unwrap();

        let mut ctx = ExecutionContext::new();
        // batch? Only support 1 RecordBatch now.
        let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("test_table", Box::new(provider));

        let logical_plan = ctx.create_logical_plan(sql).unwrap();
        let optimized_plan = ctx.optimize(&logical_plan).unwrap();
        let physical_plan = ctx.create_physical_plan(&optimized_plan).unwrap();
        LambdaDag::from(&physical_plan)
    }

    #[tokio::test]
    async fn simple_join() -> Result<(), Error> {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("d", DataType::Int32, false),
        ]));

        // define data.
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;
        // define data.
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;

        let mut ctx = ExecutionContext::new();

        let table1 = MemTable::try_new(schema1, vec![vec![batch1]])?;
        let table2 = MemTable::try_new(schema2, vec![vec![batch2]])?;

        ctx.register_table("t1", Box::new(table1));
        ctx.register_table("t2", Box::new(table2));

        let sql = concat!(
            "SELECT a, b, d ",
            "FROM t1 JOIN t2 ON a = c ",
            "ORDER BY b ASC ",
            "LIMIT 3"
        );

        let plan = ctx.create_logical_plan(&sql)?;
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;

        //       +-----------------+
        //       |global_limit_exec|
        //       +--------+--------+
        //                |
        //           +----v----+
        //           |sort_exec|
        //           +----+----+
        //                |
        //        +-------v-------+
        //        |projection_exec|
        //        +-------+-------+
        //                |
        //     +----------v----------+
        //     |coalesce_batches_exec|
        //     +----------+----------+
        //                |
        //         +------v-------+
        //         |hash_join_exec|
        //         +------+-------+
        //                |
        //       +--------v---------+
        //       |                  |
        // +-----v-----+      +-----v-----+
        // |memory_exec|      |memory_exec|
        // +-----------+      +-----------+

        let json = serde_json::to_string(&plan).unwrap();
        assert!(json.contains(r#"execution_plan":"global_limit_exec"#));
        assert!(json.contains(r#"execution_plan":"sort_exec"#));
        assert!(json.contains(r#"execution_plan":"projection_exec"#));
        assert!(json.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(json.contains(r#"execution_plan":"hash_join_exec"#));
        assert!(json.contains(r#"left":{"execution_plan":"memory_exec"#));
        assert!(json.contains(r#"right":{"execution_plan":"memory_exec"#));

        let dag = &mut LambdaDag::from(&plan);

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let subplan = dag.node_weight(NodeIndex::new(0)).unwrap();
        assert!(subplan.contains(r#"execution_plan":"global_limit_exec"#));
        assert!(subplan.contains(r#"execution_plan":"sort_exec"#));
        assert!(subplan.contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        let subplan = dag.node_weight(NodeIndex::new(1)).unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_join_exec"#));
        assert!(subplan.contains(r#"right":{"execution_plan":"memory_exec"#));
        assert!(subplan.contains(r#"left":{"execution_plan":"memory_exec"#));

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"global_limit_exec"#));
        assert!(subplan.contains(r#"execution_plan":"sort_exec"#));
        assert!(subplan.contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan.contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.contains(r#"execution_plan":"hash_join_exec"#));
        assert!(subplan.contains(r#"right":{"execution_plan":"memory_exec"#));
        assert!(subplan.contains(r#"left":{"execution_plan":"memory_exec"#));

        let batches = collect(plan).await?;
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        let actual_lines: Vec<&str> = formatted.trim().lines().collect();

        let expected = vec![
            "+---+----+----+",
            "| a | b  | d  |",
            "+---+----+----+",
            "| a | 1  | 1  |",
            "| b | 10 | 10 |",
            "| c | 10 | 10 |",
            "+---+----+----+",
        ];

        assert_eq!(expected, actual_lines);

        Ok(())
    }
}
