// Copyright (c) 2020 UMD Database Group. All Rights Reserved.
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
// Only bring in dependencies for the repl when the cli feature is enabled.

//! A directed acyclic graph (DAG) data structure to hold all sub-plans of the
//! query statement.

extern crate daggy;
use daggy::{Dag, NodeIndex, Walker};

use arrow::datatypes::Schema;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

use serde_json::Value;

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Concurrency is the number of requests that your function is serving at any
/// given time. When your function is invoked, cloud function services allocates
/// an instance of it to process the event. When the function code finishes
/// running, it can handle another request. If the function is invoked again
/// while a request is still being processed, another instance is allocated,
/// which increases the function's concurrency.
/// cloud function with concurrency = 1
pub const CONCURRENCY_1: u8 = 1;
/// cloud function with concurrency = 8
pub const CONCURRENCY_8: u8 = 8;

type DagEdge = ();
type DagPlan = Dag<DagNode, DagEdge>;

/// A node representation of the subplan of a query statement.
#[derive(Debug, Clone)]
pub struct DagNode {
    /// Subplan string.
    pub plan:        Arc<dyn ExecutionPlan>,
    /// Function concurrency in cloud environment.
    pub concurrency: u8,
}

impl DagNode {
    /// Return a Json string representation of a sub-plan.
    pub fn get_plan_str(&self) -> String {
        serde_json::to_string(&self.plan).unwrap()
    }
}

impl From<Arc<dyn ExecutionPlan>> for DagNode {
    fn from(p: Arc<dyn ExecutionPlan>) -> DagNode {
        DagNode {
            plan:        p.clone(),
            concurrency: CONCURRENCY_8,
        }
    }
}

impl Deref for DagNode {
    type Target = Arc<dyn ExecutionPlan>;

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

/// A simple directed acyclic graph representation of the physical plan of a
/// query statement. This graph allows the traversal of subplans in a
/// topological order.  It is also possible to query subplan or dependencies
/// for a given subplan.
#[derive(Debug)]
pub struct QueryDag {
    dag: DagPlan,
}

impl Deref for QueryDag {
    type Target = DagPlan;

    fn deref(&self) -> &Self::Target {
        &self.dag
    }
}

impl DerefMut for QueryDag {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dag
    }
}

impl QueryDag {
    /// Create a new `QueryDag`.
    pub fn new() -> Self {
        QueryDag {
            dag: DagPlan::new(),
        }
    }

    /// The total number of nodes in the Dag.
    pub fn node_count(&self) -> usize {
        self.dag.node_count()
    }

    /// Build a Dag representation of a given plan.
    pub fn from(plan: &Arc<dyn ExecutionPlan>) -> Self {
        Self::build_dag(plan)
    }

    /// Return the depth for the given node in the dag.
    pub fn depth(&self, node: NodeIndex) -> usize {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .count()
    }

    /// Add a new node to the `QueryDag`.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Returns the index of the new node.
    pub fn add_node(&mut self, node: DagNode) -> NodeIndex {
        self.dag.add_node(node)
    }

    /// Add a root node to the `QueryDag`.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Returns the index of the new root.
    pub fn add_parent(&mut self, child: NodeIndex, node: DagNode) -> NodeIndex {
        let (_, n) = self.dag.add_parent(child, (), node);
        n
    }

    /// Return a node for a given id.
    pub fn get_node(&self, id: NodeIndex) -> Option<&DagNode> {
        self.dag.node_weight(id)
    }

    /// Return a Json string representation of a sub-plan.
    pub fn get_plan_str(&self, id: NodeIndex) -> String {
        serde_json::to_string(&self.dag.node_weight(id).unwrap().plan).unwrap()
    }

    /// Add a new child node to the node at the given `NodeIndex`.
    /// Return the node's `NodeIndex`.
    ///
    /// child -> edge -> node
    ///
    /// Compute in **O(1)** time.
    pub fn add_child(&mut self, parent: NodeIndex, node: DagNode) -> NodeIndex {
        let (_, n) = self.dag.add_child(parent, (), node);
        n
    }

    /// Return a **Vec** with all depended subplans for the given node.
    pub fn get_depended_plans(&self, node: NodeIndex) -> Vec<(DagNode, NodeIndex)> {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .map(|(_, n)| (self.dag.node_weight(n).unwrap().clone(), n))
            .collect()
    }

    /// Return a **Vec** with all children for the given node.
    pub fn get_sub_plans(&self, node: NodeIndex) -> Vec<(DagNode, NodeIndex)> {
        let mut children = Vec::new();
        for (_, n) in self.dag.children(node).iter(&self.dag) {
            children.push((self.dag.node_weight(n).unwrap().clone(), n));
            children.append(&mut self.get_sub_plans(n));
        }
        children
    }

    /// Return the internal daggy.
    pub fn context(&mut self) -> &mut DagPlan {
        &mut self.dag
    }

    /// Build a new daggy from a physical plan.
    fn build_dag(plan: &Arc<dyn ExecutionPlan>) -> Self {
        let mut dag = QueryDag::new();
        Self::fission(&mut dag, &plan);
        assert!(dag.node_count() >= 1);
        dag
    }

    /// Add a new node to the `QueryDag`.
    fn insert_dag(dag: &mut QueryDag, leaf: NodeIndex, node: Value, concurrency: u8) -> NodeIndex {
        if leaf == NodeIndex::end() {
            dag.add_node(DagNode {
                plan: serde_json::from_value(node).unwrap(),
                concurrency,
            })
        } else {
            dag.add_child(
                leaf,
                DagNode {
                    plan: serde_json::from_value(node).unwrap(),
                    concurrency,
                },
            )
        }
    }

    /// Transform a physical plan for cloud environment execution.
    fn fission(dag: &mut QueryDag, plan: &Arc<dyn ExecutionPlan>) {
        let mut root = serde_json::to_value(&plan).unwrap();
        let mut json = &mut root;
        let mut leaf = NodeIndex::end();
        let mut curr = plan.clone();
        loop {
            match json["execution_plan"].as_str() {
                Some("hash_aggregate_exec") => match json["mode"].as_str() {
                    Some("FinalPartitioned") | Some("Final") => {
                        // Split the plan into two subplans
                        let object = (*json["input"].take().as_object().unwrap()).clone();
                        // Add a input for the new subplan
                        let input: Arc<dyn ExecutionPlan> = Arc::new(
                            MemoryExec::try_new(&[], curr.children()[0].schema().clone(), None)
                                .unwrap(),
                        );
                        json["input"] = serde_json::to_value(input).unwrap();
                        // Add the new subplan to DAG
                        leaf = Self::insert_dag(dag, leaf, root, CONCURRENCY_1);
                        // Point to the next subplan
                        root = Value::Object(object);
                        json = &mut root;
                    }
                    _ => json = &mut json["input"],
                },
                Some("hash_join_exec") => {
                    let object = (*json.take().as_object().unwrap()).clone();
                    let input: Arc<dyn ExecutionPlan> = Arc::new(
                        MemoryExec::try_new(&[], Arc::new(Schema::empty()), None).unwrap(),
                    );
                    *json = serde_json::to_value(input).unwrap();
                    leaf = Self::insert_dag(dag, leaf, root, CONCURRENCY_1);
                    root = Value::Object(object);
                    break;
                }
                _ => json = &mut json["input"],
            }
            if !json.is_object() {
                break;
            }
            curr = curr.children()[0].clone();
        }
        Self::insert_dag(dag, leaf, root, CONCURRENCY_8);
    }
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
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::ExecutionPlan;

    use runtime::prelude::*;
    use std::sync::Arc;

    #[tokio::test]
    #[ignore]
    async fn simple_query() -> Result<()> {
        let input = include_str!("../../../test/data/example-kinesis-event-1.json");
        let input: KinesisEvent = serde_json::from_str(input).unwrap();

        let partitions = vec![kinesis::to_batch(input)];

        let mut ctx = ExecutionContext::new();

        let provider = MemTable::try_new(partitions[0][0].schema(), partitions)?;
        ctx.register_table("aggregate_test_100", Arc::new(provider))?;

        let sql = "SELECT MAX(c1), MIN(c2), c3 FROM aggregate_test_100 WHERE c2 < 99 GROUP BY c3";
        let logical_plan = ctx.create_logical_plan(&sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;

        let serialized = serde_json::to_string(&physical_plan).unwrap();

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
        let coalesce_2_exec = match input[0].as_any().downcast_ref::<CoalesceBatchesExec>() {
            Some(coalesce_2_exec) => coalesce_2_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = coalesce_2_exec.children();
        let repartition_2_exec = match input[0].as_any().downcast_ref::<RepartitionExec>() {
            Some(repartition_2_exec) => repartition_2_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = repartition_2_exec.children();
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
        let repartition_1_exec = match input[0].as_any().downcast_ref::<RepartitionExec>() {
            Some(repartition_1_exec) => repartition_1_exec,
            None => panic!("Plan mismatch Error"),
        };

        let input = repartition_1_exec.children();
        let memory_exec = match input[0].as_any().downcast_ref::<MemoryExec>() {
            Some(memory_exec) => memory_exec,
            None => panic!("Plan mismatch Error"),
        };

        let memory = Arc::new(memory_exec.clone()) as Arc<dyn ExecutionPlan>;
        assert!(serde_json::to_string(&memory)
            .unwrap()
            .contains(r#"execution_plan":"memory_exec"#));

        let filter = filter_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert!(serde_json::to_string(&filter)
            .unwrap()
            .contains(r#"execution_plan":"filter_exec"#));

        let coalesce = coalesce_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert!(serde_json::to_string(&coalesce)
            .unwrap()
            .contains(r#"execution_plan":"coalesce_batches_exec"#));

        let hash_agg_1 = hash_agg_1_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert!(serde_json::to_string(&hash_agg_1)
            .unwrap()
            .contains(r#"execution_plan":"hash_aggregate_exec"#));

        let hash_agg_2 = hash_agg_2_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert!(serde_json::to_string(&hash_agg_2)
            .unwrap()
            .contains(r#"execution_plan":"hash_aggregate_exec"#));

        let projection = projection_exec.new_orphan() as Arc<dyn ExecutionPlan>;
        assert!(serde_json::to_string(&projection)
            .unwrap()
            .contains(r#"execution_plan":"projection_exec"#));

        // Construct a DAG for the physical plan partition
        let mut lambda_dag = QueryDag::new();

        let node = DagNode::from(hash_agg_2);
        let h2 = lambda_dag.add_node(node);

        let node = DagNode::from(hash_agg_1);
        let h1 = lambda_dag.add_child(h2, node);

        let node = DagNode::from(coalesce);
        let co = lambda_dag.add_child(h1, node);

        let node = DagNode::from(filter);
        let fi = lambda_dag.add_child(co, node);

        let node = DagNode::from(memory);
        let me = lambda_dag.add_child(fi, node);

        assert!(lambda_dag.get_plan_str(h2).contains("hash_aggregate_exec"));

        // Forward traversal from root
        let plans = lambda_dag.get_sub_plans(h2);
        let mut iter = plans.iter();

        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("hash_aggregate_exec"));
        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("coalesce_batches_exec"));
        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("filter_exec"));
        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("memory_exec"));

        // Backward traversal from leaf
        let plans = lambda_dag.get_depended_plans(me);
        let mut iter = plans.iter();

        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("filter_exec"));
        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("coalesce_batches_exec"));
        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("hash_aggregate_exec"));
        assert!(iter
            .next()
            .unwrap()
            .0
            .get_plan_str()
            .contains("hash_aggregate_exec"));

        Ok(())
    }

    #[tokio::test]
    async fn partition_json() {
        let json = include_str!("../../../test/data/plan/aggregate.json");
        let plan: Arc<dyn ExecutionPlan> = serde_json::from_str(&json).unwrap();

        let dag = &mut QueryDag::from(&plan);
        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"FinalPartitioned"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));
    }

    // Mem -> Proj
    #[tokio::test]
    async fn simple_select() -> Result<()> {
        let sql = concat!("SELECT c1 FROM test_table");
        quick_init(&sql)?;
        Ok(())
    }

    #[tokio::test]
    async fn select_alias() -> Result<()> {
        let sql = concat!("SELECT c1 as col_1 FROM test_table");
        quick_init(&sql)?;
        Ok(())
    }

    #[tokio::test]
    async fn cast() -> Result<()> {
        let sql = concat!("SELECT CAST(c2 AS int) FROM test_table");
        quick_init(&sql)?;
        Ok(())
    }

    #[tokio::test]
    async fn math() -> Result<()> {
        let sql = concat!("SELECT c1+c2 FROM test_table");
        quick_init(&sql)?;
        Ok(())
    }

    #[tokio::test]
    async fn math_sqrt() -> Result<()> {
        let sql = concat!("SELECT c1>=c2 FROM test_table");
        quick_init(&sql)?;
        Ok(())
    }

    // Filter
    // Memory -> Filter -> CoalesceBatches -> Projection
    #[tokio::test]
    async fn filter_query() -> Result<()> {
        let sql = concat!("SELECT c1, c2 FROM test_table WHERE c2 < 99");
        quick_init(&sql)?;
        Ok(())
    }

    // Mem -> Filter -> Coalesce
    #[tokio::test]
    async fn filter_select_all() -> Result<()> {
        let sql = concat!("SELECT * FROM test_table WHERE c2 < 99");
        quick_init(&sql)?;
        Ok(())
    }

    // Aggregate
    // Mem -> HashAgg -> HashAgg
    #[tokio::test]
    async fn aggregate_query_no_group_by_count_distinct_wide() -> Result<()> {
        let sql = concat!("SELECT COUNT(DISTINCT c1) FROM test_table");
        let dag = &mut quick_init(&sql)?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_query_no_group_by() -> Result<()> {
        let sql = concat!("SELECT MIN(c1), AVG(c4), COUNT(c3) FROM test_table");
        quick_init(&sql)?;
        Ok(())
    }

    // Aggregate + Group By
    // Mem -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn aggregate_query_group_by() -> Result<()> {
        let sql = concat!(
            "SELECT MIN(c1), AVG(c4), COUNT(c3) as c3_count ",
            "FROM test_table ",
            "GROUP BY c3"
        );
        let dag = &mut quick_init(&sql)?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        Ok(())
    }

    // Sort
    // Mem -> Project -> Sort
    #[tokio::test]
    async fn sort() -> Result<()> {
        let sql = concat!("SELECT c1, c2, c3 ", "FROM test_table ", "ORDER BY c1 ");
        quick_init(&sql)?;
        Ok(())
    }

    // Sort limit
    // Mem -> Project -> Sort -> GlobalLimit
    #[tokio::test]
    async fn sort_and_limit_by_int() -> Result<()> {
        let sql = concat!(
            "SELECT c1, c2, c3 ",
            "FROM test_table ",
            "ORDER BY c1 ",
            "LIMIT 4"
        );
        quick_init(&sql)?;
        Ok(())
    }

    // Agg + Filter
    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn aggregate_query_filter() -> Result<()> {
        let sql = concat!(
            "SELECT MIN(c1), AVG(c4), COUNT(c3) as c3_count ",
            "FROM test_table ",
            "WHERE c2 < 99"
        );
        let dag = &mut quick_init(&sql)?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        Ok(())
    }

    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn agg_query2() -> Result<()> {
        let sql = concat!(
            "SELECT MAX(c1), MIN(c2), c3 ",
            "FROM test_table ",
            "WHERE c2 < 101 AND c1 > 91 ",
            "GROUP BY c3"
        );
        let dag = &mut quick_init(&sql)?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        Ok(())
    }

    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj -> Sort ->
    // GlobalLimit
    #[tokio::test]
    async fn filter_agg_sort() -> Result<()> {
        let sql = concat!(
            "SELECT MAX(c1), MIN(c2), c3 ",
            "FROM test_table ",
            "WHERE c2 < 101 ",
            "GROUP BY c3 ",
            "ORDER BY c3 ",
            "LIMIT 3"
        );
        let dag = &mut quick_init(&sql)?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"global_limit_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"sort_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Final"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_aggregate_exec","mode":"Partial"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"filter_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        Ok(())
    }

    fn quick_init(sql: &str) -> Result<QueryDag> {
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
        )?;

        let mut ctx = ExecutionContext::new();
        // batch? Only support 1 RecordBatch now.
        let provider = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("test_table", Arc::new(provider))?;

        let logical_plan = ctx.create_logical_plan(sql)?;
        let optimized_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&optimized_plan)?;
        println!("{}", serde_json::to_string(&physical_plan)?);
        Ok(QueryDag::from(&physical_plan))
    }

    #[tokio::test]
    async fn simple_join() -> Result<()> {
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

        ctx.register_table("t1", Arc::new(table1))?;
        ctx.register_table("t2", Arc::new(table2))?;

        let sql = concat!(
            "SELECT a, b, d ",
            "FROM t1 JOIN t2 ON a = c ",
            "ORDER BY a ASC ",
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
        println!("{}", json);
        assert!(json.contains(r#"execution_plan":"global_limit_exec"#));
        assert!(json.contains(r#"execution_plan":"sort_exec"#));
        assert!(json.contains(r#"execution_plan":"projection_exec"#));
        assert!(json.contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(json.contains(r#"execution_plan":"hash_join_exec"#));
        assert!(json.contains(r#"left":{"#));
        assert!(json.contains(r#"right":{"#));

        let dag = &mut QueryDag::from(&plan);

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let subplan = dag.node_weight(NodeIndex::new(0)).unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"global_limit_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"sort_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        let subplan = dag.node_weight(NodeIndex::new(1)).unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_join_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"right":{"execution_plan"#));
        assert!(subplan.get_plan_str().contains(r#"left":{"execution_plan"#));

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"global_limit_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"sort_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"projection_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"coalesce_batches_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"memory_exec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"execution_plan":"hash_join_exec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"right":{"execution_plan"#));
        assert!(subplan.get_plan_str().contains(r#"left":{"execution_plan"#));

        let batches = collect(plan).await?;

        let expected = vec![
            "+---+----+----+",
            "| a | b  | d  |",
            "+---+----+----+",
            "| a | 1  | 1  |",
            "| b | 10 | 10 |",
            "| c | 10 | 10 |",
            "+---+----+----+",
        ];

        test_utils::assert_batches_eq!(&expected, &batches);

        Ok(())
    }
}
