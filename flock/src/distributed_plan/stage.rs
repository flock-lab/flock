// Copyright (c) 2020-present, UMD Database Group.
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

//! A directed acyclic graph (DAG) data structure to hold all sub-plans of the
//! query statement.

extern crate daggy;
use crate::configs::FLOCK_FUNCTION_CONCURRENCY;
use crate::error::{FlockError, Result};
use daggy::{Dag, NodeIndex, Walker};
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use serde_json::Value;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

type QueryStageEdge = ();
type DistributedPlan = Dag<QueryStage, QueryStageEdge>;

/// A query stage that represents a sub-plan.
#[derive(Debug, Clone)]
pub struct QueryStage {
    /// Subplans of the query statement.
    pub stage:       Vec<Arc<dyn ExecutionPlan>>,
    /// Function concurrency in cloud environment.
    pub concurrency: usize,
}

impl QueryStage {
    /// Return the displable string of the query plan in the stage.
    pub fn get_plan_str(&self) -> String {
        let mut plan_str = String::new();
        for plan in &self.stage {
            plan_str.push_str(&format!("{}\n", displayable(plan.as_ref()).indent()));
        }
        plan_str.to_string()
    }

    /// Return the concurrency of the query plan in the stage.
    pub fn get_concurrency(&self) -> usize {
        self.concurrency
    }

    /// Create a new query stage from a physical plan with a given concurrency.
    pub fn from_with_concurrency(
        stage: Vec<Arc<dyn ExecutionPlan>>,
        concurrency: usize,
    ) -> QueryStage {
        QueryStage { stage, concurrency }
    }
}

impl From<Vec<Arc<dyn ExecutionPlan>>> for QueryStage {
    fn from(stage: Vec<Arc<dyn ExecutionPlan>>) -> QueryStage {
        QueryStage {
            stage,
            concurrency: *FLOCK_FUNCTION_CONCURRENCY,
        }
    }
}

impl Deref for QueryStage {
    type Target = Vec<Arc<dyn ExecutionPlan>>;

    fn deref(&self) -> &Self::Target {
        &self.stage
    }
}

/// The DAG is constructed from the query plan.
///
/// DAG allows the traversal of the plan in a topological order. It's also used
/// to determine the number of the stages in the query plan, how many stages are
/// executed in parallel, and how many dependencies each stage has. It is used
/// to execute the query plan in a distributed fashion.
#[derive(Debug)]
pub struct QueryDag {
    dag: DistributedPlan,
}

impl Deref for QueryDag {
    type Target = DistributedPlan;

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
    pub fn new() -> QueryDag {
        QueryDag {
            dag: DistributedPlan::new(),
        }
    }

    /// The total number of nodes in the Dag.
    pub fn node_count(&self) -> usize {
        self.dag.node_count()
    }

    /// Build a Dag representation of a given plan.
    pub fn from(plan: Arc<dyn ExecutionPlan>) -> Result<QueryDag> {
        build_query_dag(plan)
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
    pub fn add_node(&mut self, node: QueryStage) -> NodeIndex {
        self.dag.add_node(node)
    }

    /// Add a root node to the `QueryDag`.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Returns the index of the new root.
    pub fn add_parent(&mut self, child: NodeIndex, node: QueryStage) -> NodeIndex {
        let (_, n) = self.dag.add_parent(child, (), node);
        n
    }

    /// Return a node for a given id.
    pub fn get_node(&self, id: NodeIndex) -> Option<&QueryStage> {
        self.dag.node_weight(id)
    }

    /// Return a dsplayable string of the specified node in the dag.
    pub fn get_plan_str(&self, id: NodeIndex) -> String {
        let mut plan_str = String::new();
        let stage = &self.dag.node_weight(id).unwrap().stage;
        for plan in stage {
            plan_str.push_str(&format!("{}\n", displayable(plan.as_ref()).indent()));
        }
        plan_str.to_string()
    }

    /// Add a new child node to the node at the given `NodeIndex`.
    /// Return the node's `NodeIndex`.
    ///
    /// child -> edge -> node
    ///
    /// Compute in **O(1)** time.
    pub fn add_child(&mut self, parent: NodeIndex, node: QueryStage) -> NodeIndex {
        let (_, n) = self.dag.add_child(parent, (), node);
        n
    }

    /// Return a **Vec** with all depended subplans for the given node.
    pub fn get_depended_stages(&self, node: NodeIndex) -> Vec<(QueryStage, NodeIndex)> {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .map(|(_, n)| (self.dag.node_weight(n).unwrap().clone(), n))
            .collect()
    }

    /// Return a **Vec** with all children for the given node.
    pub fn get_sub_stages(&self, node: NodeIndex) -> Vec<(QueryStage, NodeIndex)> {
        let mut children = Vec::new();
        for (_, n) in self.dag.children(node).iter(&self.dag) {
            children.push((self.dag.node_weight(n).unwrap().clone(), n));
            children.append(&mut self.get_sub_stages(n));
        }
        children
    }

    /// Return all nodes in the dag. The order is topological.
    pub fn get_all_stages(&self) -> Vec<&QueryStage> {
        let mut stages = vec![];
        let count = self.node_count();
        stages.push(self.get_node(NodeIndex::new(count - 1)).unwrap());

        self.dag
            .recursive_walk(NodeIndex::new(count - 1), |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .for_each(|(_, n)| stages.push(self.dag.node_weight(n).unwrap()));

        stages
    }

    /// Return the internal daggy.
    pub fn context(&mut self) -> &mut DistributedPlan {
        &mut self.dag
    }

    /// Add a new node to the `QueryDag`.
    fn insert(
        &mut self,
        parent: NodeIndex,
        nodes: Vec<Value>,
        concurrency: usize,
    ) -> Result<NodeIndex> {
        let stage = nodes
            .into_iter()
            .map(|node| serde_json::from_value(node).unwrap())
            .collect();
        if parent == NodeIndex::end() {
            Ok(self.add_node(QueryStage { stage, concurrency }))
        } else {
            // TODO: call add_parent instead of add_child
            Ok(self.add_child(parent, QueryStage { stage, concurrency }))
        }
    }
}

/// Build a DAG from a query plan.
///
/// # Arguments
/// * `plan` - The query plan.
///
/// # Returns
/// * `QueryDag` - the DAG representation of the query plan.
pub fn build_query_dag(plan: Arc<dyn ExecutionPlan>) -> Result<QueryDag> {
    build_query_dag_from_serde_json(plan)
}

fn build_query_dag_from_serde_json(plan: Arc<dyn ExecutionPlan>) -> Result<QueryDag> {
    let mut dag = QueryDag::new();
    let mut root = serde_json::to_value(&plan).unwrap();
    let mut json = &mut root;
    let mut leaf = NodeIndex::end();
    let mut curr = plan.clone();
    loop {
        match json["execution_plan"].as_str() {
            Some("hash_aggregate_exec") => match json["mode"].as_str() {
                Some("FinalPartitioned") => {
                    // Split the plan into two subplans
                    let object = (*json["input"].take().as_object().ok_or_else(|| {
                        FlockError::QueryStage(
                            "Failed to parse input for HashAggregateExec".to_string(),
                        )
                    })?)
                    .clone();
                    // Add a input for the new subplan
                    let input: Arc<dyn ExecutionPlan> = Arc::new(MemoryExec::try_new(
                        &[],
                        curr.children()[0].schema().clone(),
                        None,
                    )?);
                    json["input"] = serde_json::to_value(input)?;
                    // Add the new subplan to DAG
                    leaf = dag.insert(leaf, vec![root], *FLOCK_FUNCTION_CONCURRENCY)?;
                    // Point to the next subplan
                    root = Value::Object(object);
                    json = &mut root;
                }
                Some("Final") => {
                    // Split the plan into two subplans
                    let object = (*json["input"].take().as_object().ok_or_else(|| {
                        FlockError::QueryStage(
                            "Failed to parse input for HashAggregateExec".to_string(),
                        )
                    })?)
                    .clone();
                    // Add a input for the new subplan
                    let input: Arc<dyn ExecutionPlan> = Arc::new(MemoryExec::try_new(
                        &[],
                        curr.children()[0].schema().clone(),
                        None,
                    )?);
                    json["input"] = serde_json::to_value(input)?;
                    // Add the new subplan to DAG
                    leaf = dag.insert(leaf, vec![root], 1)?;
                    // Point to the next subplan
                    root = Value::Object(object);
                    json = &mut root;
                }
                Some("Partial") => json = &mut json["input"],
                _ => {
                    println!("{:?}", json["aggregate_mode"].as_str());
                    return Err(FlockError::QueryStage(
                        "Failed to parse aggregate mode for HashAggregateExec".to_string(),
                    ));
                }
            },
            Some("hash_join_exec") => {
                let left_obj = (*json["left"].take().as_object().ok_or_else(|| {
                    FlockError::QueryStage(
                        "Failed to parse left input for HashJoinExec".to_string(),
                    )
                })?)
                .clone();
                let right_obj = (*json["right"].take().as_object().ok_or_else(|| {
                    FlockError::QueryStage(
                        "Failed to parse right input for HashJoinExec".to_string(),
                    )
                })?)
                .clone();

                let left: Arc<dyn ExecutionPlan> =
                    Arc::new(MemoryExec::try_new(&[], curr.children()[0].schema(), None)?);
                let right: Arc<dyn ExecutionPlan> =
                    Arc::new(MemoryExec::try_new(&[], curr.children()[1].schema(), None)?);

                json["left"] = serde_json::to_value(left)?;
                json["right"] = serde_json::to_value(right)?;

                leaf = dag.insert(leaf, vec![root], *FLOCK_FUNCTION_CONCURRENCY)?;
                dag.insert(
                    leaf,
                    vec![Value::Object(left_obj), Value::Object(right_obj)],
                    *FLOCK_FUNCTION_CONCURRENCY,
                )?;
                return Ok(dag);
            }
            Some("sort_exec") => {
                let object = (*json["input"].take().as_object().ok_or_else(|| {
                    FlockError::QueryStage("Failed to parse input for SortExec".to_string())
                })?)
                .clone();
                // Add a input for the new subplan
                let input: Arc<dyn ExecutionPlan> = Arc::new(MemoryExec::try_new(
                    &[],
                    curr.children()[0].schema().clone(),
                    None,
                )?);
                json["input"] = serde_json::to_value(input)?;
                // Add the new subplan to DAG
                leaf = dag.insert(leaf, vec![root], 1)?;
                // Point to the next subplan
                root = Value::Object(object);
                json = &mut root;
            }
            _ => json = &mut json["input"],
        }
        if !json.is_object() {
            break;
        }
        curr = curr.children()[0].clone();
    }

    dag.insert(leaf, vec![root], *FLOCK_FUNCTION_CONCURRENCY)?;
    assert!(dag.node_count() >= 1);

    Ok(dag)
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate daggy;
    use daggy::NodeIndex;

    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::displayable;
    use std::sync::Arc;

    async fn quick_init(sql: &str) -> Result<QueryDag> {
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
        let physical_plan = ctx.create_physical_plan(&optimized_plan).await?;

        println!(
            "=== Physical Plan ===\n{}\n",
            displayable(physical_plan.as_ref()).indent()
        );

        QueryDag::from(physical_plan)
    }

    // Mem -> Proj
    #[tokio::test]
    async fn simple_select() -> Result<()> {
        let sql = concat!("SELECT c1 FROM test_table");
        quick_init(sql).await?;
        // === Physical Plan ===
        // ProjectionExec: expr=[c1@0 as c1]
        //   RepartitionExec: partitioning=RoundRobinBatch(16)
        //     MemoryExec: partitions=1, partition_sizes=[1]
        Ok(())
    }

    #[tokio::test]
    async fn select_alias() -> Result<()> {
        let sql = concat!("SELECT c1 as col_1 FROM test_table");
        quick_init(sql).await?;
        // === Physical Plan ===
        // ProjectionExec: expr=[c1@0 as col_1]
        //   RepartitionExec: partitioning=RoundRobinBatch(16)
        //     MemoryExec: partitions=1, partition_sizes=[1]
        Ok(())
    }

    #[tokio::test]
    async fn cast() -> Result<()> {
        let sql = concat!("SELECT CAST(c2 AS int) FROM test_table");
        quick_init(sql).await?;
        // === Physical Plan ===
        // ProjectionExec: expr=[CAST(c2@0 AS Int32) as CAST(test_table.c2 AS Int32)]
        //   RepartitionExec: partitioning=RoundRobinBatch(16)
        //     MemoryExec: partitions=1, partition_sizes=[1]
        Ok(())
    }

    #[tokio::test]
    async fn math() -> Result<()> {
        let sql = concat!("SELECT c1+c2 FROM test_table");
        quick_init(sql).await?;
        // === Physical Plan ===
        // ProjectionExec: expr=[CAST(c1@0 AS Float64) + c2@1 as test_table.c1 Plus
        // test_table.c2]   RepartitionExec: partitioning=RoundRobinBatch(16)
        //     MemoryExec: partitions=1, partition_sizes=[1]
        Ok(())
    }

    #[tokio::test]
    async fn math_less() -> Result<()> {
        let sql = concat!("SELECT c1>=c2 FROM test_table");
        quick_init(sql).await?;
        // === Physical Plan ===
        // ProjectionExec: expr=[CAST(c1@0 AS Float64) >= c2@1 as test_table.c1 GtEq
        // test_table.c2]   RepartitionExec: partitioning=RoundRobinBatch(16)
        //     MemoryExec: partitions=1, partition_sizes=[1]
        Ok(())
    }

    // Filter
    // Memory -> Filter -> CoalesceBatches -> Projection
    #[tokio::test]
    async fn filter_query() -> Result<()> {
        let sql = concat!("SELECT c1, c2 FROM test_table WHERE c2 < 99");
        quick_init(sql).await?;
        // === Physical Plan ===
        // ProjectionExec: expr=[c1@0 as c1, c2@1 as c2]
        //   CoalesceBatchesExec: target_batch_size=4096
        //     FilterExec: c2@1 < CAST(99 AS Float64)
        //       RepartitionExec: partitioning=RoundRobinBatch(16)
        //         MemoryExec: partitions=1, partition_sizes=[1]
        Ok(())
    }

    // Mem -> Filter -> Coalesce
    #[tokio::test]
    async fn filter_select_all() -> Result<()> {
        let sql = concat!("SELECT * FROM test_table WHERE c2 < 99");
        #[rustfmt::skip]
        // === Physical Plan ===
        // ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3, c4@3 as c4, c5@4 as c5, neg@5 as neg]
        //   CoalesceBatchesExec: target_batch_size=4096
        //     FilterExec: c2@1 < CAST(99 AS Float64)
        //       RepartitionExec: partitioning=RoundRobinBatch(16)
        //         MemoryExec: partitions=1, partition_sizes=[1]
        quick_init(sql).await?;
        Ok(())
    }

    // Aggregate
    // Mem -> HashAgg -> HashAgg
    #[tokio::test]
    async fn aggregate_query_no_group_by_count_distinct_wide() -> Result<()> {
        let sql = concat!("SELECT COUNT(DISTINCT c1) FROM test_table");
        let dag = &mut quick_init(sql).await?;
        #[rustfmt::skip]
        // === Physical Plan ===
        // ProjectionExec: expr=[COUNT(DISTINCT test_table.c1)@0 as COUNT(DISTINCT test_table.c1)]
        //   ProjectionExec: expr=[COUNT(test_table.c1)@0 as COUNT(DISTINCT test_table.c1)]
        //     HashAggregateExec: mode=Final, gby=[], aggr=[COUNT(test_table.c1)]
        //       CoalescePartitionsExec
        //         HashAggregateExec: mode=Partial, gby=[], aggr=[COUNT(test_table.c1)]
        //           HashAggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[]
        //             CoalesceBatchesExec: target_batch_size=4096
        //               RepartitionExec: partitioning=Hash([Column { name: "c1", index: 0 }], 16)
        //                 HashAggregateExec: mode=Partial, gby=[c1@0 as c1], aggr=[]
        //                   RepartitionExec: partitioning=RoundRobinBatch(16)
        //                     MemoryExec: partitions=1, partition_sizes=[1]

        assert_eq!(3, dag.node_count());
        assert_eq!(2, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains("HashAggregateExec: mode=Final"));
        assert!(subplan.get_plan_str().contains("MemoryExec"));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains("HashAggregateExec: mode=FinalPartitioned"));
        assert!(subplan.get_plan_str().contains("MemoryExec"));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains("HashAggregateExec: mode=Partial"));
        assert!(subplan.get_plan_str().contains("MemoryExec"));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_query_no_group_by() -> Result<()> {
        let sql = concat!("SELECT MIN(c1), AVG(c4), COUNT(c3) FROM test_table");
        #[rustfmt::skip]
        // === Physical Plan ===
        // ProjectionExec: expr=[MIN(test_table.c1)@0 as MIN(test_table.c1), AVG(test_table.c4)@1 as AVG(test_table.c4), COUNT(test_table.c3)@2 as COUNT(test_table.c3)]
        //   HashAggregateExec: mode=Final, gby=[], aggr=[MIN(test_table.c1), AVG(test_table.c4), COUNT(test_table.c3)]
        //     CoalescePartitionsExec
        //       HashAggregateExec: mode=Partial, gby=[], aggr=[MIN(test_table.c1), AVG(test_table.c4), COUNT(test_table.c3)]
        //         RepartitionExec: partitioning=RoundRobinBatch(16)
        //           MemoryExec: partitions=1, partition_sizes=[1]
        quick_init(sql).await?;
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
        #[rustfmt::skip]
        // === Physical Plan ===
        // ProjectionExec: expr=[MIN(test_table.c1)@1 as MIN(test_table.c1), AVG(test_table.c4)@2 as AVG(test_table.c4), COUNT(test_table.c3)@3 as c3_count]
        //   HashAggregateExec: mode=FinalPartitioned, gby=[c3@0 as c3], aggr=[MIN(test_table.c1), AVG(test_table.c4), COUNT(test_table.c3)]
        //     CoalesceBatchesExec: target_batch_size=4096
        //       RepartitionExec: partitioning=Hash([Column { name: "c3", index: 0 }], 16)
        //         HashAggregateExec: mode=Partial, gby=[c3@1 as c3], aggr=[MIN(test_table.c1), AVG(test_table.c4), COUNT(test_table.c3)]
        //           RepartitionExec: partitioning=RoundRobinBatch(16)
        //             MemoryExec: partitions=1, partition_sizes=[1]
        let dag = &mut quick_init(sql).await?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.get_plan_str().contains("ProjectionExec"));
        assert!(subplan
            .get_plan_str()
            .contains("HashAggregateExec: mode=FinalPartitioned"));
        assert!(subplan.get_plan_str().contains("MemoryExec"));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains("HashAggregateExec: mode=Partial"));
        assert!(subplan.get_plan_str().contains("MemoryExec"));

        Ok(())
    }

    // Sort
    // Mem -> Project -> Sort
    #[tokio::test]
    async fn sort() -> Result<()> {
        let sql = concat!("SELECT c1, c2, c3 ", "FROM test_table ", "ORDER BY c1 ");
        // === Physical Plan ===
        // SortExec: [c1@0 ASC NULLS LAST]
        //   CoalescePartitionsExec
        //     ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
        //       RepartitionExec: partitioning=RoundRobinBatch(16)
        //         MemoryExec: partitions=1, partition_sizes=[1]
        quick_init(sql).await?;
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
        // === Physical Plan ===
        // GlobalLimitExec: limit=4
        //   SortExec: [c1@0 ASC NULLS LAST]
        //     CoalescePartitionsExec
        //       ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
        //         RepartitionExec: partitioning=RoundRobinBatch(16)
        //           MemoryExec: partitions=1, partition_sizes=[1]
        quick_init(sql).await?;
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
        #[rustfmt::skip]
        // === Physical Plan ===
        // ProjectionExec: expr=[MIN(test_table.c1)@0 as MIN(test_table.c1), AVG(test_table.c4)@1 as AVG(test_table.c4), COUNT(test_table.c3)@2 as c3_count]
        //   HashAggregateExec: mode=Final, gby=[], aggr=[MIN(test_table.c1), AVG(test_table.c4), COUNT(test_table.c3)]
        //     CoalescePartitionsExec
        //       HashAggregateExec: mode=Partial, gby=[], aggr=[MIN(test_table.c1), AVG(test_table.c4), COUNT(test_table.c3)]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           FilterExec: c2@1 < CAST(99 AS Float64)
        //             RepartitionExec: partitioning=RoundRobinBatch(16)
        //               MemoryExec: partitions=1, partition_sizes=[1]
        let dag = &mut quick_init(sql).await?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.get_plan_str().contains(r#"ProjectionExec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"HashAggregateExec: mode=Final"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"HashAggregateExec: mode=Partial"#));
        assert!(subplan.get_plan_str().contains(r#"CoalesceBatchesExec"#));
        assert!(subplan.get_plan_str().contains(r#"FilterExec"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));

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
        #[rustfmt::skip]
        // === Physical Plan ===
        // ProjectionExec: expr=[MAX(test_table.c1)@1 as MAX(test_table.c1), MIN(test_table.c2)@2 as MIN(test_table.c2), c3@0 as c3]
        //   HashAggregateExec: mode=FinalPartitioned, gby=[c3@0 as c3], aggr=[MAX(test_table.c1), MIN(test_table.c2)]
        //     CoalesceBatchesExec: target_batch_size=4096
        //       RepartitionExec: partitioning=Hash([Column { name: "c3", index: 0 }], 16)
        //         HashAggregateExec: mode=Partial, gby=[c3@2 as c3], aggr=[MAX(test_table.c1), MIN(test_table.c2)]
        //           CoalesceBatchesExec: target_batch_size=4096
        //             FilterExec: c2@1 < CAST(101 AS Float64) AND c1@0 > 91
        //               RepartitionExec: partitioning=RoundRobinBatch(16)
        //                 MemoryExec: partitions=1, partition_sizes=[1]
        let dag = &mut quick_init(sql).await?;

        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.get_plan_str().contains(r#"ProjectionExec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"HashAggregateExec: mode=FinalPartitioned"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"HashAggregateExec: mode=Partial"#));
        assert!(subplan.get_plan_str().contains(r#"CoalesceBatchesExec"#));
        assert!(subplan.get_plan_str().contains(r#"FilterExec"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));

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
        #[rustfmt::skip]
        // === Physical Plan ===
        // GlobalLimitExec: limit=3
        //   SortExec: [c3@2 ASC NULLS LAST]
        //     CoalescePartitionsExec
        //       ProjectionExec: expr=[MAX(test_table.c1)@1 as MAX(test_table.c1), MIN(test_table.c2)@2 as MIN(test_table.c2), c3@0 as c3]
        //         HashAggregateExec: mode=FinalPartitioned, gby=[c3@0 as c3], aggr=[MAX(test_table.c1), MIN(test_table.c2)]
        //           CoalesceBatchesExec: target_batch_size=4096
        //             RepartitionExec: partitioning=Hash([Column { name: "c3", index: 0 }], 16)
        //               HashAggregateExec: mode=Partial, gby=[c3@2 as c3], aggr=[MAX(test_table.c1), MIN(test_table.c2)]
        //                 CoalesceBatchesExec: target_batch_size=4096
        //                   FilterExec: c2@1 < CAST(101 AS Float64)
        //                     RepartitionExec: partitioning=RoundRobinBatch(16)
        //                       MemoryExec: partitions=1, partition_sizes=[1]
        let dag = &mut quick_init(sql).await?;

        assert_eq!(3, dag.node_count());
        assert_eq!(2, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut subplan = iter.next().unwrap();
        assert!(subplan.get_plan_str().contains(r#"GlobalLimitExec"#));
        assert!(subplan.get_plan_str().contains(r#"SortExec"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));

        subplan = iter.next().unwrap();
        assert!(subplan.get_plan_str().contains(r#"ProjectionExec"#));
        assert!(subplan
            .get_plan_str()
            .contains(r#"HashAggregateExec: mode=FinalPartitioned"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));

        subplan = iter.next().unwrap();
        assert!(subplan
            .get_plan_str()
            .contains(r#"HashAggregateExec: mode=Partial"#));
        assert!(subplan.get_plan_str().contains(r#"CoalesceBatchesExec"#));
        assert!(subplan.get_plan_str().contains(r#"FilterExec"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));

        Ok(())
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

        let plan = ctx.create_logical_plan(sql)?;
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan).await?;
        #[rustfmt::skip]
        // === Physical Plan ===
        // GlobalLimitExec: limit=3
        //   SortExec: [a@0 ASC NULLS LAST]
        //     CoalescePartitionsExec
        //       ProjectionExec: expr=[a@0 as a, b@1 as b, d@3 as d]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "a", index: 0 }, Column { name: "c", index: 0 })]
        //             CoalesceBatchesExec: target_batch_size=4096
        //               RepartitionExec: partitioning=Hash([Column { name: "a", index: 0 }], 16)
        //                 RepartitionExec: partitioning=RoundRobinBatch(16)
        //                   MemoryExec: partitions=1, partition_sizes=[1]
        //             CoalesceBatchesExec: target_batch_size=4096
        //               RepartitionExec: partitioning=Hash([Column { name: "c", index: 0 }], 16)
        //                 RepartitionExec: partitioning=RoundRobinBatch(16)
        //                   MemoryExec: partitions=1, partition_sizes=[1]

        println!(
            "=== Physical Plan ===\n{}\n",
            displayable(plan.as_ref()).indent()
        );

        // The simplified workflow is:
        //
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

        let dag = &mut QueryDag::from(plan.clone())?;

        assert_eq!(3, dag.node_count());
        assert_eq!(2, dag.edge_count());

        let stages = dag.get_all_stages();
        let n = dag.node_count();

        println!("=== Query Stage 0 ===");
        let subplan = dag.node_weight(NodeIndex::new(0)).unwrap();
        println!("{}", subplan.get_plan_str());
        assert!(subplan.get_plan_str().contains(r#"GlobalLimitExec"#));
        assert!(subplan.get_plan_str().contains(r#"SortExec"#));
        assert!(subplan.get_plan_str().contains(r#"MemoryExec"#));
        assert_eq!(stages[n - 1].get_plan_str(), subplan.get_plan_str());

        println!("=== Query Stage 1 ===");
        let subplan = dag.node_weight(NodeIndex::new(1)).unwrap();
        println!("{}", subplan.get_plan_str());
        assert!(subplan.get_plan_str().contains(r#"HashJoinExec"#));
        assert!(subplan.get_plan_str().matches(r#"MemoryExec"#).count() == 2);
        assert_eq!(stages[n - 2].get_plan_str(), subplan.get_plan_str());

        println!("=== Query Stage 2 ===");
        let subplan = dag.node_weight(NodeIndex::new(2)).unwrap();
        println!("{}", subplan.get_plan_str());
        assert!(
            subplan
                .get_plan_str()
                .matches(r#"CoalesceBatchesExec"#)
                .count()
                == 2
        );
        assert!(subplan.get_plan_str().matches(r#"MemoryExec"#).count() == 2);
        assert_eq!(stages[n - 3].get_plan_str(), subplan.get_plan_str());

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

        crate::assert_batches_eq!(&expected, &batches);

        Ok(())
    }
}
