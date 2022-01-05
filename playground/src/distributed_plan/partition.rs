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

use crate::distributed_plan::shuffle_writer::ShuffleWriterExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan, Partitioning};
use flock::error::{FlockError, Result};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

type PartialQueryStageResult = (Arc<dyn ExecutionPlan>, Vec<Arc<ShuffleWriterExec>>);

/// Distributed Planer deals with the physical plan and convert it into
/// distributed plan.
pub struct DistributedPlanner {
    /// The next stage of the physical plan.
    pub next_stage_id: usize,
}

impl DistributedPlanner {
    /// Create a new distributed planner.
    pub fn new() -> Self {
        Self { next_stage_id: 0 }
    }
}

impl Default for DistributedPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedPlanner {
    /// Returns a vector of ExecutionPlans.
    #[allow(dead_code)]
    pub async fn plan_query_stages(
        &mut self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<Arc<ShuffleWriterExec>>> {
        println!("planning query stages\n");
        let (new_plan, mut stages) = self.plan_query_stages_internal(execution_plan).await?;
        stages.push(create_shuffle_writer(self.next_stage_id(), new_plan, None)?);
        Ok(stages)
    }

    /// Returns a potentially modified version of the input execution_plan along
    /// with the resulting query stages. This function is needed because the
    /// input execution_plan might need to be modified, but it might not hold a
    /// complete query stage (its parent might also belong to the same stage)
    #[allow(dead_code)]
    fn plan_query_stages_internal(
        &mut self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> BoxFuture<Result<PartialQueryStageResult>> {
        async move {
            // recurse down and replace children
            if execution_plan.children().is_empty() {
                return Ok((execution_plan, vec![]));
            }

            let mut stages = vec![];
            let mut children = vec![];
            for child in execution_plan.children() {
                let (new_child, mut child_stages) =
                    self.plan_query_stages_internal(child.clone()).await?;
                children.push(new_child);
                stages.append(&mut child_stages);
            }

            if let Some(coalesce) = execution_plan
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
            {
                let shuffle_writer =
                    create_shuffle_writer(self.next_stage_id(), children[0].clone(), None)?;
                let shuffle_reader: Arc<dyn ExecutionPlan> =
                    Arc::new(MemoryExec::try_new(&[], children[0].schema(), None)?);

                stages.push(shuffle_writer);
                Ok((coalesce.with_new_children(vec![shuffle_reader])?, stages))
            } else if let Some(repart) = execution_plan.as_any().downcast_ref::<RepartitionExec>() {
                match repart.output_partitioning() {
                    Partitioning::Hash(_, _) => {
                        let shuffle_writer = create_shuffle_writer(
                            self.next_stage_id(),
                            children[0].clone(),
                            Some(repart.partitioning().to_owned()),
                        )?;
                        let shuffle_reader: Arc<dyn ExecutionPlan> =
                            Arc::new(MemoryExec::try_new(&[], children[0].schema(), None)?);
                        stages.push(shuffle_writer);
                        Ok((shuffle_reader, stages))
                    }
                    _ => {
                        // remove any non-hash repartition from the distributed plan
                        Ok((children[0].clone(), stages))
                    }
                }
            } else if let Some(window) = execution_plan.as_any().downcast_ref::<WindowAggExec>() {
                Err(FlockError::NotImplemented(format!(
                    "WindowAggExec with window {:?}",
                    window
                )))
            } else {
                Ok((execution_plan.with_new_children(children)?, stages))
            }
        }
        .boxed()
    }

    /// Generate a new stage ID
    #[allow(dead_code)]
    fn next_stage_id(&mut self) -> usize {
        self.next_stage_id += 1;
        self.next_stage_id
    }
}

#[allow(dead_code)]
fn create_shuffle_writer(
    stage_id: usize,
    plan: Arc<dyn ExecutionPlan>,
    partitioning: Option<Partitioning>,
) -> Result<Arc<ShuffleWriterExec>> {
    Ok(Arc::new(ShuffleWriterExec::try_new(
        stage_id,
        plan,
        partitioning,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
    use datafusion::physical_plan::hash_join::HashJoinExec;
    use datafusion::physical_plan::sort::SortExec;
    use datafusion::physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, projection::ProjectionExec,
    };
    use datafusion::physical_plan::{displayable, ExecutionPlan};
    use datafusion::prelude::CsvReadOptions;
    use flock::datasource::tpch::*;
    use indoc::indoc;

    macro_rules! downcast_exec {
        ($exec: expr, $ty: ty) => {
            $exec.as_any().downcast_ref::<$ty>().unwrap()
        };
    }

    pub async fn datafusion_test_context(path: &str) -> Result<ExecutionContext> {
        let default_shuffle_partitions = 8;
        let config = ExecutionConfig::new().with_target_partitions(default_shuffle_partitions);
        let mut ctx = ExecutionContext::with_config(config);
        for table in TPCH_TABLES {
            let schema = get_tpch_schema(table);
            let options = CsvReadOptions::new()
                .schema(&schema)
                .delimiter(b'|')
                .has_header(false)
                .file_extension(".tbl");
            let dir = format!("{}/{}", path, table);
            ctx.register_csv(table, &dir, options).await?;
        }
        Ok(ctx)
    }

    #[tokio::test]
    async fn distributed_hash_aggregate_plan() -> Result<()> {
        let path = std::env::var("CARGO_MANIFEST_DIR").unwrap() + "/../flock/src/tests/data/tpch";
        let mut ctx = datafusion_test_context(&path).await?;

        // simplified form of TPC-H query 1
        let df = ctx
            .sql(indoc! {"
                    SELECT l_returnflag,
                           Sum(l_extendedprice * 1) AS sum_disc_price
                    FROM   lineitem
                    GROUP  BY l_returnflag
                    ORDER  BY l_returnflag;
                "})
            .await?;

        let plan = df.to_logical_plan();
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan).await?;

        let mut planner = DistributedPlanner::new();
        let stages = planner.plan_query_stages(plan).await?;
        for stage in &stages {
            println!(
                "=== Physical subplan ===\n{}\n",
                displayable(stage.as_ref()).indent()
            );
        }

        #[rustfmt::skip]
        // Expected result:
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "l_returnflag", index: 0 }], 8))
        //   HashAggregateExec: mode=Partial, gby=[l_returnflag@1 as l_returnflag], aggr=[SUM(lineitem.l_extendedprice Multiply Int64(1))]
        //     CsvExec: files=[/home/gangliao/Squirtle/playground/../flock/src/tests/data/tpch/lineitem/partition0.tbl, /home/gangliao/Squirtle/playground/../flock/src/tests/data/tpch/lineitem/partition1.tbl], has_header=false, batch_size=8192, limit=None
        //
        // === Physical subplan ===
        // ShuffleWriterExec: None
        //   ProjectionExec: expr=[l_returnflag@0 as l_returnflag, SUM(lineitem.l_extendedprice * Int64(1))@1 as sum_disc_price]
        //     HashAggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag], aggr=[SUM(lineitem.l_extendedprice Multiply Int64(1))]
        //       CoalesceBatchesExec: target_batch_size=4096
        //         MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: None
        //   SortExec: [l_returnflag@0 ASC NULLS LAST]
        //     CoalescePartitionsExec
        //       MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(3, stages.len());

        // verify stage 0
        let stage0 = stages[0].children()[0].clone();
        let partial_hash = downcast_exec!(stage0, HashAggregateExec);
        assert!(*partial_hash.mode() == AggregateMode::Partial);
        let input = partial_hash.children()[0].clone();
        println!("Stage 0:\nInput {:#?}\n", input.schema());

        // verify stage 1
        let stage1 = stages[1].children()[0].clone();
        let projection = downcast_exec!(stage1, ProjectionExec);
        let final_hash = projection.children()[0].clone();
        let final_hash = downcast_exec!(final_hash, HashAggregateExec);
        assert!(*final_hash.mode() == AggregateMode::FinalPartitioned);
        let coalesce = final_hash.children()[0].clone();
        let coalesce = downcast_exec!(coalesce, CoalesceBatchesExec);
        let input = coalesce.children()[0].clone();
        let input = downcast_exec!(input, MemoryExec);
        println!("Stage 1:\nInput {:#?}\n", input.schema());

        // verify stage 2
        let stage2 = stages[2].children()[0].clone();
        let sort = downcast_exec!(stage2, SortExec);
        let coalesce_partitions = sort.children()[0].clone();
        let coalesce_partitions = downcast_exec!(coalesce_partitions, CoalescePartitionsExec);
        assert_eq!(
            coalesce_partitions.output_partitioning().partition_count(),
            1
        );
        let input = coalesce_partitions.children()[0].clone();
        let input = downcast_exec!(input, MemoryExec);
        println!("Stage 2:\nInput {:#?}\n", input.schema());

        Ok(())
    }

    #[tokio::test]
    async fn distributed_join_plan() -> Result<()> {
        let path = std::env::var("CARGO_MANIFEST_DIR").unwrap() + "/../flock/src/tests/data/tpch";
        let mut ctx = datafusion_test_context(&path).await?;

        // simplified form of TPC-H query 12
        let df = ctx
            .sql(indoc! {"
                SELECT  l_shipmode,
                        SUM(CASE
                            WHEN o_orderpriority = '1-URGENT'
                                    OR o_orderpriority = '2-HIGH' THEN 1
                            ELSE 0
                            END) AS high_line_count,
                        SUM(CASE
                            WHEN o_orderpriority <> '1-URGENT'
                                AND o_orderpriority <> '2-HIGH' THEN 1
                            ELSE 0
                            END) AS low_line_count
                FROM    lineitem
                        join orders
                            ON l_orderkey = o_orderkey
                WHERE   l_shipmode IN ( 'MAIL', 'SHIP' )
                        AND l_commitdate < l_receiptdate
                        AND l_shipdate < l_commitdate
                        AND l_receiptdate >= DATE '1994-01-01'
                        AND l_receiptdate < DATE '1995-01-01'
                GROUP   BY l_shipmode
                ORDER   BY l_shipmode;
            "})
            .await?;

        let plan = df.to_logical_plan();
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan).await?;

        let mut planner = DistributedPlanner::new();
        let stages = planner.plan_query_stages(plan).await?;
        for stage in &stages {
            println!(
                "=== Physical subplan ===\n{}\n",
                displayable(stage.as_ref()).indent()
            );
        }

        #[rustfmt::skip]
        // Expected result:
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "l_orderkey", index: 0 }], 8))
        //   CoalesceBatchesExec: target_batch_size=4096
        //     FilterExec: l_shipmode@4 IN ([Literal { value: Utf8("MAIL") }, Literal { value: Utf8("SHIP") }])
        //       CoalesceBatchesExec: target_batch_size=4096
        //         FilterExec: l_commitdate@2 < l_receiptdate@3 AND l_shipdate@1 < l_commitdate@2 AND l_receiptdate@3 >= 8766 AND l_receiptdate@3 < 9131
        //           CsvExec: files=[/home/gangliao/Squirtle/playground/../flock/src/tests/data/tpch/lineitem/partition0.tbl, /home/gangliao/Squirtle/playground/../flock/src/tests/data/tpch/lineitem/partition1.tbl], has_header=false, batch_size=8192, limit=None
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "o_orderkey", index: 0 }], 8))
        //   CsvExec: files=[/home/gangliao/Squirtle/playground/../flock/src/tests/data/tpch/orders/orders.tbl], has_header=false, batch_size=8192, limit=None
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "l_shipmode", index: 0 }], 8))
        //   HashAggregateExec: mode=Partial, gby=[l_shipmode@4 as l_shipmode], aggr=[SUM(CASE WHEN #orders.o_orderpriority = Utf8("1-URGENT") OR #orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), SUM(CASE WHEN #orders.o_orderpriority != Utf8("1-URGENT") AND #orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
        //     CoalesceBatchesExec: target_batch_size=4096
        //       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "l_orderkey", index: 0 }, Column { name: "o_orderkey", index: 0 })]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           MemoryExec: partitions=0, partition_sizes=[]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: None
        //   ProjectionExec: expr=[l_shipmode@0 as l_shipmode, SUM(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@1 as high_line_count, SUM(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@2 as low_line_count]
        //     HashAggregateExec: mode=FinalPartitioned, gby=[l_shipmode@0 as l_shipmode], aggr=[SUM(CASE WHEN #orders.o_orderpriority = Utf8("1-URGENT") OR #orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), SUM(CASE WHEN #orders.o_orderpriority != Utf8("1-URGENT") AND #orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
        //       CoalesceBatchesExec: target_batch_size=4096
        //         MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: None
        //   SortExec: [l_shipmode@0 ASC NULLS LAST]
        //     CoalescePartitionsExec
        //       MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(5, stages.len());

        // verify partitioning for each stage

        // csv "lineitem" (2 files)
        assert_eq!(
            2,
            stages[0].children()[0]
                .output_partitioning()
                .partition_count()
        );
        assert_eq!(
            8,
            stages[0]
                .shuffle_output_partitioning()
                .unwrap()
                .partition_count()
        );

        // csv "orders" (1 file)
        assert_eq!(
            1,
            stages[1].children()[0]
                .output_partitioning()
                .partition_count()
        );
        assert_eq!(
            8,
            stages[1]
                .shuffle_output_partitioning()
                .unwrap()
                .partition_count()
        );

        // join and partial hash aggregate
        let input = stages[2].children()[0].clone();
        assert_eq!(
            8,
            stages[2]
                .shuffle_output_partitioning()
                .unwrap()
                .partition_count()
        );

        let hash_agg = downcast_exec!(input, HashAggregateExec);

        let coalesce_batches = hash_agg.children()[0].clone();
        let coalesce_batches = downcast_exec!(coalesce_batches, CoalesceBatchesExec);

        let join = coalesce_batches.children()[0].clone();
        let join = downcast_exec!(join, HashJoinExec);

        let join_input_1 = join.children()[0].clone();
        // skip CoalesceBatches
        let input_1 = join_input_1.children()[0].clone();
        let input_1 = downcast_exec!(input_1, MemoryExec);
        println!("Stage 2:\nInput_1 {:#?}\n", input_1.schema());

        let join_input_2 = join.children()[1].clone();
        // skip CoalesceBatches
        let input_2 = join_input_2.children()[0].clone();
        let input_2 = downcast_exec!(input_2, MemoryExec);
        println!("Stage 2:\nInput_2 {:#?}\n", input_2.schema());

        // final partitioned hash aggregate
        assert!(stages[3].shuffle_output_partitioning().is_none());

        // coalesce partitions and sort
        assert_eq!(
            1,
            stages[4].children()[0]
                .output_partitioning()
                .partition_count()
        );
        assert!(stages[4].shuffle_output_partitioning().is_none());

        Ok(())
    }
}
