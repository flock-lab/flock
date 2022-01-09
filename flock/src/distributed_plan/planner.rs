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

//! Distributed plnner is a unified API for users to split their query plan into
//! multiple functions, and execute them in distributed fashion on cloud.

use crate::distributed_plan::shuffle_writer::ShuffleWriterExec;
use crate::error::Result;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{memory::MemoryExec, ExecutionPlan, Partitioning};
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
    use crate::datasource::nexmark::*;
    use crate::datasource::ysb::*;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
    use datafusion::physical_plan::hash_join::HashJoinExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::{displayable, ExecutionPlan};

    macro_rules! downcast_exec {
        ($exec: expr, $ty: ty) => {
            $exec.as_any().downcast_ref::<$ty>().unwrap()
        };
    }

    #[tokio::test]
    async fn nexmark_q1_distributed_plan() -> Result<()> {
        let mut ctx = register_nexmark_tables().await?;
        let df = ctx
            .sql(include_str!("../../../benchmarks/src/nexmark/query/q1.sql"))
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
        // ShuffleWriterExec: None
        //   ProjectionExec: expr=[auction@0 as auction, bidder@1 as bidder, 0.908 * CAST(price@2 AS Float64) as price, b_date_time@3 as b_date_time]
        //     MemoryExec: partitions=1, partition_sizes=[1]
        assert_eq!(1, stages.len());

        Ok(())
    }

    #[tokio::test]
    async fn nexmark_q2_distributed_plan() -> Result<()> {
        let mut ctx = register_nexmark_tables().await?;
        let df = ctx
            .sql(include_str!("../../../benchmarks/src/nexmark/query/q2.sql"))
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
        // ShuffleWriterExec: None
        //   ProjectionExec: expr=[auction@0 as auction, price@1 as price]
        //     CoalesceBatchesExec: target_batch_size=4096
        //       FilterExec: CAST(auction@0 AS Int64) % 123 = 0
        //         MemoryExec: partitions=1, partition_sizes=[1]
        assert_eq!(1, stages.len());

        Ok(())
    }

    #[tokio::test]
    async fn nexmark_q3_distributed_plan() -> Result<()> {
        let mut ctx = register_nexmark_tables().await?;
        let df = ctx
            .sql(include_str!("../../../benchmarks/src/nexmark/query/q3.sql"))
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
        // ShuffleWriterExec: Some(Hash([Column { name: "seller", index: 1 }], 16))
        //   CoalesceBatchesExec: target_batch_size=4096
        //     FilterExec: CAST(category@2 AS Int64) = 10
        //       MemoryExec: partitions=1, partition_sizes=[1]


        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "p_id", index: 0 }], 16))
        //   CoalesceBatchesExec: target_batch_size=4096
        //     FilterExec: state@3 = or OR state@3 = id OR state@3 = ca
        //       MemoryExec: partitions=1, partition_sizes=[1]


        // === Physical subplan ===
        // ShuffleWriterExec: None
        //   ProjectionExec: expr=[name@4 as name, city@5 as city, state@6 as state, a_id@0 as a_id]
        //     CoalesceBatchesExec: target_batch_size=4096
        //       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "seller", index: 1 }, Column { name: "p_id", index: 0 })]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           MemoryExec: partitions=0, partition_sizes=[]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(3, stages.len());

        // verify stage 0
        let stage0 = stages[0].children()[0].clone();
        let coalesce = downcast_exec!(stage0, CoalesceBatchesExec);
        let filter = coalesce.children()[0].clone();
        let filter = downcast_exec!(filter, FilterExec);
        let input = filter.children()[0].clone();
        let input = downcast_exec!(input, MemoryExec);
        println!("Stage 0:\nInput {:#?}\n", input.schema());

        // verify stage 1
        let stage1 = stages[1].children()[0].clone();
        let coalesce = downcast_exec!(stage1, CoalesceBatchesExec);
        let filter = coalesce.children()[0].clone();
        let filter = downcast_exec!(filter, FilterExec);
        let input = filter.children()[0].clone();
        let input = downcast_exec!(input, MemoryExec);
        println!("Stage 1:\nInput {:#?}\n", input.schema());

        // verify stage 2
        let stage2 = stages[2].children()[0].clone();
        let projection = downcast_exec!(stage2, ProjectionExec);
        let coalesce = projection.children()[0].clone();
        let coalesce = downcast_exec!(coalesce, CoalesceBatchesExec);
        let join = coalesce.children()[0].clone();
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

        Ok(())
    }

    #[tokio::test]
    async fn nexmark_q4_distributed_plan() -> Result<()> {
        let mut ctx = register_nexmark_tables().await?;
        let df = ctx
            .sql(include_str!("../../../benchmarks/src/nexmark/query/q4.sql"))
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
        // ShuffleWriterExec: Some(Hash([Column { name: "a_id", index: 0 }], 16))
        //   MemoryExec: partitions=1, partition_sizes=[1]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "auction", index: 0 }], 16))
        //   MemoryExec: partitions=1, partition_sizes=[1]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "a_id", index: 0 }, Column { name: "category", index: 1 }], 16))
        //   HashAggregateExec: mode=Partial, gby=[a_id@0 as a_id, category@3 as category], aggr=[MAX(bid.price)]
        //     CoalesceBatchesExec: target_batch_size=4096
        //       FilterExec: b_date_time@6 >= a_date_time@1 AND b_date_time@6 <= expires@2
        //         CoalesceBatchesExec: target_batch_size=4096
        //           HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "a_id", index: 0 }, Column { name: "auction", index: 0 })]
        //             CoalesceBatchesExec: target_batch_size=4096
        //               MemoryExec: partitions=0, partition_sizes=[]
        //             CoalesceBatchesExec: target_batch_size=4096
        //               MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "category", index: 0 }], 16))
        //   HashAggregateExec: mode=Partial, gby=[category@1 as category], aggr=[AVG(Q.final)]
        //     ProjectionExec: expr=[final@0 as final, category@1 as category]
        //       ProjectionExec: expr=[MAX(bid.price)@2 as final, category@1 as category]
        //         HashAggregateExec: mode=FinalPartitioned, gby=[a_id@0 as a_id, category@1 as category], aggr=[MAX(bid.price)]
        //           CoalesceBatchesExec: target_batch_size=4096
        //             MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: None
        //   ProjectionExec: expr=[category@0 as category, AVG(Q.final)@1 as AVG(Q.final)]
        //     HashAggregateExec: mode=FinalPartitioned, gby=[category@0 as category], aggr=[AVG(Q.final)]
        //       CoalesceBatchesExec: target_batch_size=4096
        //         MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(5, stages.len());

        Ok(())
    }

    #[tokio::test]
    async fn show_nexmark_distributed_plans() -> Result<()> {
        let mut ctx = register_nexmark_tables().await?;
        let sqls = vec![
            include_str!("../../../benchmarks/src/nexmark/query/q5.sql"),
            include_str!("../../../benchmarks/src/nexmark/query/q6.sql"),
            include_str!("../../../benchmarks/src/nexmark/query/q7.sql"),
            include_str!("../../../benchmarks/src/nexmark/query/q8.sql"),
            include_str!("../../../benchmarks/src/nexmark/query/q9.sql"),
            include_str!("../../../benchmarks/src/nexmark/query/q10.sql"),
            include_str!("../../../benchmarks/src/nexmark/query/q11.sql"),
            include_str!("../../../benchmarks/src/nexmark/query/q13.sql"),
        ];

        for sql in sqls {
            let df = ctx.sql(sql).await?;

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
        }

        Ok(())
    }

    #[tokio::test]
    async fn ysb_distributed_plan() -> Result<()> {
        let mut ctx = register_ysb_tables().await?;
        let df = ctx
            .sql(include_str!("../../../benchmarks/src/ysb/ysb.sql"))
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
        // ShuffleWriterExec: Some(Hash([Column { name: "ad_id", index: 0 }], 16))
        //   CoalesceBatchesExec: target_batch_size=4096
        //     FilterExec: event_type@1 = view
        //       MemoryExec: partitions=1, partition_sizes=[1]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "c_ad_id", index: 0 }], 16))
        //   MemoryExec: partitions=1, partition_sizes=[1]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: Some(Hash([Column { name: "campaign_id", index: 0 }], 16))
        //   HashAggregateExec: mode=Partial, gby=[campaign_id@3 as campaign_id], aggr=[COUNT(UInt8(1))]
        //     CoalesceBatchesExec: target_batch_size=4096
        //       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "ad_id", index: 0 }, Column { name: "c_ad_id", index: 0 })]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           MemoryExec: partitions=0, partition_sizes=[]
        //         CoalesceBatchesExec: target_batch_size=4096
        //           MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Physical subplan ===
        // ShuffleWriterExec: None
        //   ProjectionExec: expr=[campaign_id@0 as campaign_id, COUNT(UInt8(1))@1 as COUNT(UInt8(1))]
        //     HashAggregateExec: mode=FinalPartitioned, gby=[campaign_id@0 as campaign_id], aggr=[COUNT(UInt8(1))]
        //       CoalesceBatchesExec: target_batch_size=4096
        //         MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(4, stages.len());

        // verify stage 0
        let stage0 = stages[0].children()[0].clone();
        let coalesce = downcast_exec!(stage0, CoalesceBatchesExec);
        let filter = coalesce.children()[0].clone();
        let filter = downcast_exec!(filter, FilterExec);
        let input = filter.children()[0].clone();
        let input = downcast_exec!(input, MemoryExec);
        println!("Stage 0:\nInput {:#?}\n", input.schema());

        // verify stage 1
        let stage1 = stages[1].children()[0].clone();
        let input = downcast_exec!(stage1, MemoryExec);
        println!("Stage 1:\nInput {:#?}\n", input.schema());

        // verify stage 2
        let stage2 = stages[2].children()[0].clone();
        let partial_hash = downcast_exec!(stage2, HashAggregateExec);
        assert!(*partial_hash.mode() == AggregateMode::Partial);
        let coalesce = partial_hash.children()[0].clone();
        let coalesce = downcast_exec!(coalesce, CoalesceBatchesExec);
        let join = coalesce.children()[0].clone();
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

        // verify stage 3
        let stage3 = stages[3].children()[0].clone();
        let projection = downcast_exec!(stage3, ProjectionExec);
        let final_hash = projection.children()[0].clone();
        let final_hash = downcast_exec!(final_hash, HashAggregateExec);
        assert!(*final_hash.mode() == AggregateMode::FinalPartitioned);
        let coalesce = final_hash.children()[0].clone();
        let coalesce = downcast_exec!(coalesce, CoalesceBatchesExec);
        let input = coalesce.children()[0].clone();
        let input = downcast_exec!(input, MemoryExec);
        println!("Stage 3:\nInput {:#?}\n", input.schema());

        Ok(())
    }
}
