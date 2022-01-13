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

use crate::distributed_plan::stage::{self, QueryDag};
use crate::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;
use std::sync::Arc;

/// Distributed Planer deals with the physical plan and convert it into
/// distributed plan.
#[derive(Debug)]
pub struct DistributedPlanner {}

impl DistributedPlanner {
    /// Create a new distributed planner.
    pub fn new() -> Self {
        DistributedPlanner {}
    }
}

impl Default for DistributedPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl DistributedPlanner {
    /// Returns a new distributed plan (DAG) with the given query plan.
    #[allow(dead_code)]
    pub async fn plan_query_stages(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<QueryDag> {
        debug!("planning query stages\n");
        Ok(self.plan_query_stages_internal(execution_plan)?)
    }

    /// Returns a potentially DAG of the execution plan.
    fn plan_query_stages_internal(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<QueryDag> {
        stage::build_query_dag(execution_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::nexmark::*;
    use crate::datasource::ysb::*;
    use datafusion::physical_plan::displayable;

    #[tokio::test]
    async fn nexmark_q1_distributed_plan() -> Result<()> {
        let mut ctx = register_nexmark_tables().await?;
        let df = ctx
            .sql(include_str!("../../../benchmarks/src/nexmark/query/q1.sql"))
            .await?;

        let plan = df.to_logical_plan();
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan).await?;

        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan).await?;
        let stages = &dag.get_all_stages();
        for (i, stage) in stages.iter().enumerate() {
            println!("=== Stage {} ===\n{}\n", i, stage.get_plan_str());
        }

        #[rustfmt::skip]
        // Expected result:
        //
        // === Stage 0 ===
        // ProjectionExec: expr=[auction@0 as auction, bidder@1 as bidder, 0.908 * CAST(price@2 AS Float64) as price, b_date_time@3 as b_date_time]
        //   RepartitionExec: partitioning=RoundRobinBatch(16)
        //     MemoryExec: partitions=0, partition_sizes=[]
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

        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan).await?;
        let stages = &dag.get_all_stages();
        for (i, stage) in stages.iter().enumerate() {
            println!("=== Stage {} ===\n{}\n", i, stage.get_plan_str());
        }

        #[rustfmt::skip]
        // Expected result:
        //
        // === Stage 0 ===
        // ProjectionExec: expr=[auction@0 as auction, price@1 as price]
        //   CoalesceBatchesExec: target_batch_size=4096
        //     FilterExec: CAST(auction@0 AS Int64) % 123 = 0
        //       RepartitionExec: partitioning=RoundRobinBatch(16)
        //         MemoryExec: partitions=0, partition_sizes=[]
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

        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan).await?;
        let stages = &dag.get_all_stages();
        for (i, stage) in stages.iter().enumerate() {
            println!("=== Stage {} ===\n{}\n", i, stage.get_plan_str());
        }

        #[rustfmt::skip]
        // Expected result:
        //
        // === Stage 0 ===
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "seller", index: 1 }], 16)
        //     CoalesceBatchesExec: target_batch_size=4096
        //       FilterExec: CAST(category@2 AS Int64) = 10
        //         RepartitionExec: partitioning=RoundRobinBatch(16)
        //           MemoryExec: partitions=0, partition_sizes=[]

        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "p_id", index: 0 }], 16)
        //     CoalesceBatchesExec: target_batch_size=4096
        //       FilterExec: state@3 = or OR state@3 = id OR state@3 = ca
        //         RepartitionExec: partitioning=RoundRobinBatch(16)
        //           MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Stage 1 ===
        // ProjectionExec: expr=[name@4 as name, city@5 as city, state@6 as state, a_id@0 as a_id]
        //   CoalesceBatchesExec: target_batch_size=4096
        //     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "seller", index: 1 }, Column { name: "p_id", index: 0 })]
        //       MemoryExec: partitions=0, partition_sizes=[]
        //       MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(2, stages.len());

        // verify stage 0
        let plan = stages[0].get_plan_str();
        assert_eq!(
            plan.matches(r#"RepartitionExec: partitioning=Hash"#)
                .count(),
            2
        );
        assert_eq!(
            plan.matches(r#"RepartitionExec: partitioning=RoundRobinBatch"#)
                .count(),
            2
        );
        assert_eq!(plan.matches(r#"CoalesceBatchesExec"#).count(), 4);
        assert_eq!(plan.matches(r#"FilterExec"#).count(), 2);
        assert_eq!(plan.matches(r#"MemoryExec"#).count(), 2);

        // verify stage 1
        let plan = stages[1].get_plan_str();
        assert_eq!(plan.matches(r#"ProjectionExec"#).count(), 1);
        assert_eq!(plan.matches(r#"CoalesceBatchesExec"#).count(), 1);
        assert_eq!(plan.matches(r#"ProjectionExec"#).count(), 1);
        assert_eq!(plan.matches(r#"MemoryExec"#).count(), 2);

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

        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan).await?;
        let stages = &dag.get_all_stages();
        for (i, stage) in stages.iter().enumerate() {
            println!("=== Stage {} ===\n{}\n", i, stage.get_plan_str());
        }

        #[rustfmt::skip]
        // Expected result:
        //
        // === Stage 0 ===
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "a_id", index: 0 }], 16)
        //     RepartitionExec: partitioning=RoundRobinBatch(16)
        //       MemoryExec: partitions=0, partition_sizes=[]
        //
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "auction", index: 0 }], 16)
        //     RepartitionExec: partitioning=RoundRobinBatch(16)
        //       MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Stage 1 ===
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "a_id", index: 0 }, Column { name: "category", index: 1 }], 16)
        //     HashAggregateExec: mode=Partial, gby=[a_id@0 as a_id, category@3 as category], aggr=[MAX(bid.price)]
        //       CoalesceBatchesExec: target_batch_size=4096
        //         FilterExec: b_date_time@6 >= a_date_time@1 AND b_date_time@6 <= expires@2
        //           CoalesceBatchesExec: target_batch_size=4096
        //             HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "a_id", index: 0 }, Column { name: "auction", index: 0 })]
        //               MemoryExec: partitions=0, partition_sizes=[]
        //               MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Stage 2 ===
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "category", index: 0 }], 16)
        //     HashAggregateExec: mode=Partial, gby=[category@1 as category], aggr=[AVG(Q.final)]
        //       ProjectionExec: expr=[final@0 as final, category@1 as category]
        //         ProjectionExec: expr=[MAX(bid.price)@2 as final, category@1 as category]
        //           HashAggregateExec: mode=FinalPartitioned, gby=[a_id@0 as a_id, category@1 as category], aggr=[MAX(bid.price)]
        //             MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Stage 3 ===
        // ProjectionExec: expr=[category@0 as category, AVG(Q.final)@1 as AVG(Q.final)]
        //   HashAggregateExec: mode=FinalPartitioned, gby=[category@0 as category], aggr=[AVG(Q.final)]
        //     MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(4, stages.len());

        Ok(())
    }

    #[tokio::test]
    async fn show_nexmark_distributed_plans() -> Result<()> {
        let mut ctx = register_nexmark_tables().await?;
        let sqls = vec![
            include_str!("../../../benchmarks/src/nexmark/query/q3.sql"),
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
            println!(
                "=== Physical Plan ===\n{}\n",
                displayable(plan.as_ref()).indent()
            );

            let planner = DistributedPlanner::new();
            let dag = planner.plan_query_stages(plan).await?;
            let stages = &dag.get_all_stages();
            for (i, stage) in stages.iter().enumerate() {
                println!("=== Stage {} ===\n{}\n", i, stage.get_plan_str());
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

        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan).await?;
        let stages = &dag.get_all_stages();
        for (i, stage) in stages.iter().enumerate() {
            println!("=== Stage {} ===\n{}\n", i, stage.get_plan_str());
        }

        #[rustfmt::skip]
        // Expected result:
        // === Stage 0 ===
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "ad_id", index: 0 }], 16)
        //     CoalesceBatchesExec: target_batch_size=4096
        //       FilterExec: event_type@1 = view
        //         RepartitionExec: partitioning=RoundRobinBatch(16)
        //           MemoryExec: partitions=0, partition_sizes=[]
        //
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "c_ad_id", index: 0 }], 16)
        //     RepartitionExec: partitioning=RoundRobinBatch(16)
        //       MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Stage 1 ===
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "campaign_id", index: 0 }], 16)
        //     HashAggregateExec: mode=Partial, gby=[campaign_id@3 as campaign_id], aggr=[COUNT(UInt8(1))]
        //       CoalesceBatchesExec: target_batch_size=4096
        //         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "ad_id", index: 0 }, Column { name: "c_ad_id", index: 0 })]
        //           MemoryExec: partitions=0, partition_sizes=[]
        //           MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Stage 2 ===
        // ProjectionExec: expr=[campaign_id@0 as campaign_id, COUNT(UInt8(1))@1 as COUNT(UInt8(1))]
        //   HashAggregateExec: mode=FinalPartitioned, gby=[campaign_id@0 as campaign_id], aggr=[COUNT(UInt8(1))]
        //     MemoryExec: partitions=0, partition_sizes=[]
        assert_eq!(3, stages.len());

        Ok(())
    }
}
