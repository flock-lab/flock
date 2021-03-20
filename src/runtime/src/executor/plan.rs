// Copyright 2020 UMD Database Group. All Rights Reserved.
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

//! The helper crate for cloud function to extract information from a physical
//! plan.
//!
//! A generic execution plan which includes:
//! - `CoalesceBatchesExec`: Combines small batches into larger batches for more
//!   efficient use of vectorized processing by upstream operators.
//! - `CsvExec`: Execution plan for scanning a CSV file.
//! - `DummyExec`: Dummy execution plan.
//! - `EmptyExec`: Execution plan for empty relation (produces no rows).
//! - `ExplainExec`: Explain execution plan operator. This operator contains the
//!   string values of the various plans it has when it is created, and passes
//!   them to its output.
//! - `FilterExec`: Evaluates a boolean predicate against all input batches to
//!   determine which rows to include in its output batches.
//! - `HashAggregateExec`: Hash aggregate execution plan.
//! - `HashJoinExec`: Join execution plan executes partitions in parallel and
//!   combines them into a set of partitions.
//! - `GlobalLimitExec`: Limit execution plan.
//! - `LocalLimitExec`: Applies a limit to a single partition.
//! - `MemoryExec`: Execution plan for reading in-memory batches of data.
//! - `Merge`: Execution plan executes partitions in parallel and combines them
//!   into a single partition. No guarantees are made about the order of the
//!   resulting partition.
//! - `ParquetExec`: Execution plan for scanning one or more `Parquet`
//!   partitions.
//! - `ProjectionExec`: Execution plan for a projection.
//! - `RepartitionExec`: The repartition operator maps N input partitions to M
//!   output partitions based on a partitioning scheme. No guarantees are made
//!   about the order of the resulting partitions.
//! - `Sort`: The sort execution plan.

use crate::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

macro_rules! query_has_op_function {
    ($OPERATOR:ident, $FUNC:ident) => {
        /// Returns true if the current execution plan contains a given operator.
        pub fn $FUNC(plan: &Arc<dyn ExecutionPlan>) -> bool {
            let mut curr = plan.clone();
            loop {
                if curr.as_any().downcast_ref::<$OPERATOR>().is_some() {
                    return true;
                }
                if curr.children().is_empty() {
                    break;
                }
                curr = curr.children()[0].clone();
            }
            false
        }
    };
}

query_has_op_function!(SortExec, contain_sort);
query_has_op_function!(HashJoinExec, contain_join);
query_has_op_function!(HashAggregateExec, contain_aggregate);

/// Planning phase and return the execution plan.
pub fn physical_plan(ctx: &mut ExecutionContext, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
    let logical_plan = ctx.create_logical_plan(sql)?;
    let logical_plan = ctx.optimize(&logical_plan)?;
    Ok(ctx.create_physical_plan(&logical_plan)?)
}
