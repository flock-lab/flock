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

//! The helper of lambda function to initialize the physical plan from the cloud
//! environment.

use datafusion::physical_plan::ExecutionPlan;

/// Environment key for the JSON-formatted physical sub-plan.
pub const PLAN_JSON: &str = "PLAN_JSON";

/// The lazy-loaded singleton of lambda function supports dynamic dispatch
/// through `LambdaPlan`.
pub enum LambdaPlan {
    /// A generic execution plan which includes:
    /// - `CoalesceBatchesExec`: Combines small batches into larger batches for
    ///   more efficient use of vectorized processing by upstream operators.
    /// - `CsvExec`: Execution plan for scanning a CSV file.
    /// - `DummyExec`: Dummy execution plan.
    /// - `EmptyExec`: Execution plan for empty relation (produces no rows).
    /// - `ExplainExec`: Explain execution plan operator. This operator contains
    ///   the string values of the various plans it has when it is created, and
    ///   passes them to its output.
    /// - `FilterExec`: Evaluates a boolean predicate against all input batches
    ///   to determine which rows to include in its output batches.
    /// - `HashAggregateExec`: Hash aggregate execution plan.
    /// - `HashJoinExec`: Join execution plan executes partitions in parallel
    ///   and combines them into a set of partitions.
    /// - `GlobalLimitExec`: Limit execution plan.
    /// - `LocalLimitExec`: Applies a limit to a single partition.
    /// - `MemoryExec`: Execution plan for reading in-memory batches of data.
    /// - `Merge`: Execution plan executes partitions in parallel and combines
    ///   them into a single partition. No guarantees are made about the order
    ///   of the resulting partition.
    /// - `ParquetExec`: Execution plan for scanning one or more `Parquet`
    ///   partitions.
    /// - `ProjectionExec`: Execution plan for a projection.
    /// - `RepartitionExec`: The repartition operator maps N input partitions to
    ///   M output partitions based on a partitioning scheme. No guarantees are
    ///   made about the order of the resulting partitions.
    /// - `Sort`: The sort execution plan.
    OpsExec(Box<dyn ExecutionPlan>),
    /// No execution plan for initialization.
    None,
}

/// Performs an initialization routine once and only once.
#[macro_export]
macro_rules! init_plan {
    ($init:ident, $plan:ident) => {{
        unsafe {
            $init.call_once(|| match std::env::var(PLAN_JSON) {
                Ok(json) => {
                    $plan = LambdaPlan::OpsExec(
                        serde_json::from_str::<Box<dyn ExecutionPlan>>(&json).unwrap(),
                    );
                }
                Err(e) => panic!("Didn't configure the environment {}, {}", PLAN_JSON, e),
            });
            match &mut $plan {
                LambdaPlan::OpsExec(plan) => (plan.schema(), plan),
                LambdaPlan::None => panic!("Unexpected plan!"),
                _ => unimplemented!(),
            }
        }
    }};
}

/// Executes the physical plan.
#[macro_export]
macro_rules! exec_plan {
    ($plan:ident, $batch:expr) => {{
        $plan.feed_batches($batch);
        let it = $plan.execute(0).await?;
        common::collect(it).await?
    }};
}
