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

//! The execution strategy dynamically adjusts the execution plan of the cloud
//! function at runtime to achieve the optimal performance and cost.
//! Furthermore, it achieves adaptive query optimization. For example, most
//! queries do not require distributed dataflow processing. Squirtle can
//! intelligently make optimization strategies at runtime and choose to complete
//! the entire query processing in a single cloud function.

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// The execution strategy of the first cloud function.
pub enum ExecutionStrategy {
    /// The current query is executed by a single cloud function.
    MonolithExec,
    /// The current query is split into multiple cloud functions (DAG) for
    /// execution.
    MacroExec,
    /// Unknown error.
    UnknownExec,
}

/// The query executor in cloud function.
pub struct Executor;

impl Executor {
    /// Choose an optimal strategy according to the size of the batch and the
    /// attributes of the query.
    pub fn choose_strategy(plan: Arc<dyn ExecutionPlan>, batch: &RecordBatch) -> ExecutionStrategy {
        ExecutionStrategy::UnknownExec
    }
}
