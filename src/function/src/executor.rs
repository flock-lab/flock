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

//! By default, Squirtle supports two types of interactive execution modes:
//! central and distributed. During the planning phase in the client-side, the
//! optimizer analyzes the query and generate both central and distributed
//! modes. In central mode, Squirtle executes the query plan immediately using a
//! single cloud function. In contrast, in distributed mode, the first cloud
//! function acts as the query coordinator. It schedules work on other cloud
//! functions that then together execute the query in a distributed dataflow
//! model. The execution strategy dynamically adjusts execution modes at runtime
//! to achieve the optimal performance and cost, which achieves
//! adaptive query optimization.
//!
//! For example, small queries begin executing on the immediate cloud function
//! that receives the requests. Squirtle schedules larger queries for
//! distributed execution by dynamically provisioning execution funtions in
//! asynchonrous dataflow fashion.

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// The execution strategy of the first cloud function.
pub enum ExecutionStrategy {
    /// In centralized execution, the system analyzes, plans, and executes the
    /// query immediately at the first cloud function that receives it.
    Centralized,
    /// In distributed mode, the first cloud function to receive the query acts
    /// only as the query coordinator. That function schedules work on separate
    /// functions (DAG) that then together execute the query.
    Distributed,
    /// Unknown error.
    UnknownExec,
}

/// The query executor in cloud function.
pub struct Executor;

impl Executor {
    /// Choose an optimal strategy according to the size of the batch and the
    /// attributes of the query.
    pub fn choose_strategy(
        _plan: Arc<dyn ExecutionPlan>,
        batch: &RecordBatch,
    ) -> ExecutionStrategy {
        let _: usize = batch
            .columns()
            .iter()
            .map(|a| a.get_array_memory_size())
            .sum();
        ExecutionStrategy::UnknownExec
    }
}
