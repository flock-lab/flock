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
//! optimizer analyzes the query and generate the physical plan, which is
//! compressed and serialized to the environment context of cloud functios. The
//! execution strategy dynamically adjusts central and distributed execution
//! modes at runtime to achieve the optimal performance and cost, that is,
//! adaptive query optimization. In central mode, Squirtle executes the query
//! plan immediately using a single cloud function. In contrast, in distributed
//! mode, the first cloud function acts as the query coordinator. It schedules
//! work on other cloud functions that then together execute the query in a
//! distributed dataflow model.

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::StreamExt;
use runtime::prelude::*;
use std::sync::Arc;

/// The execution strategy of the first cloud function.
///
/// Small queries begin executing on the immediate cloud function that
/// receives the requests; Larger queries beigin executing on dynamically
/// provisioning cloud funtions in asynchonrous fashion.
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

const FIVE_MB: usize = 5 * 1024 * 1024;
const TEN_MB: usize = 10 * 1024 * 1024;
const TWENTY_MB: usize = 20 * 1024 * 1024;

impl Executor {
    /// Choose an optimal strategy according to the size of the batch and the
    /// attributes of the query.
    pub fn choose_strategy(ctx: &ExecutionContext, batch: &RecordBatch) -> ExecutionStrategy {
        let size: usize = batch
            .columns()
            .iter()
            .map(|a| a.get_array_memory_size())
            .sum();
        if contain_join(&ctx.plan) {
            if size < FIVE_MB {
                ExecutionStrategy::Centralized
            } else {
                ExecutionStrategy::Distributed
            }
        } else if contain_aggregate(&ctx.plan) {
            if size < TEN_MB {
                ExecutionStrategy::Centralized
            } else {
                ExecutionStrategy::Distributed
            }
        } else if size < TWENTY_MB {
            ExecutionStrategy::Centralized
        } else {
            ExecutionStrategy::Distributed
        }
    }

    /// Combines small batches into larger batches for more efficient use of
    /// vectorized processing by upstream operators
    pub async fn coalesce_batches(
        input_partitions: Vec<Vec<RecordBatch>>,
        target_batch_size: usize,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        // create physical plan
        let exec = MemoryExec::try_new(&input_partitions, input_partitions[0][0].schema(), None)?;
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(CoalesceBatchesExec::new(Arc::new(exec), target_batch_size));

        // execute and collect results
        let output_partition_count = exec.output_partitioning().partition_count();
        let mut output_partitions = Vec::with_capacity(output_partition_count);
        for i in 0..output_partition_count {
            // execute this *output* partition and collect all batches
            let mut stream = exec.execute(i).await?;
            let mut batches = vec![];
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            output_partitions.push(batches);
        }
        Ok(output_partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_batches() -> Result<()> {
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 10)?;
        let partitions = vec![partition];

        let output_partitions = Executor::coalesce_batches(partitions, 20).await?;
        assert_eq!(1, output_partitions.len());

        // input is 10 batches x 8 rows (80 rows)
        // expected output is batches of at least 20 rows (except for the final batch)
        let batches = &output_partitions[0];
        assert_eq!(4, batches.len());
        assert_eq!(24, batches[0].num_rows());
        assert_eq!(24, batches[1].num_rows());
        assert_eq!(24, batches[2].num_rows());
        assert_eq!(8, batches[3].num_rows());

        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn create_vec_batches(schema: &Arc<Schema>, num_batches: usize) -> Result<Vec<RecordBatch>> {
        let batch = create_batch(schema);
        let mut vec = Vec::with_capacity(num_batches);
        for _ in 0..num_batches {
            vec.push(batch.clone());
        }
        Ok(vec)
    }

    fn create_batch(schema: &Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }
}
