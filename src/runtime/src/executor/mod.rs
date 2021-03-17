// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
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
//! compressed and serialized to the environment context of cloud functions. The
//! execution strategy dynamically adjusts central and distributed execution
//! modes at runtime to achieve the optimal performance and cost, that is,
//! adaptive query optimization. In central mode, Squirtle executes the query
//! plan immediately using a single cloud function. In contrast, in distributed
//! mode, the first cloud function acts as the query coordinator. It schedules
//! work on other cloud functions that then together execute the query in a
//! distributed dataflow model.

use crate::config::GLOBALS as globals;
use crate::context::CloudFunction;
use crate::context::ExecutionContext;
use crate::error::{Result, SquirtleError};
use crate::payload::{Payload, Uuid, UuidBuilder};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use futures::executor::block_on;
use futures::stream::StreamExt;
use plan::*;
use rand::Rng;
use rayon::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::InvokeAsyncRequest;
use rusoto_lambda::{Lambda, LambdaClient};
use serde_json::Value;
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
}

/// The query executor on cloud function.
#[async_trait]
pub trait Executor {
    /// Combines small batches into larger batches for more efficient use of
    /// vectorized processing by upstream operators
    async fn coalesce_batches(
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

    /// Maps N input partitions to M output partitions based on a
    /// partitioning scheme. No guarantees are made about the order of the
    /// resulting partitions.
    async fn repartition(
        input_partitions: Vec<Vec<RecordBatch>>,
        partitioning: Partitioning,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        // create physical plan
        let exec = MemoryExec::try_new(&input_partitions, input_partitions[0][0].schema(), None)?;
        let exec = RepartitionExec::try_new(Arc::new(exec), partitioning)?;

        // execute and collect results
        let mut output_partitions = vec![];
        for i in 0..exec.partitioning().partition_count() {
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

    /// Event sink or data sink is a function designed to send the events from
    /// the function to the customers.
    async fn event_sink(batches: Vec<Vec<RecordBatch>>) -> Result<Value> {
        // coalesce batches to one and only one batch
        let batches = Self::repartition(batches, Partitioning::RoundRobinBatch(1)).await?;
        assert_eq!(1, batches.len());

        let batch_size = batches[0].par_iter().map(|r| r.num_rows()).sum();
        let output_partitions = LambdaExecutor::coalesce_batches(batches, batch_size).await?;
        assert_eq!(1, output_partitions.len());
        assert_eq!(1, output_partitions[0].len());

        Ok(Payload::to_value(&output_partitions[0], Uuid::default()))
    }
}

/// The query executor on AWS Lambda Function.
pub struct LambdaExecutor;

#[async_trait]
impl Executor for LambdaExecutor {}

impl LambdaExecutor {
    /// Choose an optimal strategy according to the size of the batch and the
    /// attributes of the query.
    pub fn choose_strategy(ctx: &ExecutionContext, batch: &[RecordBatch]) -> ExecutionStrategy {
        let size: usize = batch
            .par_iter()
            .map(|r| {
                r.columns()
                    .par_iter()
                    .map(|a| a.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum();
        if contain_join(&ctx.plan) {
            if size
                < globals["lambda"]["join_threshold"]
                    .parse::<usize>()
                    .unwrap()
            {
                ExecutionStrategy::Centralized
            } else {
                ExecutionStrategy::Distributed
            }
        } else if contain_aggregate(&ctx.plan) {
            if size
                < globals["lambda"]["aggregate_threshold"]
                    .parse::<usize>()
                    .unwrap()
            {
                ExecutionStrategy::Centralized
            } else {
                ExecutionStrategy::Distributed
            }
        } else if size
            < globals["lambda"]["regular_threshold"]
                .parse::<usize>()
                .unwrap()
        {
            ExecutionStrategy::Centralized
        } else {
            ExecutionStrategy::Distributed
        }
    }

    /// Returns the next cloud function names for invocation.
    fn next_function(ctx: &ExecutionContext) -> Result<String> {
        let mut lambdas = match &ctx.next {
            CloudFunction::None => vec![],
            CloudFunction::Chorus((name, num)) => {
                (0..*num).map(|i| format!("{}-{}", name, i)).collect()
            }
            CloudFunction::Solo(name) => vec![name.to_owned()],
        };

        if lambdas.is_empty() {
            return Err(SquirtleError::Internal(
                "No distributed execution plan".to_owned(),
            ));
        }

        let mut function_name = lambdas[0].clone();
        if lambdas.len() > 1 {
            // mapping to the same lambda function name through hashing technology.
            let mut rng = rand::thread_rng();
            function_name = lambdas.remove(rng.gen_range(0..lambdas.len()));
        }

        Ok(function_name)
    }

    /// Invoke functions in the next stage of the data flow.
    pub fn invoke_async_functions(
        ctx: &ExecutionContext,
        batches: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        // retrieve the next lambda function names
        let next_func = LambdaExecutor::next_function(&ctx)?;

        // create uuid builder to assign id to each payload
        let mut uuid_builder = UuidBuilder::new(&ctx.name, batches.len());

        let client = &LambdaClient::new(Region::default());
        let nums = batches.len();
        (0..nums).for_each(|_| {
            let uuid = uuid_builder.next();
            // call the lambda function asynchronously until it succeeds.
            loop {
                let request = InvokeAsyncRequest {
                    function_name: next_func.clone(),
                    invoke_args:   Payload::to_bytes(&[batches.pop().unwrap()], uuid.clone()),
                };

                if let Ok(reponse) = block_on(client.invoke_async(request)) {
                    if let Some(code) = reponse.status {
                        // A success response (202 Accepted) indicates that the request
                        // is queued for invocation.
                        if code == 202 {
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SquirtleError;
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::expressions::Column;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn test_concat_batches() -> Result<()> {
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 10);
        let partitions = vec![partition];

        let output_partitions = LambdaExecutor::coalesce_batches(partitions, 20).await?;
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

    #[tokio::test]
    async fn one_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition];

        // repartition from 1 input to 4 output
        let output_partitions =
            LambdaExecutor::repartition(partitions, Partitioning::RoundRobinBatch(4)).await?;

        assert_eq!(4, output_partitions.len());
        assert_eq!(13, output_partitions[0].len());
        assert_eq!(13, output_partitions[1].len());
        assert_eq!(12, output_partitions[2].len());
        assert_eq!(12, output_partitions[3].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_one_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 1 output
        let output_partitions =
            LambdaExecutor::repartition(partitions, Partitioning::RoundRobinBatch(1)).await?;

        assert_eq!(1, output_partitions.len());
        assert_eq!(150, output_partitions[0].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 5 output
        let output_partitions =
            LambdaExecutor::repartition(partitions, Partitioning::RoundRobinBatch(5)).await?;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_hash_partition() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(&schema, 50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        let output_partitions = LambdaExecutor::repartition(
            partitions,
            Partitioning::Hash(vec![Arc::new(Column::new(&"c0"))], 8),
        )
        .await?;

        let total_rows: usize = output_partitions.iter().map(|x| x.len()).sum();

        assert_eq!(8, output_partitions.len());
        assert_eq!(total_rows, 8 * 50 * 3);

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_round_robin_within_tokio_task() -> Result<()> {
        let join_handle: JoinHandle<Result<Vec<Vec<RecordBatch>>>> = tokio::spawn(async move {
            // define input partitions
            let schema = test_schema();
            let partition = create_vec_batches(&schema, 50);
            let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

            // repartition from 3 input to 5 output
            LambdaExecutor::repartition(partitions, Partitioning::RoundRobinBatch(5)).await
        });

        let output_partitions = join_handle
            .await
            .map_err(|e| SquirtleError::Internal(e.to_string()))??;

        assert_eq!(5, output_partitions.len());
        assert_eq!(30, output_partitions[0].len());
        assert_eq!(30, output_partitions[1].len());
        assert_eq!(30, output_partitions[2].len());
        assert_eq!(30, output_partitions[3].len());
        assert_eq!(30, output_partitions[4].len());

        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn create_vec_batches(schema: &Arc<Schema>, num_batches: usize) -> Vec<RecordBatch> {
        let batch = create_batch(schema);
        let mut vec = Vec::with_capacity(num_batches);
        for _ in 0..num_batches {
            vec.push(batch.clone());
        }
        vec
    }

    fn create_batch(schema: &Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }
}

pub mod plan;
