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

//! By default, Flock supports two types of interactive execution modes:
//! central and distributed. During the planning phase in the client-side, the
//! optimizer analyzes the query and generate the physical plan, which is
//! compressed and serialized to the environment context of cloud functions. The
//! execution strategy dynamically adjusts central and distributed execution
//! modes at runtime to achieve the optimal performance and cost, that is,
//! adaptive query optimization. In central mode, Flock executes the query
//! plan immediately using a single cloud function. In contrast, in distributed
//! mode, the first cloud function acts as the query coordinator. It schedules
//! work on other cloud functions that then together execute the query in a
//! distributed dataflow model.

use crate::config::FLOCK_CONF;
use crate::encoding::Encoding;
use crate::error::{FlockError, Result};
use crate::runtime::context::CloudFunction;
use crate::runtime::context::ExecutionContext;
use crate::runtime::payload::Uuid;
use crate::runtime::transform::*;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::Partitioning;
use plan::*;
use rand::Rng;
use rayon::prelude::*;
use serde_json::Value;

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
    /// Event sink or data sink is a function designed to send the events from
    /// the function to the customers.
    async fn event_sink(batches: Vec<Vec<RecordBatch>>) -> Result<Value> {
        // coalesce batches to one and only one batch
        let batches = repartition(batches, Partitioning::RoundRobinBatch(1)).await?;
        assert_eq!(1, batches.len());

        let batch_size = batches[0].par_iter().map(|r| r.num_rows()).sum();
        let output_partitions = coalesce_batches(batches, batch_size).await?;
        assert_eq!(1, output_partitions.len());
        assert_eq!(1, output_partitions[0].len());

        Ok(to_value(
            &output_partitions[0],
            Uuid::default(),
            Encoding::default(),
        ))
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
                < FLOCK_CONF["lambda"]["join_threshold"]
                    .parse::<usize>()
                    .unwrap()
            {
                ExecutionStrategy::Centralized
            } else {
                ExecutionStrategy::Distributed
            }
        } else if contain_aggregate(&ctx.plan) {
            if size
                < FLOCK_CONF["lambda"]["aggregate_threshold"]
                    .parse::<usize>()
                    .unwrap()
            {
                ExecutionStrategy::Centralized
            } else {
                ExecutionStrategy::Distributed
            }
        } else if size
            < FLOCK_CONF["lambda"]["regular_threshold"]
                .parse::<usize>()
                .unwrap()
        {
            ExecutionStrategy::Centralized
        } else {
            ExecutionStrategy::Distributed
        }
    }

    /// Returns the next cloud function names for invocation.
    pub fn next_function(ctx: &ExecutionContext) -> Result<String> {
        let mut lambdas = match &ctx.next {
            CloudFunction::Sink(..) => vec![],
            CloudFunction::Group((name, num)) => {
                (0..*num).map(|i| format!("{}-{}", name, i)).collect()
            }
            CloudFunction::Lambda(name) => vec![name.to_owned()],
        };

        if lambdas.is_empty() {
            return Err(FlockError::Internal(
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
}

pub mod plan;
