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

//! The generic lambda function for sub-plan execution on AWS Lambda.

use arrow::record_batch::RecordBatch;
use aws_lambda_events::event::kafka::KafkaEvent;
use aws_lambda_events::event::kinesis::KinesisEvent;
use datafusion::physical_plan::Partitioning;
use futures::executor::block_on;
use lambda_runtime::{handler_fn, Context};
use log::warn;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{InvokeAsyncRequest, Lambda, LambdaClient};
use serde_json::Value;
use std::cell::Cell;
use std::sync::Once;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Initializes the lambda function once and only once.
static INIT: Once = Once::new();

thread_local! {
    /// Is in the testing environment.
    static IS_TESTING: Cell<bool> = Cell::new(false);
}

/// A wrapper to allow the declaration of the execution context of the lambda
/// function.
enum CloudFunctionContext {
    Lambda(Box<ExecutionContext>),
    Uninitialized,
}

/// Lambda execution context.
static mut EXECUTION_CONTEXT: CloudFunctionContext = CloudFunctionContext::Uninitialized;

/// Performs an initialization routine once and only once.
macro_rules! init_exec_context {
    () => {{
        unsafe {
            // Init query executor from the cloud evironment.
            let init_context = || match std::env::var(&globals["lambda"]["name"]) {
                Ok(s) => {
                    EXECUTION_CONTEXT =
                        CloudFunctionContext::Lambda(Box::new(ExecutionContext::unmarshal(&s)));
                }
                Err(_) => {
                    panic!("No execution context in the cloud environment.");
                }
            };
            if IS_TESTING.with(|t| t.get()) {
                init_context();
            } else {
                INIT.call_once(init_context);
            }
            match &mut EXECUTION_CONTEXT {
                CloudFunctionContext::Lambda(ctx) => ctx,
                CloudFunctionContext::Uninitialized => panic!("Uninitialized execution context!"),
            }
        }
    }};
}

#[tokio::main]
async fn main() -> Result<()> {
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}

/// Invoke functions in the next stage of the data flow.
fn invoke_async_functions(ctx: &ExecutionContext, batches: &mut Vec<RecordBatch>) -> Result<()> {
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
                invoke_args:   Payload::to_vec(
                    &[batches.pop().unwrap()],
                    uuid.clone(),
                    Encoding::default(),
                )
                .into(),
            };

            if let Ok(reponse) = block_on(client.invoke_async(request)) {
                if let Some(code) = reponse.status {
                    // A success response (202 Accepted) indicates that the request
                    // is queued for invocation.
                    if code == 202 {
                        break;
                    } else {
                        warn!("Unknown invoke error: {}, retry ... ", code);
                    }
                }
            }
        }
    });

    Ok(())
}

async fn source_handler(ctx: &mut ExecutionContext, event: Value) -> Result<Value> {
    let batch = match &ctx.datasource {
        DataSource::KinesisEvent(_) => {
            let kinesis_event: KinesisEvent = serde_json::from_value(event).unwrap();
            let batch = kinesis::to_batch(kinesis_event);
            if batch.is_empty() {
                return Err(SquirtleError::Execution("No Kinesis input!".to_owned()));
            }
            batch
        }
        DataSource::KafkaEvent(_) => {
            let kafka_event: KafkaEvent = serde_json::from_value(event).unwrap();
            let batch = kafka::to_batch(kafka_event);
            if batch.is_empty() {
                return Err(SquirtleError::Execution("No Kafka input!".to_owned()));
            }
            batch
        }
        _ => unimplemented!(),
    };

    match LambdaExecutor::choose_strategy(&ctx, &batch) {
        ExecutionStrategy::Centralized => {
            // feed data into the physical plan
            let output_partitions = LambdaExecutor::coalesce_batches(
                vec![batch],
                globals["lambda"]["target_batch_size"]
                    .parse::<usize>()
                    .unwrap(),
            )
            .await?;

            let num_batches = output_partitions[0].len();
            let parallelism = globals["lambda"]["parallelism"].parse::<usize>().unwrap();

            if num_batches > parallelism {
                ctx.feed_one_source(
                    &LambdaExecutor::repartition(
                        output_partitions,
                        Partitioning::RoundRobinBatch(parallelism),
                    )
                    .await?,
                );
            } else if num_batches > 1 {
                ctx.feed_one_source(
                    &LambdaExecutor::repartition(
                        output_partitions,
                        Partitioning::RoundRobinBatch(num_batches),
                    )
                    .await?,
                );
            } else {
                // only one batch exists
                assert!(num_batches == 1);
                ctx.feed_one_source(&output_partitions);
            }

            // query execution
            let batches = ctx.execute().await?;

            // send the results back to the client-side
            LambdaExecutor::event_sink(vec![batches]).await
        }
        ExecutionStrategy::Distributed => {
            let mut batches = LambdaExecutor::coalesce_batches(
                vec![batch],
                globals["lambda"]["payload_batch_size"]
                    .parse::<usize>()
                    .unwrap(),
            )
            .await?;
            assert_eq!(1, batches.len());

            invoke_async_functions(&ctx, &mut batches[0])?;
            Ok(serde_json::to_value(&ctx.name)?)
        }
    }
}

async fn payload_handler(ctx: &mut ExecutionContext, event: Value) -> Result<Value> {
    let (batches, uuid) = Payload::to_batch(event);

    ctx.feed_one_source(&vec![batches]);
    let batches = ctx.execute().await?;

    Ok(Payload::to_value(&batches, uuid, Encoding::default()))
}

async fn handler(event: Value, _: Context) -> Result<Value> {
    let mut ctx = init_exec_context!();

    match &ctx.datasource {
        DataSource::Payload => payload_handler(&mut ctx, event).await,
        DataSource::KinesisEvent(_) | DataSource::KafkaEvent(_) => {
            source_handler(&mut ctx, event).await
        }
        DataSource::Json => Ok(event),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::ExecutionPlan;
    use driver::QueryFlow;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn generic_lambda() -> Result<()> {
        IS_TESTING.with(|t| t.set(true));
        let plan = include_str!("../../../test/data/plan/simple_select.json");
        let name = "hello".to_owned();
        let next =
            CloudFunction::Solo("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836Z".to_owned());
        let datasource = DataSource::Json;

        let plan: Arc<dyn ExecutionPlan> = serde_json::from_str(&plan).unwrap();
        let lambda_context = ExecutionContext {
            plan,
            name,
            next,
            datasource,
        };

        let encoded = lambda_context.marshal(Encoding::default());

        // Configures the cloud environment
        std::env::set_var(&globals["lambda"]["name"], encoded);

        // First lambda call
        let event = json!({
            "db": "cmsc624"
        });
        assert_eq!(
            handler(event.clone(), Context::default())
                .await
                .expect("expected Ok(_) value"),
            event
        );

        // Second lambda call
        let event = json!({
            "net": "cmsc711"
        });
        assert_eq!(
            handler(event.clone(), Context::default())
                .await
                .expect("expected Ok(_) value"),
            event
        );

        Ok(())
    }

    fn init_lambda_exec(num: usize) -> Value {
        let sql = concat!(
            "SELECT MAX(c1), MIN(c2), c3 ",
            "FROM t1 ",
            "WHERE c2 < 99 GROUP BY c3"
        );
        let table_name = "t1";

        // 1. data source
        let datasource = DataSource::kinesis();
        // 2. data schema
        let (event, schema) = test_utils::random_event(&datasource, num);
        // 3. physical plan
        let plan = test_utils::physical_plan(&schema, &sql, &table_name);

        // create query flow
        let qflow = QueryFlow::new(sql, schema, datasource, plan);

        // set environment context for the first cloud function
        test_utils::set_env_context(&qflow, qflow.dag.node_count() - 1);

        event
    }

    #[tokio::test]
    async fn centralized_execution() -> Result<()> {
        IS_TESTING.with(|t| t.set(true));
        for (i, num) in [100, 1024, 10240].iter().enumerate() {
            let event = init_lambda_exec(*num);

            // cloud function execution
            let res = handler(event, Context::default()).await?;

            // check the result of function execution
            let (batches, _) = Payload::to_batch(res);

            if i == 0 {
                println!(
                    "{}",
                    arrow::util::pretty::pretty_format_batches(&batches).unwrap(),
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn repartition_execution() -> Result<()> {
        IS_TESTING.with(|t| t.set(true));
        let record_num = 10240;
        let event = init_lambda_exec(record_num);

        let kinesis_event: KinesisEvent = serde_json::from_value(event).unwrap();
        let batch = kinesis::to_batch(kinesis_event);

        assert_eq!(10, batch.len());

        (0..10).for_each(|i| assert_eq!(1024, batch[i].num_rows()));

        let new_batch =
            LambdaExecutor::repartition(vec![batch], Partitioning::RoundRobinBatch(8)).await?;

        assert_eq!(8, new_batch.len());

        (0..2).for_each(|i| {
            assert_eq!(2, new_batch[i].len());
            assert_eq!(1024, new_batch[i][0].num_rows());
            assert_eq!(1024, new_batch[i][1].num_rows());
        });

        (2..8).for_each(|i| {
            assert_eq!(1, new_batch[i].len());
            assert_eq!(1024, new_batch[i][0].num_rows());
        });

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn distributed_execution() -> Result<()> {
        IS_TESTING.with(|t| t.set(true));
        let event = init_lambda_exec(300000);

        // cloud function execution
        let _ = handler(event, Context::default()).await?;

        Ok(())
    }
}
