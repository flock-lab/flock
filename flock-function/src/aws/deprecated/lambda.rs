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

//! The generic lambda function for sub-plan execution on AWS Lambda.

use aws_lambda_events::event::kafka::KafkaEvent;
use aws_lambda_events::event::kinesis::KinesisEvent;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Partitioning;
use futures::executor::block_on;
use lambda_runtime::{service_fn, LambdaEvent};
use log::warn;
use rayon::prelude::*;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{InvokeAsyncRequest, Lambda, LambdaClient};
use serde_json::Value;
use std::cell::Cell;
use std::sync::Once;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Initializes the lambda function once and only once.
static INIT: Once = Once::new();

thread_local! {
    /// Is in the testing environment.
    static IS_TESTING: Cell<bool> = Cell::new(false);
}

/// A wrapper to allow the declaration of the execution context of the lambda
/// function.
enum CloudFunctionContext {
    Lambda((Box<ExecutionContext>, Arena)),
    Uninitialized,
}

/// Lambda execution context.
static mut EXECUTION_CONTEXT: CloudFunctionContext = CloudFunctionContext::Uninitialized;

/// Performs an initialization routine once and only once.
macro_rules! init_exec_context {
    () => {{
        unsafe {
            // Init query executor from the cloud evironment.
            let init_context = || match std::env::var(&FLOCK_CONF["lambda"]["environment"]) {
                Ok(s) => {
                    EXECUTION_CONTEXT = CloudFunctionContext::Lambda((
                        Box::new(ExecutionContext::unmarshal(&s).unwrap()),
                        Arena::new(),
                    ));
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
                CloudFunctionContext::Lambda((ctx, arena)) => (ctx, arena),
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
fn invoke_next_functions(ctx: &ExecutionContext, batches: &mut Vec<RecordBatch>) -> Result<()> {
    // retrieve the next lambda function names
    let next_func = LambdaExecutor::next_function(&ctx)?;

    // create uuid builder to assign id to each payload
    let uuid_builder = UuidBuilder::new(&ctx.name, batches.len());

    let client = &LambdaClient::new(Region::default());
    batches.into_par_iter().enumerate().for_each(|(i, batch)| {
        // call the lambda function asynchronously until it succeeds.
        loop {
            let uuid = uuid_builder.get(i);
            let request = InvokeAsyncRequest {
                function_name: next_func.clone(),
                invoke_args:   to_bytes(&batch, uuid, Encoding::default()),
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
                return Err(FlockError::Execution("No Kinesis input!".to_owned()));
            }
            batch
        }
        DataSource::KafkaEvent(_) => {
            let kafka_event: KafkaEvent = serde_json::from_value(event).unwrap();
            let batch = kafka::to_batch(kafka_event);
            if batch.is_empty() {
                return Err(FlockError::Execution("No Kafka input!".to_owned()));
            }
            batch
        }
        _ => unimplemented!(),
    };

    match LambdaExecutor::choose_strategy(&ctx, &batch) {
        ExecutionStrategy::Centralized => {
            // feed data into the physical plan
            let output_partitions = coalesce_batches(
                vec![batch],
                FLOCK_CONF["lambda"]["target_batch_size"]
                    .parse::<usize>()
                    .unwrap(),
            )
            .await?;

            let num_batches = output_partitions[0].len();
            let concurrency = FLOCK_CONF["lambda"]["concurrency"]
                .parse::<usize>()
                .unwrap();

            if num_batches > concurrency {
                ctx.feed_one_source(
                    &repartition(
                        output_partitions,
                        Partitioning::RoundRobinBatch(concurrency),
                    )
                    .await?,
                )
                .await?;
            } else if num_batches > 1 {
                ctx.feed_one_source(
                    &repartition(
                        output_partitions,
                        Partitioning::RoundRobinBatch(num_batches),
                    )
                    .await?,
                )
                .await?;
            } else {
                // only one batch exists
                assert!(num_batches == 1);
                ctx.feed_one_source(&output_partitions).await?;
            }

            // query execution
            let batches = ctx.execute().await?;

            // send the results back to the client-side
            LambdaExecutor::event_sink(vec![batches]).await
        }
        ExecutionStrategy::Distributed => {
            let mut batches = coalesce_batches(
                vec![batch],
                FLOCK_CONF["lambda"]["payload_batch_size"]
                    .parse::<usize>()
                    .unwrap(),
            )
            .await?;
            assert_eq!(1, batches.len());

            invoke_next_functions(&ctx, &mut batches[0])?;
            Ok(serde_json::to_value(&ctx.name)?)
        }
    }
}

async fn payload_handler(
    ctx: &mut ExecutionContext,
    arena: &mut Arena,
    event: Value,
) -> Result<Value> {
    let input_partitions = {
        if match &ctx.next {
            CloudFunction::Sink(..) | CloudFunction::Lambda(..) => true,
            CloudFunction::Group(..) => false,
        } {
            // ressemble lambda n to 1
            let (ready, uuid) = arena.reassemble(event);
            if ready {
                arena.batches(uuid.qid)
            } else {
                return Err(FlockError::Execution(
                    "window data collection has not been completed.".to_string(),
                ));
            }
        } else {
            // partition lambda 1 to n
            let (batch, _, _) = to_batch(event);
            vec![batch]
        }
    };

    if input_partitions.is_empty() || input_partitions[0].is_empty() {
        return Err(FlockError::Execution("payload data is empty.".to_string()));
    }

    // TODO(gangliao): repartition input batches to speedup the operations.
    ctx.feed_one_source(&input_partitions).await?;
    let output_partitions = ctx.execute().await?;

    if ctx.next != CloudFunction::Sink(..) {
        let mut batches = coalesce_batches(
            vec![output_partitions],
            FLOCK_CONF["lambda"]["payload_batch_size"]
                .parse::<usize>()
                .unwrap(),
        )
        .await?;
        assert_eq!(1, batches.len());
        // call the next stage of the dataflow graph.
        invoke_next_functions(&ctx, &mut batches[0])?;
    }

    // TODO(gangliao): sink results to other cloud services.
    Ok(serde_json::to_value(&ctx.name)?)
}

async fn handler(event: LambdaEvent<Value>) -> Result<Value> {
    let (mut ctx, mut arena) = init_exec_context!();

    match &ctx.datasource {
        DataSource::Payload(_) => payload_handler(&mut ctx, &mut arena, event).await,
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
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::ExecutionPlan;
    use driver::QueryFlow;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    #[ignore]
    async fn generic_lambda() -> Result<()> {
        IS_TESTING.with(|t| t.set(true));
        let plan = include_str!("../../../tests/data/plan/simple_select.json");
        let name = "hello".to_owned();
        let next =
            CloudFunction::Lambda("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836Z".to_owned());

        let plan: Arc<dyn ExecutionPlan> = serde_json::from_str(&plan).unwrap();
        let lambda_context = ExecutionContext {
            plan,
            name,
            next,
            ..Default::default()
        };

        let encoded = lambda_context.marshal(Encoding::default())?;

        // Configures the cloud environment
        std::env::set_var(&FLOCK_CONF["lambda"]["environment"], encoded);

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
            let (batches, _, _) = to_batch(res);

            if i == 0 {
                println!("{}", pretty_format_batches(&batches)?,);
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

        let new_batch = repartition(vec![batch], Partitioning::RoundRobinBatch(8)).await?;

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
