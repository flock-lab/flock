// Copyright (c) 2020 UMD Database Group. All Rights Reserved.
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
// Only bring in dependencies for the repl when the cli feature is enabled.

//! The generic lambda function for sub-plan execution on AWS Lambda.
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Partitioning;
use futures::executor::block_on;
use lambda_runtime::{handler_fn, Context};
use lazy_static::lazy_static;
use log::warn;
use nexmark::event::{Auction, Bid, Person};
use rayon::prelude::*;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{InvokeAsyncRequest, Lambda, LambdaClient};
use serde_json::json;
use serde_json::Value;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::Once;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Initializes the lambda function once and only once.
static INIT: Once = Once::new();

/// The function invocation counter per lambda instance.
static mut INVOCATION_COUNTER_PER_INSTANCE: u32 = 0;

thread_local! {
    /// Is in the testing environment.
    static IS_TESTING: Cell<bool> = Cell::new(false);
}

lazy_static! {
    static ref PERSON_SCHEMA: SchemaRef = Arc::new(Person::schema());
    static ref AUCTION_SCHEMA: SchemaRef = Arc::new(Auction::schema());
    static ref BID_SCHEMA: SchemaRef = Arc::new(Bid::schema());
    static ref PARALLELISM: usize = globals["lambda"]["parallelism"].parse::<usize>().unwrap();
    static ref CONTEXT_NAME: String = globals["lambda"]["name"].to_string();
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
            let init_context = || match std::env::var(&**CONTEXT_NAME) {
                Ok(s) => {
                    EXECUTION_CONTEXT = CloudFunctionContext::Lambda((
                        Box::new(ExecutionContext::unmarshal(&s)),
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

async fn payload_handler(
    ctx: &mut ExecutionContext,
    arena: &mut Arena,
    event: Value,
) -> Result<Value> {
    let input_partitions = {
        if match &ctx.next {
            CloudFunction::None | CloudFunction::Solo(..) => true,
            CloudFunction::Chorus(..) => false,
        } {
            // ressemble lambda n to 1
            let (ready, uuid) = arena.reassemble(event);
            if ready {
                arena.batches(uuid.tid)
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
    ctx.feed_one_source(&input_partitions);
    let output_partitions = ctx.execute().await?;

    if ctx.next != CloudFunction::None {
        let mut batches = LambdaExecutor::coalesce_batches(
            vec![output_partitions],
            globals["lambda"]["payload_batch_size"]
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

async fn nexmark_bench_handler(ctx: &mut ExecutionContext, event: Payload) -> Result<Value> {
    let tid = event.uuid.tid.clone();
    if let DataSource::NexMarkEvent(source) = &ctx.datasource {
        match source.window {
            StreamWindow::TumblingWindow(Schedule::Seconds(_sec)) => {
                unimplemented!();
            }
            StreamWindow::HoppingWindow((_window, _hop))
            | StreamWindow::SlidingWindow((_window, _hop)) => {
                unimplemented!();
            }
            StreamWindow::ElementWise => {
                assert_eq!(event.uuid.seq_len, 1);
                collect(ctx, event).await?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(json!({"name": &ctx.name, "tid": tid}))
}

async fn handler(event: Payload, _: Context) -> Result<Value> {
    let (mut ctx, mut _arena) = init_exec_context!();

    match &ctx.datasource {
        // TODO(gangliao): support other data sources.
        // DataSource::Payload => payload_handler(&mut ctx, &mut arena, event).await,
        DataSource::NexMarkEvent(_) => nexmark_bench_handler(&mut ctx, event).await,
        _ => unimplemented!(),
    }
}

async fn feed_one_source(ctx: &mut ExecutionContext, batches: Vec<RecordBatch>) -> Result<()> {
    let num_batches = batches.len();
    let parallelism = globals["lambda"]["parallelism"].parse::<usize>().unwrap();

    if num_batches > parallelism {
        ctx.feed_one_source(
            &LambdaExecutor::repartition(vec![batches], Partitioning::RoundRobinBatch(parallelism))
                .await?,
        );
    } else if num_batches > 1 {
        ctx.feed_one_source(
            &LambdaExecutor::repartition(vec![batches], Partitioning::RoundRobinBatch(num_batches))
                .await?,
        );
    } else {
        // only one batch exists
        assert!(num_batches == 1);
        ctx.feed_one_source(&vec![batches]);
    }

    Ok(())
}

async fn feed_two_source(
    ctx: &mut ExecutionContext,
    left: Vec<RecordBatch>,
    right: Vec<RecordBatch>,
) -> Result<()> {
    let partitions = |n| {
        if n > *PARALLELISM {
            *PARALLELISM
        } else if n > 1 {
            n
        } else {
            1
        }
    };

    let n_left = partitions(left.len());
    let n_right = partitions(right.len());

    // repartition the batches in the left.
    let left = if n_left == 1 {
        vec![left]
    } else {
        LambdaExecutor::repartition(vec![left], Partitioning::RoundRobinBatch(n_left)).await?
    };

    // repartition the batches in the right.
    let right = if n_right == 1 {
        vec![right]
    } else {
        LambdaExecutor::repartition(vec![right], Partitioning::RoundRobinBatch(n_right)).await?
    };

    ctx.feed_two_source(&left, &right);
    Ok(())
}

async fn collect(ctx: &mut ExecutionContext, event: Payload) -> Result<Vec<RecordBatch>> {
    // feed the data to the dataflow graph
    let (r1, r2, _uuid) = event.to_record_batch();
    if r2.is_empty() {
        feed_one_source(ctx, r1).await?;
    } else {
        feed_two_source(ctx, r1, r2).await?;
    }

    // query execution
    let output = ctx.execute().await?;

    if ctx.debug {
        // let formatted =
        // arrow::util::pretty::pretty_format_batches(&output).unwrap();
        // println!("{}", formatted);
        unsafe {
            INVOCATION_COUNTER_PER_INSTANCE += 1;
            println!("# invocations: {}", INVOCATION_COUNTER_PER_INSTANCE);
        }
    }

    Ok(output)
}

#[tokio::main]
async fn main() -> Result<()> {
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}
