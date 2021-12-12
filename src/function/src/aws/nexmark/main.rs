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

mod executor;
mod utils;
mod window;

use arrow::record_batch::RecordBatch;
use futures::executor::block_on;
use hashring::HashRing;
use lambda_runtime::{handler_fn, Context};
use lazy_static::lazy_static;
use log::{info, warn};
use rayon::prelude::*;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{InvokeAsyncRequest, Lambda, LambdaClient};
use serde_json::json;
use serde_json::Value;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::Once;
use window::{elementwise_tasks, hopping_window_tasks, tumbling_window_tasks};

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

lazy_static! {
    static ref CONTEXT_NAME: String = FLOCK_CONF["lambda"]["name"].to_string();
}

/// A wrapper to allow the declaration of the execution context of the lambda
/// function.
enum CloudFunctionContext {
    Lambda((Box<ExecutionContext>, Arena)),
    Uninitialized,
}
/// Lambda execution context.
static mut EXECUTION_CONTEXT: CloudFunctionContext = CloudFunctionContext::Uninitialized;

/// A wrapper to allow the declaration of consistent hashing.
enum ConsistentHashContext {
    Lambda((HashRing<String>, String /* group name */)),
    Uninitialized,
}
/// Consistent hashing context.
static mut CONSISTENT_HASH_CONTEXT: ConsistentHashContext = ConsistentHashContext::Uninitialized;

/// Performs an initialization routine once and only once.
macro_rules! init_exec_context {
    () => {{
        unsafe {
            // Init query executor from the cloud evironment.
            let init_context = || match std::env::var(&**CONTEXT_NAME) {
                Ok(s) => {
                    let ctx = ExecutionContext::unmarshal(&s);
                    let next_function = match &ctx.next {
                        CloudFunction::Lambda(name) => (name.clone(), 1),
                        CloudFunction::Group((name, group_size)) => {
                            (name.clone(), group_size.clone())
                        }
                        CloudFunction::None => (String::new(), 0),
                    };

                    // The *consistent hash* technique distributes the data packets in a time window
                    // to the same function name in the function group. Because each function in the
                    // function group has a concurrency of *1*, all data packets from the same query
                    // can be routed to the same function execution environment.
                    let mut ring: HashRing<String> = HashRing::new();
                    if next_function.1 == 1 {
                        ring.add(next_function.0.clone());
                    } else if next_function.1 > 1 {
                        for i in 0..next_function.1 {
                            ring.add(format!("{}-{:02}", next_function.0, i));
                        }
                    }
                    CONSISTENT_HASH_CONTEXT =
                        ConsistentHashContext::Lambda((ring, next_function.0));
                    EXECUTION_CONTEXT = CloudFunctionContext::Lambda((Box::new(ctx), Arena::new()));
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
    event: Payload,
) -> Result<Value> {
    info!("Receiving a data packet: {:?}", event.uuid);
    let tid = event.uuid.tid.clone();

    let input_partitions = {
        if match &ctx.next {
            CloudFunction::None | CloudFunction::Lambda(..) => true,
            CloudFunction::Group(..) => false,
        } {
            // ressemble data packets to a single window.
            let (ready, uuid) = arena.reassemble(event);
            if ready {
                info!("Received all data packets for the window: {:?}", uuid.tid);
                arena.batches(uuid.tid)
            } else {
                let response = format!("Window data collection has not been completed.");
                info!("{}", response);
                return Ok(json!({ "response": response }));
            }
        } else {
            // data packet is an individual event for the current function.
            let (r1_records, r2_records, _) = event.to_record_batch();
            (vec![r1_records], vec![r2_records])
        }
    };

    let output = executor::collect(ctx, input_partitions.0, input_partitions.1).await?;

    if ctx.next != CloudFunction::None {
        let mut batches = LambdaExecutor::coalesce_batches(
            vec![output],
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
    Ok(json!({"name": &ctx.name, "tid": tid}))
}

async fn nexmark_bench_handler(ctx: &ExecutionContext, payload: Payload) -> Result<Value> {
    // Copy data source from the payload.
    let mut source = match payload.datasource {
        Some(DataSource::NexMarkEvent(source)) => source,
        _ => unreachable!(),
    };

    // Each source function is a data generator.
    let gen = source.config.get_as_or("threads", 1);
    let sec = source.config.get_as_or("seconds", 10);
    let eps = source.config.get_as_or("events-per-second", 1000);

    source.config.insert("threads", format!("{}", 1));
    source
        .config
        .insert("events-per-second", format!("{}", eps / gen));
    assert!(eps / gen > 0);

    let events = Arc::new(source.generate_data()?);
    let query_number = payload.query_number.expect("Query number is missing.");

    info!("Nexmark Benchmark: Query {:?}", query_number);
    info!("{:?}", source);
    info!("[OK] Generate nexmark events.");
    let (mut ring, group_name) = unsafe {
        match &mut CONSISTENT_HASH_CONTEXT {
            ConsistentHashContext::Lambda((ring, group_name)) => (ring, group_name),
            ConsistentHashContext::Uninitialized => {
                panic!("Uninitialized consistent hash context.")
            }
        }
    };

    match source.window {
        StreamWindow::TumblingWindow(Schedule::Seconds(window_size)) => {
            tumbling_window_tasks(
                query_number,
                events,
                sec,
                window_size,
                &mut ring,
                group_name.clone(),
            )
            .await?;
        }
        StreamWindow::HoppingWindow((window_size, hop_size)) => {
            hopping_window_tasks(
                query_number,
                events,
                sec,
                window_size,
                hop_size,
                &mut ring,
                group_name.clone(),
            )
            .await?;
        }
        StreamWindow::SlidingWindow((_window, _hop)) => {
            unimplemented!();
        }
        StreamWindow::ElementWise => {
            elementwise_tasks(query_number, events, sec, &mut ring, group_name.clone()).await?;
        }
        _ => unimplemented!(),
    };

    Ok(json!({"name": &ctx.name, "type": format!("nexmark_bench")}))
}

async fn handler(event: Payload, _: Context) -> Result<Value> {
    let (mut ctx, mut arena) = init_exec_context!();

    match &ctx.datasource {
        // TODO(gangliao): support other data sources.
        DataSource::Payload => payload_handler(&mut ctx, &mut arena, event).await,
        DataSource::NexMarkEvent(_) => nexmark_bench_handler(&ctx, event).await,
        _ => unimplemented!(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}
