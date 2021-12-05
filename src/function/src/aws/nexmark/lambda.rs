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
use arrow::util::pretty::pretty_format_batches;
use bytes::Bytes;
use chrono::Utc;
use datafusion::physical_plan::Partitioning;
use futures::executor::block_on;
use lambda_runtime::{handler_fn, Context};
use lazy_static::lazy_static;
use log::{info, warn};
use nexmark::event::{Auction, Bid, Person};
use nexmark::{NexMarkSource, NexMarkStream};
use rayon::prelude::*;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{
    InvocationRequest, InvocationResponse, InvokeAsyncRequest, Lambda, LambdaClient,
};
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

#[allow(dead_code)]
static LAMBDA_SYNC_CALL: &str = "RequestResponse";

#[allow(dead_code)]
static LAMBDA_ASYNC_CALL: &str = "Event";

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
    static ref LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
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

fn nexmark_event_to_payload(
    events: Arc<NexMarkStream>,
    time: usize,
    generator: usize,
    query_number: usize,
    uuid: Uuid,
) -> Result<Payload> {
    let event = events
        .select(time, generator)
        .expect("Failed to select event.");

    if event.persons.is_empty() && event.auctions.is_empty() && event.bids.is_empty() {
        return Err(FlockError::Execution("No Nexmark input!".to_owned()));
    }

    match query_number {
        0 | 1 | 2 => Ok(to_payload(
            &NexMarkSource::to_batch(&event.bids, BID_SCHEMA.clone()),
            &vec![],
            uuid,
        )),
        3 => Ok(to_payload(
            &NexMarkSource::to_batch(&event.persons, PERSON_SCHEMA.clone()),
            &NexMarkSource::to_batch(&event.auctions, AUCTION_SCHEMA.clone()),
            uuid,
        )),
        4 => Ok(to_payload(
            &NexMarkSource::to_batch(&event.auctions, AUCTION_SCHEMA.clone()),
            &NexMarkSource::to_batch(&event.bids, BID_SCHEMA.clone()),
            uuid,
        )),
        _ => unimplemented!(),
    }
}

async fn invoke_lambda_function(
    function_name: String,
    payload: Option<Bytes>,
) -> Result<InvocationResponse> {
    match LAMBDA_CLIENT
        .invoke(InvocationRequest {
            function_name,
            payload,
            invocation_type: Some(format!("Event")),
            ..Default::default()
        })
        .await
    {
        Ok(response) => return Ok(response),
        Err(err) => {
            return Err(FlockError::Execution(format!(
                "Lambda function execution failure: {}",
                err
            )))
        }
    }
}

async fn payload_handler(
    ctx: &mut ExecutionContext,
    arena: &mut Arena,
    event: Payload,
) -> Result<Value> {
    let tid = event.uuid.tid.clone();
    let input_partitions = {
        if match &ctx.next {
            CloudFunction::None | CloudFunction::Lambda(..) => true,
            CloudFunction::Group(..) => false,
        } {
            // ressemble data packets to a single window.
            let (ready, uuid) = arena.reassemble(event);
            if ready {
                arena.batches(uuid.tid)
            } else {
                return Err(FlockError::Execution(
                    "window data collection has not been completed.".to_string(),
                ));
            }
        } else {
            // data packet is an individual event for the current function.
            let (r1_records, r2_records, _) = event.to_record_batch();
            (vec![r1_records], vec![r2_records])
        }
    };

    if input_partitions.0.is_empty() || input_partitions.0[0].is_empty() {
        return Err(FlockError::Execution("payload data is empty.".to_string()));
    }

    let output = collect(ctx, input_partitions).await?;

    if ctx.next != CloudFunction::None {
        let mut batches = LambdaExecutor::coalesce_batches(
            vec![output],
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

    if ctx.debug {
        println!("{:?}", source);
        println!("[OK] Generate nexmark events.");
    }

    let tasks = match source.window {
        StreamWindow::TumblingWindow(Schedule::Seconds(_sec)) => {
            unimplemented!();
        }
        StreamWindow::HoppingWindow((_window, _hop))
        | StreamWindow::SlidingWindow((_window, _hop)) => {
            unimplemented!();
        }
        StreamWindow::ElementWise => (0..sec).map(|t| {
            if ctx.debug {
                println!("[OK] Send nexmark event (epoch: {}).", t);
            }
            let e = events.clone();
            let q = ctx.query_number.expect("query number is not set.");
            let f = match &ctx.next {
                CloudFunction::Lambda(name) => name.clone(),
                _ => unreachable!(),
            };
            tokio::spawn(async move {
                let u = UuidBuilder::new_with_ts(&f, Utc::now().timestamp(), 1).next();
                let p = serde_json::to_vec(&nexmark_event_to_payload(e, t, 0, q, u)?)?.into();
                Ok(vec![invoke_lambda_function(f, Some(p)).await?])
            })
        }),
        _ => unimplemented!(),
    }
    // this collect *is needed* so that the join below can switch between tasks.
    .collect::<Vec<tokio::task::JoinHandle<Result<Vec<InvocationResponse>>>>>();

    for task in tasks {
        let res_vec = task.await.expect("Lambda function execution failed.")?;
        res_vec.into_iter().for_each(|response| {
            info!(
                "[OK] Received status from async lambda function. {:?}",
                response
            );
        });
    }

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

async fn collect(
    ctx: &mut ExecutionContext,
    partitions: (Vec<Vec<RecordBatch>>, Vec<Vec<RecordBatch>>),
) -> Result<Vec<RecordBatch>> {
    let r1 = LambdaExecutor::repartition(partitions.0, Partitioning::RoundRobinBatch(*PARALLELISM))
        .await?;

    // check partitions.1 is empty
    if partitions.1.iter().map(|v| v.len()).sum::<usize>() == 0 {
        ctx.feed_one_source(&r1);
    } else {
        let r2 =
            LambdaExecutor::repartition(partitions.1, Partitioning::RoundRobinBatch(*PARALLELISM))
                .await?;
        ctx.feed_two_source(&r1, &r2);
    }

    // query execution
    let output = ctx.execute().await?;

    if ctx.debug {
        println!("{}", pretty_format_batches(&output)?);
        unsafe {
            INVOCATION_COUNTER_PER_INSTANCE += 1;
            info!("# invocations: {}", INVOCATION_COUNTER_PER_INSTANCE);
        }
    }

    Ok(output)
}

#[tokio::main]
async fn main() -> Result<()> {
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}
