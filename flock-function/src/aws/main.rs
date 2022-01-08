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

//! The main entry point for the generic lambda function.

#![feature(get_mut_unchecked)]

mod actor;
mod nexmark;
mod s3;
mod window;
mod ysb;

use flock::prelude::*;
use hashring::HashRing;
use lambda_runtime::{handler_fn, Context};
use lazy_static::lazy_static;
use log::info;
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

lazy_static! {
    static ref CONTEXT_NAME: String = FLOCK_CONF["lambda"]["environment"].to_string();
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
                    let ctx = ExecutionContext::unmarshal(&s).unwrap();
                    let next_function = match &ctx.next {
                        CloudFunction::Lambda(name) => (name.clone(), 1),
                        CloudFunction::Group((name, group_size)) => {
                            (name.clone(), group_size.clone())
                        }
                        CloudFunction::Sink(..) => (String::new(), 0),
                    };

                    // The *consistent hash* technique distributes the data packets in a time window
                    // to the same function name in the function group. Because each function in the
                    // function group has a concurrency of *1*, all data packets from the same query
                    // can be routed to the same function execution environment.
                    let mut ring: HashRing<String> = HashRing::new();
                    if next_function.1 == 1 {
                        ring.add(next_function.0.clone());
                    } else if next_function.1 > 1 {
                        (0..next_function.1).for_each(|i| {
                            ring.add(format!("{}-{:02}", next_function.0, i));
                        });
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

async fn handler(event: Payload, _: Context) -> Result<Value> {
    let (ctx, arena) = init_exec_context!();

    info!("Lambda function architecture: {}", std::env::consts::ARCH);

    match &event.datasource {
        DataSource::Payload(_) => actor::handler(ctx, arena, event).await,
        DataSource::NEXMarkEvent(_) => nexmark::handler(ctx, event).await,
        DataSource::YSBEvent(_) => ysb::handler(ctx, event).await,
        DataSource::S3(_) => s3::handler(ctx, event).await,
        _ => unimplemented!(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}
