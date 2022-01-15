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

use flock::prelude::*;
use hashring::HashRing;
use lazy_static::lazy_static;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Once;

/// Initializes the lambda function once and only once.
pub static INIT: Once = Once::new();

thread_local! {
    /// Is in the testing environment.
    pub static IS_TESTING: Cell<bool> = Cell::new(false);
}

lazy_static! {
    pub static ref CONTEXT_NAME: String = FLOCK_CONF["lambda"]["environment"].to_string();
}

/// A wrapper to allow the declaration of the execution context of the lambda
/// function.
pub enum CloudFunctionContext {
    Lambda((Box<ExecutionContext>, Arena)),
    Uninitialized,
}
/// Lambda execution context.
pub static mut EXECUTION_CONTEXT: CloudFunctionContext = CloudFunctionContext::Uninitialized;

/// A wrapper to allow the declaration of consistent hashing.
pub enum ConsistentHashContext {
    Lambda((HashRing<String>, String /* group name */)),
    Uninitialized,
}
/// Consistent hashing context.
pub static mut CONSISTENT_HASH_CONTEXT: ConsistentHashContext =
    ConsistentHashContext::Uninitialized;

/// Performs an initialization routine once and only once.
#[macro_export]
macro_rules! init_exec_context {
    () => {{
        unsafe {
            // Init query executor from the cloud evironment.
            let init_context = || match std::env::var(&**CONTEXT_NAME) {
                Ok(s) => {
                    let ctx = context::unmarshal(&s).unwrap();
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

/// Returns the consistent hash context.
#[macro_export]
macro_rules! consistent_hash_context {
    () => {{
        unsafe {
            match &mut CONSISTENT_HASH_CONTEXT {
                ConsistentHashContext::Uninitialized => {
                    panic!("Uninitialized consistent hash context.")
                }
                ConsistentHashContext::Lambda((ring, group_name)) => (ring, group_name),
            }
        }
    }};
}

/// Updates consistent hash context.
///
/// To make data source generator function work generally, we *cannot*
/// use `CONSISTENT_HASH_CONTEXT` from cloud environment directly. The cloud
/// environment is used to specialize the plan for each function (stage
/// of the query). We WANT to use the same data source function to handle
/// all benchamrk queries.
pub fn update_consistent_hash_context(metadata: &Option<HashMap<String, String>>) -> Result<()> {
    if let Some(metadata) = metadata {
        if let Some(workers) = metadata.get("workers") {
            let next_function = serde_json::from_str(workers)?;

            let (group_name, group_size) = match &next_function {
                CloudFunction::Lambda(name) => (name.clone(), 1),
                CloudFunction::Group((name, size)) => (name.clone(), *size),
                CloudFunction::Sink(..) => (String::new(), 0),
            };

            // The *consistent hash* technique distributes the data packets in a time window
            // to the same function name in the function group. Because each function in the
            // function group has a concurrency of *1*, all data packets from the same query
            // can be routed to the same function execution environment.
            let mut ring: HashRing<String> = HashRing::new();
            match group_size {
                0 => {
                    unreachable!("group_size should not be 0.");
                }
                1 => {
                    // only one function in the function group, the data packets are routed to the
                    // next function.
                    ring.add(group_name.clone());
                }
                _ => {
                    // multiple functions in the function group, the data packets are routed to the
                    // function with the same hash value.
                    (0..group_size).for_each(|i| {
                        ring.add(format!("{}-{:02}", group_name, i));
                    });
                }
            }

            unsafe {
                // * `ring`: the consistent hashing ring to forward the windowed events to the
                // same function execution environment.
                // * `group_name`: function group name.
                CONSISTENT_HASH_CONTEXT = ConsistentHashContext::Lambda((ring, group_name));
            }
        }
    }

    Ok(())
}
