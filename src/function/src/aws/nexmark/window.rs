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

use crate::utils::*;
use chrono::Utc;
use hashring::HashRing;
use nexmark::NexMarkStream;
use runtime::prelude::*;
use std::sync::Arc;

/// Generate tumble windows workloads for the nexmark benchmark on cloud
/// function services.
///
/// # Arguments
/// - `ctx`: the Flock runtime context.
/// - `source`: the source stream of nexmark events.
/// - `seconds`: the total number of seconds to generate workloads.
/// - `window_size`: the size of the window in seconds.
/// - `ring`: the consistent hashing ring to forward the windowed events to the
///   same function execution environment.
/// - `group_name`: the name of the group of the function.
///
/// # Returns
/// The task join handles of the generated workloads.
pub async fn tumbling_window_tasks(
    ctx: &ExecutionContext,
    source: Arc<NexMarkStream>,
    seconds: usize,
    window_size: usize,
    ring: &mut HashRing<String>,
    group_name: String,
) -> Result<()> {
    assert!(seconds >= window_size);

    let mut window: Box<Vec<Payload>> = Box::new(vec![]);
    let query_number = ctx.query_number.expect("query number is not set.");

    for time in 0..seconds / window_size {
        let start = time * window_size;
        let end = start + window_size;
        let mut uuid = UuidBuilder::new_with_ts(&group_name, Utc::now().timestamp(), window_size);

        window.drain(..);
        for t in start..end {
            window.push(nexmark_event_to_payload(
                source.clone(),
                t,
                0, // generator id
                query_number,
                uuid.next(),
            )?);
        }

        // Distribute the window data to a single function execution environment.
        let function_name = ring.get(&uuid.tid).expect("hash ring failure.").to_string();
        let events = window.clone();

        // Call the next stage of the dataflow graph.
        if ctx.debug {
            println!(
                "[OK] Send nexmark events from a window (epoch: {}-{}) to function: {}.",
                start, end, function_name
            );
        }

        for i in 0..window_size {
            let resp = invoke_lambda_function(
                function_name.clone(),
                Some(serde_json::to_vec(&events[i])?.into()),
            )
            .await?;
            if ctx.debug {
                println!(
                    "[OK] Received status from async lambda function. {:?}",
                    resp
                );
            }
        }
    }

    Ok(())
}

/// Generate hopping windows workloads for the nexmark benchmark on cloud
/// function services.
///
/// # Arguments
/// - `ctx`: the Flock runtime context.
/// - `source`: the source stream of nexmark events.
/// - `seconds`: the total number of seconds to generate workloads.
/// - `window_size`: the size of the window in seconds.
/// - `hop_size`: the size of the hop in seconds.
/// - `ring`: the consistent hashing ring to forward the windowed events to the
///   same function execution environment.
/// - `group_name`: the name of the group of the function.
///
/// # Returns
/// The task join handles of the generated workloads.
pub async fn hopping_window_tasks(
    ctx: &ExecutionContext,
    source: Arc<NexMarkStream>,
    seconds: usize,
    window_size: usize,
    hop_size: usize,
    ring: &mut HashRing<String>,
    group_name: String,
) -> Result<()> {
    assert!(seconds >= window_size);

    let mut window: Box<Vec<Payload>> = Box::new(vec![]);
    let query_number = ctx.query_number.expect("query number is not set.");

    for time in (0..seconds).step_by(hop_size) {
        if time + window_size > seconds {
            break;
        }

        // Move the hopping window forward.
        let mut start_pos = 0;
        if !window.is_empty() {
            window.drain(..hop_size);
            start_pos = window_size - hop_size;
        }

        // Update the hopping window, and generate the next batch of data.
        let mut uuid = UuidBuilder::new_with_ts(&group_name, Utc::now().timestamp(), window_size);
        for t in time + start_pos..time + window_size {
            window.push(nexmark_event_to_payload(
                source.clone(),
                t,
                0, // generator id
                query_number,
                uuid.next(),
            )?);
        }

        // Distribute the window data to a single function execution environment.
        let function_name = ring.get(&uuid.tid).expect("hash ring failure.").to_string();
        let events = window.clone();

        // Call the next stage of the dataflow graph.
        if ctx.debug {
            println!(
                "[OK] Send nexmark events from a window (epoch: {}-{}) to function: {}.",
                time,
                time + window_size,
                function_name
            );
        }

        for t in time..time + window_size {
            let offset = t - time;
            let resp = invoke_lambda_function(
                function_name.clone(),
                Some(serde_json::to_vec(&events[offset])?.into()),
            )
            .await?;
            if ctx.debug {
                println!(
                    "[OK] Received status from async lambda function. {:?}",
                    resp
                );
            }
        }
    }

    Ok(())
}

/// Generate normal elementwose workloads for the nexmark benchmark on cloud
/// function services.
///
/// # Arguments
/// - `ctx`: the Flock runtime context.
/// - `source`: the source stream of nexmark events.
/// - `seconds`: the total number of seconds to generate workloads.
/// - `ring`: the consistent hashing ring to forward the windowed events to the
///   same function execution environment.
/// - `group_name`: the name of the group of the function.
///
/// # Returns
/// The task join handles of the generated workloads.
pub async fn elementwise_tasks(
    ctx: &ExecutionContext,
    source: Arc<NexMarkStream>,
    seconds: usize,
    ring: &mut HashRing<String>,
    group_name: String,
) -> Result<()> {
    for epoch in 0..seconds {
        if ctx.debug {
            println!("[OK] Send nexmark event (epoch: {}).", epoch);
        }

        let events = source.clone();
        let query_number = ctx.query_number.expect("query number is not set.");

        if ring.len() == 1 {
            // lambda default concurrency
            let function_name = group_name.clone();
            let uuid = UuidBuilder::new_with_ts(&function_name, Utc::now().timestamp(), 1).next();
            let payload = serde_json::to_vec(&nexmark_event_to_payload(
                events,
                epoch,
                0,
                query_number,
                uuid,
            )?)?
            .into();
            let resp = invoke_lambda_function(function_name, Some(payload)).await?;
            if ctx.debug {
                println!(
                    "[OK] Received status from async lambda function. {:?}",
                    resp
                );
            }
        } else {
            // consistent hashing ring
            let (r1, r2) = nexmark_event_to_batches(
                events,
                epoch,
                0, // generator id
                query_number,
            )?;
            let size = if r1.len() > r2.len() {
                r1.len()
            } else {
                r2.len()
            };
            let mut uuid = UuidBuilder::new_with_ts(&group_name, Utc::now().timestamp(), size);
            // Distribute the epoch data to a single function execution environment.
            let function_name = ring.get(&uuid.tid).expect("hash ring failure.").to_string();
            for i in 0..size {
                let batch1 = if i < r1.len() { r1[i].clone() } else { vec![] };
                let batch2 = if i < r2.len() { r2[i].clone() } else { vec![] };
                let payload = serde_json::to_vec(&to_payload(&batch1, &batch2, uuid.next()))?;
                if ctx.debug {
                    println!("[{}]: function payload bytes: {}", i, payload.len());
                }
                let resp =
                    invoke_lambda_function(function_name.clone(), Some(payload.into())).await?;
                if ctx.debug {
                    println!(
                        "[OK] Received status from async lambda function. {:?}",
                        resp
                    );
                }
            }
        }
    }

    Ok(())
}
