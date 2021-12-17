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

use crate::actor::*;
use chrono::Utc;
use driver::deploy::common::*;
use log::info;
use runtime::datasource::RelationPartitions;
use runtime::prelude::*;
use rusoto_lambda::InvocationResponse;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Generate tumble windows workloads for the benchmark on cloud
/// function services.
///
/// # Arguments
/// * `payload` - The payload of the function.
/// * `stream` - the source stream of events.
/// * `seconds` - the total number of seconds to generate workloads.
/// * `window_size` - the size of the window in seconds.
pub async fn tumbling_window_tasks(
    payload: Payload,
    stream: Arc<dyn DataStream>,
    seconds: usize,
    window_size: usize,
) -> Result<()> {
    assert!(seconds >= window_size);
    let mut tasks: Vec<JoinHandle<Result<InvocationResponse>>> = vec![];

    // To make data source generator function work generally, we *cannot*
    // use `CONSISTENT_HASH_CONTEXT` from cloud environment. The cloud
    // environment is used to specialize the plan for each function (stage
    // of the query). We WANT to use the same data source function to handle
    // all benchamrk queries.
    // `ring`: the consistent hashing ring to forward the windowed events to the
    // same function execution environment.
    // `group_name`: the name of the group of the function.
    let (mut ring, group_name) = infer_actor_info(payload.metadata)?;

    let mut window: Box<Vec<(RelationPartitions, RelationPartitions)>> = Box::new(vec![]);

    for time in 0..seconds / window_size {
        let start = time * window_size;
        let end = start + window_size;

        // Update the tumbling window, and generate the next batch of data.
        window.drain(..);
        for t in start..end {
            window.push(stream.select_event_to_batches(
                t,
                0, // generator id
                payload.query_number,
            )?);
        }

        // Calculate the total data packets to be sent.
        let size = window
            .iter()
            .map(|(a, b)| if a.len() > b.len() { a.len() } else { b.len() })
            .sum::<usize>();

        let mut uuid_builder = UuidBuilder::new_with_ts(&group_name, Utc::now().timestamp(), size);

        // Distribute the window data to a single function execution environment.
        let function_name = ring
            .get(&uuid_builder.tid)
            .expect("hash ring failure.")
            .to_string();

        // Call the next stage of the dataflow graph.
        info!(
            "[OK] Send {} events from a window (epoch: {}-{}) to function: {}.",
            size,
            time,
            time + window_size,
            function_name
        );

        let mut eid = 0;
        for (a, b) in window.iter() {
            let num = if a.len() > b.len() { a.len() } else { b.len() };
            for i in 0..num {
                let ac = a.clone();
                let bc = b.clone();
                let empty = vec![];
                let epoch_id = eid;
                let uuid = uuid_builder.next();
                let func_name = function_name.clone();
                tasks.push(tokio::spawn(async move {
                    let r1 = || if i < ac.len() { &ac[i] } else { &empty };
                    let r2 = || if i < bc.len() { &bc[i] } else { &empty };
                    let payload = serde_json::to_vec(&to_payload(r1(), r2(), uuid))?;
                    info!(
                        "[OK] Event {} - function payload bytes: {}",
                        epoch_id,
                        payload.len()
                    );
                    invoke_lambda_function(func_name, Some(payload.into())).await
                }));
                eid += 1;
            }
        }
    }

    for task in tasks {
        task.await.expect("Lambda function execution failed.")?;
    }

    Ok(())
}

/// Generate hopping windows workloads for the benchmark on cloud
/// function services.
///
/// # Arguments
/// * `payload` - The payload of the function.
/// * `stream` - the source stream of events.
/// * `seconds` - the total number of seconds to generate workloads.
/// * `window_size` - the size of the window in seconds.
/// * `hop_size` - the size of the hop in seconds.
pub async fn hopping_window_tasks(
    payload: Payload,
    stream: Arc<dyn DataStream>,
    seconds: usize,
    window_size: usize,
    hop_size: usize,
) -> Result<()> {
    assert!(seconds >= window_size);

    let mut tasks: Vec<JoinHandle<Result<InvocationResponse>>> = vec![];
    let (mut ring, group_name) = infer_actor_info(payload.metadata)?;
    let mut window: Box<Vec<(RelationPartitions, RelationPartitions)>> = Box::new(vec![]);

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
        for t in time + start_pos..time + window_size {
            window.push(stream.select_event_to_batches(
                t,
                0, // generator id
                payload.query_number,
            )?);
        }

        // Calculate the total data packets to be sent.
        let size = window
            .iter()
            .map(|(a, b)| if a.len() > b.len() { a.len() } else { b.len() })
            .sum::<usize>();

        let mut uuid_builder = UuidBuilder::new_with_ts(&group_name, Utc::now().timestamp(), size);

        // Distribute the window data to a single function execution environment.
        let function_name = ring
            .get(&uuid_builder.tid)
            .expect("hash ring failure.")
            .to_string();

        // Call the next stage of the dataflow graph.
        info!(
            "[OK] Send {} events from a window (epoch: {}-{}) to function: {}.",
            size,
            time,
            time + window_size,
            function_name
        );

        let mut eid = 0;
        for (a, b) in window.iter() {
            let num = if a.len() > b.len() { a.len() } else { b.len() };
            for i in 0..num {
                let ac = a.clone();
                let bc = b.clone();
                let empty = vec![];
                let epoch_id = eid;
                let uuid = uuid_builder.next();
                let func_name = function_name.clone();
                tasks.push(tokio::spawn(async move {
                    let r1 = || if i < ac.len() { &ac[i] } else { &empty };
                    let r2 = || if i < bc.len() { &bc[i] } else { &empty };
                    let payload = serde_json::to_vec(&to_payload(r1(), r2(), uuid))?;
                    info!(
                        "[OK] Event {} - function payload bytes: {}",
                        epoch_id,
                        payload.len()
                    );
                    invoke_lambda_function(func_name, Some(payload.into())).await
                }));
                eid += 1;
            }
        }
    }

    for task in tasks {
        task.await.expect("Lambda function execution failed.")?;
    }

    Ok(())
}

/// Generate normal elementwose workloads for the benchmark on cloud
/// function services.
///
/// # Arguments
/// * `payload` - The payload of the function.
/// * `stream` - the source stream of events.
/// * `seconds` - the total number of seconds to generate workloads.
pub async fn elementwise_tasks(
    payload: Payload,
    stream: Arc<dyn DataStream + Send + Sync>,
    seconds: usize,
) -> Result<()> {
    let (mut ring, group_name) = infer_actor_info(payload.metadata)?;
    let mut tasks: Vec<JoinHandle<Result<InvocationResponse>>> = vec![];
    for epoch in 0..seconds {
        info!("[OK] Send events (epoch: {}).", epoch);
        let events = stream.clone();
        if ring.len() == 1 {
            // lambda default concurrency is 1000.
            let function_name = group_name.clone();
            tasks.push(tokio::spawn(async move {
                let uuid =
                    UuidBuilder::new_with_ts(&function_name, Utc::now().timestamp(), 1).next();
                let payload = serde_json::to_vec(&events.select_event_to_payload(
                    epoch,
                    0,
                    payload.query_number,
                    uuid,
                )?)?
                .into();
                invoke_lambda_function(function_name, Some(payload)).await
            }));
        } else {
            // Calculate the total data packets to be sent.
            // transfrom tuple (a, b) to (Arc::new(a), Arc::new(b))
            let (a, b) = events.select_event_to_batches(
                epoch,
                0, // generator id
                payload.query_number,
            )?;
            let size = if a.len() > b.len() { a.len() } else { b.len() };

            let mut uuid_builder =
                UuidBuilder::new_with_ts(&group_name, Utc::now().timestamp(), size);

            // Distribute the epoch data to a single function execution environment.
            let function_name = ring
                .get(&uuid_builder.tid)
                .expect("hash ring failure.")
                .to_string();

            // Call the next stage of the dataflow graph.
            info!(
                "[OK] Send {} events from epoch {} to function: {}.",
                size, epoch, function_name
            );

            for i in 0..size {
                let ac = a.clone();
                let bc = b.clone();
                let empty = vec![];
                let uuid = uuid_builder.next();
                let func_name = function_name.clone();
                tasks.push(tokio::spawn(async move {
                    let r1 = || if i < ac.len() { &ac[i] } else { &empty };
                    let r2 = || if i < bc.len() { &bc[i] } else { &empty };
                    let payload = serde_json::to_vec(&to_payload(r1(), r2(), uuid))?;
                    info!(
                        "[OK] Event {} - function payload bytes: {}",
                        i,
                        payload.len()
                    );
                    invoke_lambda_function(func_name, Some(payload.into())).await
                }));
            }
        }
    }

    for task in tasks {
        task.await.expect("Lambda function execution failed.")?;
    }

    Ok(())
}
