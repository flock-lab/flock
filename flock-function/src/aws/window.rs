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
use arrow::array::{Int32Array, TimestampMillisecondArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::logical_plan::{col, count_distinct};
use datafusion::physical_plan::expressions::col as expr_col;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::Partitioning::RoundRobinBatch;
use flock::datasource::nexmark::config::BASE_TIME;
use flock::prelude::*;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

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
    let sync = infer_invocation_type(&payload.metadata)?;
    let invocation_type = if sync {
        FLOCK_LAMBDA_SYNC_CALL.to_string()
    } else {
        FLOCK_LAMBDA_ASYNC_CALL.to_string()
    };

    // To make data source generator function work generally, we *cannot*
    // use `CONSISTENT_HASH_CONTEXT` from cloud environment. The cloud
    // environment is used to specialize the plan for each function (stage
    // of the query). We WANT to use the same data source function to handle
    // all benchamrk queries.
    // `ring`: the consistent hashing ring to forward the windowed events to the
    // same function execution environment.
    // `group_name`: the name of the group of the function.
    let (mut ring, group_name) = infer_actor_info(&payload.metadata)?;

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
                sync,
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
        let empty = vec![];
        for (a, b) in window.iter() {
            let num = if a.len() > b.len() { a.len() } else { b.len() };
            for i in 0..num {
                let payload = serde_json::to_vec(&to_payload(
                    if i < a.len() { &a[i] } else { &empty },
                    if i < b.len() { &b[i] } else { &empty },
                    uuid_builder.next_uuid(),
                    sync,
                ))?;
                info!(
                    "[OK] Event {} - function payload bytes: {}",
                    eid,
                    payload.len()
                );
                invoke_lambda_function(
                    function_name.clone(),
                    Some(payload.into()),
                    invocation_type.clone(),
                )
                .await?;
                eid += 1;
            }
        }
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
    let sync = infer_invocation_type(&payload.metadata)?;
    let invocation_type = if sync {
        FLOCK_LAMBDA_SYNC_CALL.to_string()
    } else {
        FLOCK_LAMBDA_ASYNC_CALL.to_string()
    };

    let (mut ring, group_name) = infer_actor_info(&payload.metadata)?;
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
                sync,
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
        let empty = vec![];
        for (a, b) in window.iter() {
            let num = if a.len() > b.len() { a.len() } else { b.len() };
            for i in 0..num {
                let payload = serde_json::to_vec(&to_payload(
                    if i < a.len() { &a[i] } else { &empty },
                    if i < b.len() { &b[i] } else { &empty },
                    uuid_builder.next_uuid(),
                    sync,
                ))?;
                info!(
                    "[OK] Event {} - function payload bytes: {}",
                    eid,
                    payload.len()
                );
                invoke_lambda_function(
                    function_name.clone(),
                    Some(payload.into()),
                    invocation_type.clone(),
                )
                .await?;
                eid += 1;
            }
        }
    }

    Ok(())
}

/// Get back the input data from the registered table after the query is
/// executed to avoid copying the input data.
fn get_input_from_registered_table(
    ctx: &mut ExecutionContext,
    table_name: &str,
) -> Result<Vec<Vec<RecordBatch>>> {
    Ok(unsafe {
        Arc::get_mut_unchecked(
            &mut ctx
                .deregister_table(table_name)
                .map_err(FlockError::DataFusion)?
                .ok_or_else(|| {
                    FlockError::Internal(format!("Failed to deregister table `{}`", table_name))
                })?,
        )
        .as_mut_any()
        .downcast_mut::<MemTable>()
        .unwrap()
        .batches()
    })
}

/// Add each unique partition to a distinct session window.
///
/// # Arguments
/// * `partitions` - the partitions to be added.
/// * `windows` - the session windows.
/// * `timeouts` - the session timeouts.
///
/// # Return
/// The updated session windows.
fn add_partitions_to_session_windows(
    partitions: Vec<Vec<RecordBatch>>,
    windows: &mut HashMap<usize, Vec<Vec<RecordBatch>>>,
    timeout: usize,
) -> Result<Vec<Vec<Vec<RecordBatch>>>> {
    Ok(partitions
        .into_iter()
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut session = vec![];
            let bidder = p[0]
                .column(1 /* bidder field */)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0);
            let current_timestamp = p[0]
                .column(3 /* b_date_time field */)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0);
            let curr_date_time = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(current_timestamp / 1000, 0),
                Utc,
            );

            if !windows.contains_key(&(bidder as usize)) {
                windows
                    .entry(bidder as usize)
                    .or_insert_with(Vec::new)
                    .push(p);
            } else {
                // get the last batch in the window.
                let batch = windows
                    .get(&(bidder as usize))
                    .unwrap()
                    .last()
                    .unwrap()
                    .last()
                    .unwrap();
                // get the last bid record's timestamp.
                let last_timestamp = batch
                    .column(3 /* b_date_time field */)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(batch.num_rows() - 1);
                let last_date_time = DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(last_timestamp / 1000, 0),
                    Utc,
                );

                // If the current date time isn't 10 seconds later than the last date time,
                // then we can add the current batch to the window . Otherwise, we have a new
                // session window.
                if curr_date_time.signed_duration_since(last_date_time)
                    > chrono::Duration::seconds(timeout as i64)
                {
                    session = windows.remove(&(bidder as usize)).unwrap();
                }
                windows
                    .entry(bidder as usize)
                    .or_insert_with(Vec::new)
                    .push(p);
            }
            session
        })
        .collect::<Vec<Vec<Vec<RecordBatch>>>>())
}

/// Find new session windows after timeout.
///
/// # Arguments
/// * `windows` - the session windows.
/// * `timeout` - the session timeout.
/// * `epoch_count` - the number of epochs have been processed.
///
/// # Return
/// The keys of the new session windows.
fn find_timeout_session_windows(
    windows: &HashMap<usize, Vec<Vec<RecordBatch>>>,
    timeout: usize,
    epoch_count: usize,
) -> Result<Vec<usize>> {
    let mut to_remove = vec![];

    windows.iter().for_each(|(bidder, batches)| {
        // Get the last batch
        let batch = batches.last().unwrap().last().unwrap();

        // Get the last bid's date time
        let last_timestamp = batch
            .column(3 /* b_date_time field */)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(batch.num_rows() - 1);

        let last_date_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(last_timestamp / 1000, 0), Utc);

        let epoch_gap_time = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(BASE_TIME as i64 / 1000 + epoch_count as i64, 0),
            Utc,
        );

        if epoch_gap_time.signed_duration_since(last_date_time)
            > chrono::Duration::seconds(timeout as i64)
        {
            to_remove.push(bidder.to_owned());
        }
    });

    Ok(to_remove)
}

/// Session windows group events that arrive at similar times, filtering out
/// periods of time where there is no data. A session window begins when the
/// first event occurs. If another event occurs within the specified timeout
/// from the last ingested event, then the window extends to include the new
/// event. Otherwise if no events occur within the timeout, then the window is
/// closed at the timeout.
pub async fn session_window_tasks(
    payload: Payload,
    stream: Arc<dyn DataStream>,
    seconds: usize,
    timeout: usize,
) -> Result<()> {
    let sync = infer_invocation_type(&payload.metadata)?;
    let (group_key, table_name) = infer_session_keys(&payload.metadata)?;
    let (mut ring, group_name) = infer_actor_info(&payload.metadata)?;

    let invocation_type = if sync {
        FLOCK_LAMBDA_SYNC_CALL.to_string()
    } else {
        FLOCK_LAMBDA_ASYNC_CALL.to_string()
    };

    let granule_size = if sync {
        *FLOCK_SYNC_GRANULE_SIZE
    } else {
        *FLOCK_ASYNC_GRANULE_SIZE
    };

    let mut ctx = ExecutionContext::new();
    let mut windows: HashMap<usize, Vec<Vec<RecordBatch>>> = HashMap::new();

    for time in 0..seconds {
        let (r1, _) = stream.select_event_to_batches(
            time,
            0, // generator id
            payload.query_number,
            sync,
        )?;
        let schema = r1[0][0].schema();

        let table = MemTable::try_new(schema.clone(), r1.to_vec())?;
        ctx.deregister_table(&*table_name)?;
        ctx.register_table(&*table_name, Arc::new(table))?;

        // Equivalent to `SELECT COUNT(DISTINCT group_key) FROM table_name;`
        let output = ctx
            .table(&*table_name)?
            .aggregate(vec![], vec![count_distinct(col(&group_key))])?
            .collect()
            .await?;

        let distinct_keys = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);

        // Each partition has a unique key after `repartition` execution.
        let partitions = repartition(
            get_input_from_registered_table(&mut ctx, &table_name)?,
            Partitioning::HashDiff(vec![expr_col(&group_key, &schema)?], distinct_keys as usize),
        )
        .await?;

        // Update the window.
        let mut sessions = add_partitions_to_session_windows(partitions, &mut windows, timeout)?;
        let to_remove = find_timeout_session_windows(&windows, timeout, time)?;
        to_remove.iter().for_each(|bidder| {
            sessions.push(windows.remove(bidder).unwrap());
        });

        let tasks = sessions
            .into_iter()
            .filter(|session| !session.is_empty())
            .map(|session| {
                let function_group = group_name.clone();
                let invoke_type = invocation_type.clone();

                let query_code = group_name.split('-').next().unwrap();
                let timestamp = Utc::now().timestamp();
                let rand_id = uuid::Uuid::new_v4().as_u128();
                let tid = format!("{}-{}-{}", query_code, timestamp, rand_id);

                // Distribute the window data to a single function execution environment.
                let function_name = ring.get(&tid).expect("hash ring failure.").to_string();
                info!("Session window -> function name: {}", function_name);

                tokio::spawn(async move {
                    let output = repartition(session, RoundRobinBatch(1)).await?;
                    let window = coalesce_batches(output, granule_size * 2).await?;
                    let size = window[0].len();
                    let mut uuid_builder =
                        UuidBuilder::new_with_ts_uuid(&function_group, timestamp, rand_id, size);

                    // Call the next stage of the dataflow graph.
                    info!(
                        "[OK] Send {} events from a session window to function: {}.",
                        size, function_name
                    );

                    for (eid, partition) in window.iter().enumerate() {
                        let payload = serde_json::to_vec(&to_payload(
                            partition,
                            &[],
                            uuid_builder.next_uuid(),
                            sync,
                        ))?;
                        info!(
                            "[OK] Event {} - function payload bytes: {}",
                            eid,
                            payload.len()
                        );
                        invoke_lambda_function(
                            function_name.clone(),
                            Some(payload.into()),
                            invoke_type.clone(),
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .collect::<Vec<tokio::task::JoinHandle<Result<()>>>>();
        futures::future::join_all(tasks).await;
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
    let (mut ring, group_name) = infer_actor_info(&payload.metadata)?;
    let sync = infer_invocation_type(&payload.metadata)?;
    let invocation_type = if sync {
        FLOCK_LAMBDA_SYNC_CALL.to_string()
    } else {
        FLOCK_LAMBDA_ASYNC_CALL.to_string()
    };

    for epoch in 0..seconds {
        info!("[OK] Send events (epoch: {}).", epoch);
        let events = stream.clone();
        if ring.len() == 1 {
            // lambda default concurrency is 1000.
            let function_name = group_name.clone();
            let uuid =
                UuidBuilder::new_with_ts(&function_name, Utc::now().timestamp(), 1).next_uuid();
            let payload = serde_json::to_vec(&events.select_event_to_payload(
                epoch,
                0,
                payload.query_number,
                uuid,
                sync,
            )?)?;
            info!("[OK] function payload bytes: {}", payload.len());
            invoke_lambda_function(function_name, Some(payload.into()), invocation_type.clone())
                .await?;
        } else {
            // Calculate the total data packets to be sent.
            // transfrom tuple (a, b) to (Arc::new(a), Arc::new(b))
            let (a, b) = events.select_event_to_batches(
                epoch,
                0, // generator id
                payload.query_number,
                sync,
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

            let empty = vec![];
            for i in 0..size {
                let payload = serde_json::to_vec(&to_payload(
                    if i < a.len() { &a[i] } else { &empty },
                    if i < b.len() { &b[i] } else { &empty },
                    uuid_builder.next_uuid(),
                    sync,
                ))?;
                info!(
                    "[OK] Event {} - function payload bytes: {}",
                    i,
                    payload.len()
                );
                invoke_lambda_function(
                    function_name.clone(),
                    Some(payload.into()),
                    invocation_type.clone(),
                )
                .await?;
            }
        }
    }

    Ok(())
}
