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
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::arrow::array::{
    Int32Array, TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext as DataFusionExecutionContext;
use datafusion::logical_plan::{col, count_distinct};
use datafusion::physical_plan::collect_partitioned;
use datafusion::physical_plan::expressions::col as expr_col;
use datafusion::physical_plan::Partitioning::{HashDiff, RoundRobinBatch};
use flock::aws::lambda;
use flock::datasource::nexmark::config::BASE_TIME;
use flock::prelude::*;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

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
    if seconds < window_size {
        warn!(
            "seconds: {} is less than window_size: {}",
            seconds, window_size
        );
    }
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
                lambda::invoke_function(&function_name, &invocation_type, Some(payload.into()))
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
    if seconds < window_size {
        warn!(
            "seconds: {} is less than window_size: {}",
            seconds, window_size
        );
    }
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
                lambda::invoke_function(&function_name, &invocation_type, Some(payload.into()))
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
    ctx: &mut DataFusionExecutionContext,
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
    if seconds < timeout {
        warn!("seconds: {} is less than timeout: {}", seconds, timeout);
    }
    let sync = infer_invocation_type(&payload.metadata)?;
    let (group_key, table_name) = infer_session_keys(&payload.metadata)?;
    let (mut ring, group_name) = infer_actor_info(&payload.metadata)?;

    let (invocation_type, granule_size) = if sync {
        (FLOCK_LAMBDA_SYNC_CALL.to_string(), *FLOCK_SYNC_GRANULE_SIZE)
    } else {
        (
            FLOCK_LAMBDA_ASYNC_CALL.to_string(),
            *FLOCK_ASYNC_GRANULE_SIZE,
        )
    };

    let mut ctx = DataFusionExecutionContext::new();
    let mut windows: HashMap<usize, Vec<Vec<RecordBatch>>> = HashMap::new();

    let events = (0..seconds)
        .map(|t| {
            let (r1, _) = stream
                .select_event_to_batches(
                    t,
                    0, // generator id
                    payload.query_number,
                    sync,
                )
                .unwrap();
            r1.to_vec()
        })
        .collect::<Vec<Vec<Vec<RecordBatch>>>>();

    let schema = events[0][0][0].schema();

    for (time, batches) in events.into_iter().enumerate() {
        info!("Processing events in epoch: {}", time);
        let table = MemTable::try_new(schema.clone(), batches)?;
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
            HashDiff(vec![expr_col(&group_key, &schema)?], distinct_keys as usize),
        )
        .await?;

        // Update the window.
        let mut sessions = add_partitions_to_session_windows(partitions, &mut windows, timeout)?;
        let to_remove = find_timeout_session_windows(&windows, timeout, time)?;
        to_remove.iter().for_each(|bidder| {
            sessions.push(windows.remove(bidder).unwrap());
        });

        let tasks = coalesce_windows(sessions, granule_size)?
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
                        lambda::invoke_function(&function_name, &invoke_type, Some(payload.into()))
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

/// Add each unique partition to a distinct tumbling window.
///
/// # Arguments
/// * `partitions` - the partitions to be added.
/// * `windows` - the current tumbling windows.
/// * `window_size` - the size of the window.
///
/// # Return
/// The updated session windows.
fn add_partitions_to_tumbling_windows(
    partitions: Vec<Vec<RecordBatch>>,
    windows: &mut HashMap<usize, Vec<Vec<RecordBatch>>>,
    window_size: usize,
) -> Result<Vec<Vec<Vec<RecordBatch>>>> {
    Ok(partitions
        .into_iter()
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut window = vec![];
            let bidder = p[0]
                .column(1 /* bidder field */)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0);
            let current_timestamp = p[0]
                .column(4 /* p_time field */)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0);
            let current_process_time = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(current_timestamp / 1000 / 1000 / 1000, 0),
                Utc,
            );

            if !windows.contains_key(&(bidder as usize)) {
                windows
                    .entry(bidder as usize)
                    .or_insert_with(Vec::new)
                    .push(p);
            } else {
                // get the first batch in the window.
                let batch = windows
                    .get(&(bidder as usize))
                    .unwrap()
                    .first()
                    .unwrap()
                    .first()
                    .unwrap();
                // get the first bid's process time.
                let first_timestamp = batch
                    .column(4 /* p_time field */)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0);
                let first_process_time = DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(first_timestamp / 1000 / 1000 / 1000, 0),
                    Utc,
                );

                // If the current process time isn't 10 seconds later than the beginning of
                // the entry, then we can add the current batch to the map. Otherwise, we
                // have a new tumbling window.
                if current_process_time.signed_duration_since(first_process_time)
                    > chrono::Duration::seconds(window_size as i64)
                {
                    window = windows.remove(&(bidder as usize)).unwrap();
                }
                windows
                    .entry(bidder as usize)
                    .or_insert_with(Vec::new)
                    .push(p);
            }
            window
        })
        .collect::<Vec<Vec<Vec<RecordBatch>>>>())
}

/// Find new tumbling windows after timeout.
///
/// # Arguments
/// * `windows` - the tumbling windows.
/// * `timeout` - the tumbling timeout.
///
/// # Return
/// The keys of the new tumbling windows.
fn find_timeout_tumbling_windows(
    windows: &HashMap<usize, Vec<Vec<RecordBatch>>>,
    timeout: usize,
) -> Result<Vec<usize>> {
    let mut to_remove = vec![];

    windows.iter().for_each(|(bidder, batches)| {
        // get the first bid's prcessing time
        let first_timestamp = batches[0][0]
            .column(4 /* p_time field */)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);

        let first_process_time = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(first_timestamp / 1000 / 1000 / 1000, 0),
            Utc,
        );
        if Utc::now().signed_duration_since(first_process_time)
            > chrono::Duration::seconds(timeout as i64)
        {
            to_remove.push(bidder.to_owned());
        }
    });

    Ok(to_remove)
}

/// This function is used to coalesce smaller session windows or global windows
/// to bigger ones so that the number of events in each payload is greater than
/// the granule size, and close to the payload limit.
fn coalesce_windows(
    windows: Vec<Vec<Vec<RecordBatch>>>,
    granule_size: usize,
) -> Result<Vec<Vec<Vec<RecordBatch>>>> {
    #[allow(unused_assignments)]
    let mut curr_size = 0;
    let mut total_size = 0;
    let mut res = vec![];
    let mut tmp = vec![];
    for window in windows {
        curr_size = window
            .iter()
            .map(|v| v.iter().map(|b| b.num_rows()).sum::<usize>())
            .sum::<usize>();
        total_size += curr_size;
        if total_size <= granule_size * 2 || tmp.is_empty() {
            tmp.push(window);
        } else {
            res.push(tmp.into_iter().flatten().collect::<Vec<Vec<RecordBatch>>>());
            tmp = vec![window];
            total_size = curr_size;
        }
    }
    if !tmp.is_empty() {
        res.push(tmp.into_iter().flatten().collect::<Vec<Vec<RecordBatch>>>());
    }
    Ok(res)
}

/// A global windows assigner assigns all elements with the same key to the same
/// single global window. This windowing scheme is only useful if you also
/// specify a custom trigger. Otherwise, no computation will be performed, as
/// the global window does not have a natural end at which we could process the
/// aggregated elements.
///
/// # Arguments
/// * `payload` - The payload of the function invocation.
/// * `stream` - The data stream.
/// * `seconds` - The number of seconds to group events into.
/// * `window_size` - The size of the window.
pub async fn global_window_tasks(
    payload: Payload,
    stream: Arc<dyn DataStream>,
    seconds: usize,
    window_size: usize,
) -> Result<()> {
    if seconds < window_size {
        warn!(
            "seconds: {} is less than window_size: {}",
            seconds, window_size
        );
    }
    let sync = infer_invocation_type(&payload.metadata)?;
    let (group_key, table_name) = infer_session_keys(&payload.metadata)?;
    let (mut ring, group_name) = infer_actor_info(&payload.metadata)?;
    let add_process_time_sql = infer_add_process_time_query(&payload.metadata)?;

    let (invocation_type, granule_size) = if sync {
        (FLOCK_LAMBDA_SYNC_CALL.to_string(), *FLOCK_SYNC_GRANULE_SIZE)
    } else {
        (
            FLOCK_LAMBDA_ASYNC_CALL.to_string(),
            *FLOCK_ASYNC_GRANULE_SIZE,
        )
    };

    let mut ctx = DataFusionExecutionContext::new();
    let mut windows: HashMap<usize, Vec<Vec<RecordBatch>>> = HashMap::new();

    let events = (0..seconds)
        .map(|t| {
            let (r1, _) = stream
                .select_event_to_batches(
                    t,
                    0, // generator id
                    payload.query_number,
                    sync,
                )
                .unwrap();
            r1.to_vec()
        })
        .collect::<Vec<Vec<Vec<RecordBatch>>>>();

    let schema = events[0][0][0].schema();

    for (time, batches) in events.into_iter().enumerate() {
        info!("Processing events in epoch: {}", time);
        let now = Instant::now();
        let table = MemTable::try_new(schema.clone(), batches)?;
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

        // Equivalent to `SELECT *, now() as p_time FROM table_name;`
        let output = collect_partitioned(physical_plan(&ctx, &add_process_time_sql).await?).await?;
        let schema_with_ptime = output[0][0].schema();

        // Each partition has a unique key after `repartition` execution.
        let partitions = repartition(
            output,
            HashDiff(
                vec![expr_col(&group_key, &schema_with_ptime)?],
                distinct_keys as usize,
            ),
        )
        .await?;

        // Update the window.
        let mut tumblings =
            add_partitions_to_tumbling_windows(partitions, &mut windows, window_size)?;
        let to_remove = find_timeout_tumbling_windows(&windows, window_size)?;
        to_remove.iter().for_each(|bidder| {
            tumblings.push(windows.remove(bidder).unwrap());
        });

        let tasks = coalesce_windows(tumblings, granule_size)?
            .into_iter()
            .filter(|window| !window.is_empty())
            .map(|window| {
                let function_group = group_name.clone();
                let invoke_type = invocation_type.clone();

                let query_code = group_name.split('-').next().unwrap();
                let timestamp = Utc::now().timestamp();
                let rand_id = uuid::Uuid::new_v4().as_u128();
                let tid = format!("{}-{}-{}", query_code, timestamp, rand_id);

                // Distribute the window data to a single function execution environment.
                let function_name = ring.get(&tid).expect("hash ring failure.").to_string();
                info!("Tumbling window -> function name: {}", function_name);

                tokio::spawn(async move {
                    let window = repartition(window, RoundRobinBatch(1)).await?;
                    let window = coalesce_batches(window, granule_size * 2).await?;
                    let size = window[0].len();
                    let mut uuid_builder =
                        UuidBuilder::new_with_ts_uuid(&function_group, timestamp, rand_id, size);

                    // Call the next stage of the dataflow graph.
                    info!(
                        "[OK] Send {} events from a tumbling window to function: {}.",
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
                        lambda::invoke_function(&function_name, &invoke_type, Some(payload.into()))
                            .await?;
                    }
                    Ok(())
                })
            })
            .collect::<Vec<tokio::task::JoinHandle<Result<()>>>>();
        futures::future::join_all(tasks).await;

        let elapsed = now.elapsed().as_millis() as u64;
        if elapsed < 1000 {
            std::thread::sleep(std::time::Duration::from_millis(1000 - elapsed));
        }
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
    let query_number = payload.query_number;
    let metadata = payload.metadata;
    let (mut ring, group_name) = infer_actor_info(&metadata)?;
    let sync = infer_invocation_type(&metadata)?;
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
            let mut payload = events.select_event_to_payload(epoch, 0, query_number, uuid, sync)?;
            payload.metadata = metadata.clone();
            let bytes = serde_json::to_vec(&payload)?;
            info!("[OK] function payload bytes: {}", bytes.len());
            lambda::invoke_function(&function_name, &invocation_type, Some(bytes.into())).await?;
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
                let mut payload = to_payload(
                    if i < a.len() { &a[i] } else { &empty },
                    if i < b.len() { &b[i] } else { &empty },
                    uuid_builder.next_uuid(),
                    sync,
                );
                payload.query_number = query_number;
                payload.metadata = metadata.clone();

                let bytes = serde_json::to_vec(&payload)?;
                info!("[OK] Event {} - function payload bytes: {}", i, bytes.len());
                lambda::invoke_function(&function_name, &invocation_type, Some(bytes.into()))
                    .await?;
            }
        }
    }

    Ok(())
}
