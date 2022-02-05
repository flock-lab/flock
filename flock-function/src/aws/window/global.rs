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

use super::coalesce_windows;
use crate::actor::*;
use crate::{consistent_hash_context, ConsistentHashContext, CONSISTENT_HASH_CONTEXT};
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::arrow::array::{Int32Array, TimestampNanosecondArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext as DataFusionExecutionContext;
use datafusion::logical_plan::{col, count_distinct};
use datafusion::physical_plan::collect_partitioned;
use datafusion::physical_plan::expressions::col as expr_col;
use datafusion::physical_plan::Partitioning::{HashDiff, RoundRobinBatch};
use flock::aws::lambda;
use flock::prelude::*;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

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
pub async fn launch_tasks(
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
    let add_process_time_sql = infer_add_process_time_query(&payload.metadata)?;
    let (ring, group_name) = consistent_hash_context!();

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
                let qid = format!("{}-{}-{}", query_code, timestamp, rand_id);

                // Distribute the window data to a single function execution environment.
                let function_name = ring.get(&qid).expect("hash ring failure.").to_string();
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
                            "[OK] Event {} - {} function's payload bytes: {}",
                            eid,
                            function_name,
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
