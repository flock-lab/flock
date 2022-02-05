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
use crate::{consistent_hash_context, ConsistentHashContext, CONSISTENT_HASH_CONTEXT};
use chrono::Utc;
use flock::aws::lambda;
use flock::prelude::*;
use log::{info, warn};
use std::sync::Arc;

/// Generate hopping windows workloads for the benchmark on cloud
/// function services.
///
/// # Arguments
/// * `payload` - The payload of the function.
/// * `stream` - the source stream of events.
/// * `seconds` - the total number of seconds to generate workloads.
/// * `window_size` - the size of the window in seconds.
/// * `hop_size` - the size of the hop in seconds.
pub async fn launch_tasks(
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

    let (ring, group_name) = consistent_hash_context!();
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

        let mut uuid_builder = UuidBuilder::new_with_ts(group_name, Utc::now().timestamp(), size);

        // Distribute the window data to a single function execution environment.
        let function_name = ring
            .get(&uuid_builder.qid)
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
                    "[OK] Event {} - {} function's payload bytes: {}",
                    eid,
                    function_name,
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
