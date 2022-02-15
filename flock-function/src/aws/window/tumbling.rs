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

use super::is_distributed;
use crate::actor::*;
use crate::{consistent_hash_context, ConsistentHashContext, CONSISTENT_HASH_CONTEXT};
use chrono::Utc;
use flock::aws::{lambda, s3};
use flock::prelude::*;
use log::{info, warn};
use std::sync::Arc;

/// Generate tumble windows workloads for the benchmark on cloud
/// function services.
///
/// # Arguments
/// * `payload` - The payload of the function.
/// * `stream` - the source stream of events.
/// * `seconds` - the total number of seconds to generate workloads.
/// * `window_size` - the size of the window in seconds.
pub async fn launch_tasks(
    ctx: &mut ExecutionContext,
    payload: Payload,
    stream: Arc<dyn DataStream + Send + Sync>,
    seconds: usize,
    window_size: usize,
) -> Result<()> {
    if seconds < window_size {
        warn!(
            "seconds: {} is less than window_size: {}",
            seconds, window_size
        );
    }
    let metadata = payload.metadata;
    let (ring, group_name) = consistent_hash_context!();
    let sync = infer_invocation_type(&metadata)?;
    let invocation_type = if sync {
        FLOCK_LAMBDA_SYNC_CALL.to_string()
    } else {
        FLOCK_LAMBDA_ASYNC_CALL.to_string()
    };

    let mut window: Box<Vec<(RelationPartitions, RelationPartitions)>> = Box::new(vec![]);

    for time in 0..seconds / window_size {
        let start = time * window_size;
        let end = start + window_size;

        if is_distributed(ctx) {
            // Distribute the workloads to the cloud function services.
            let mut input1 = vec![];
            let mut input2 = vec![];
            for t in start..end {
                let (r1, r2) = stream.select_event_to_batches(t, 0, None, sync)?;
                if !r1.is_empty() {
                    input1.push(r1);
                }
                if !r2.is_empty() {
                    input2.push(r2);
                }
            }
            let mut input = vec![];
            if !input1.is_empty() {
                input.push(input1.into_iter().flatten().collect());
            }
            if !input2.is_empty() {
                input.push(input2.into_iter().flatten().collect());
            }

            ctx.feed_data_sources(input).await?;
            let output = Arc::new(ctx.execute_partitioned().await?);
            let size = output[0].len();
            let mut uuid_builder =
                UuidBuilder::new_with_ts(group_name, Utc::now().timestamp(), size);

            // Creates the S3 bucket for the current query if state backend is S3.
            if ctx
                .state_backend
                .as_any()
                .downcast_ref::<S3StateBackend>()
                .is_some()
            {
                s3::create_bucket(&uuid_builder.qid).await?;
            }

            let tasks = (0..size)
                .map(|i| {
                    let data = output.clone();
                    let function_name = group_name.clone();
                    let meta = metadata.clone();
                    let invoke_type = invocation_type.clone();
                    let uuid = uuid_builder.next_uuid();
                    tokio::spawn(async move {
                        let mut payload = to_payload(
                            &data[0][i],
                            if data.len() == 1 { &[] } else { &data[1][i] },
                            uuid,
                            sync,
                        );
                        payload.metadata = meta;

                        let bytes = serde_json::to_vec(&payload)?;
                        info!(
                            "[OK] {} function's payload bytes: {}",
                            function_name,
                            bytes.len()
                        );
                        lambda::invoke_function(&function_name, &invoke_type, Some(bytes.into()))
                            .await
                            .map(|_| ())
                    })
                })
                .collect::<Vec<tokio::task::JoinHandle<Result<()>>>>();
            futures::future::join_all(tasks).await;
            ctx.clean_data_sources().await?;
        } else {
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

            let mut uuid_builder =
                UuidBuilder::new_with_ts(group_name, Utc::now().timestamp(), size);

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
                        "[OK] Event {} - {} function payload bytes: {}",
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
    }

    Ok(())
}
