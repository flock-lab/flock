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
use datafusion::physical_plan::empty::EmptyExec;
use flock::aws::{lambda, s3};
use flock::prelude::*;
use log::info;
use std::sync::Arc;

/// Generate normal elementwose workloads for the benchmark on cloud
/// function services.
///
/// # Arguments
/// * `payload` - The payload of the function.
/// * `stream` - the source stream of events.
/// * `seconds` - the total number of seconds to generate workloads.
pub async fn launch_tasks(
    ctx: &mut ExecutionContext,
    payload: Payload,
    stream: Arc<dyn DataStream + Send + Sync>,
    seconds: usize,
) -> Result<()> {
    let query_number = payload.query_number;
    let metadata = payload.metadata;
    let (ring, group_name) = consistent_hash_context!();
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
            assert!(!ctx.plan.execution_plans.is_empty());
            let exec_plans = &ctx.plan.execution_plans;
            if exec_plans[0].as_any().downcast_ref::<EmptyExec>().is_some() {
                // centralized mode
                let function_name = group_name.clone();
                let uuid =
                    UuidBuilder::new_with_ts(&function_name, Utc::now().timestamp(), 1).next_uuid();
                let mut payload =
                    events.select_event_to_payload(epoch, 0, query_number, uuid, sync)?;
                payload.metadata = metadata.clone();
                let bytes = serde_json::to_vec(&payload)?;
                info!(
                    "[OK] {} function's payload bytes: {}",
                    function_name,
                    bytes.len()
                );
                lambda::invoke_function(&function_name, &invocation_type, Some(bytes.into()))
                    .await?;
            } else {
                // distributed mode
                let partitions = events.select_event_to_batches(
                    epoch,
                    0, // generator id
                    payload.query_number,
                    sync,
                )?;
                let mut input = vec![];
                for b in vec![partitions.0, partitions.1] {
                    if !b.is_empty() {
                        input.push(b);
                    }
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
                            payload.query_number = query_number;
                            payload.metadata = meta;

                            let bytes = serde_json::to_vec(&payload)?;
                            info!(
                                "[OK] {} function's payload bytes: {}",
                                function_name,
                                bytes.len()
                            );
                            lambda::invoke_function(
                                &function_name,
                                &invoke_type,
                                Some(bytes.into()),
                            )
                            .await
                            .map(|_| ())
                        })
                    })
                    .collect::<Vec<tokio::task::JoinHandle<Result<()>>>>();
                futures::future::join_all(tasks).await;
                ctx.clean_data_sources().await?;
            }
        } else {
            // Calculate the total data packets to be sent.
            let (a, b) = events.select_event_to_batches(
                epoch,
                0, // generator id
                payload.query_number,
                sync,
            )?;
            let size = if a.len() > b.len() { a.len() } else { b.len() };

            let mut uuid_builder =
                UuidBuilder::new_with_ts(group_name, Utc::now().timestamp(), size);

            // Distribute the epoch data to a single function execution environment.
            let function_name = ring
                .get(&uuid_builder.qid)
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
                info!(
                    "[OK] Event {} - {} function's payload bytes: {}",
                    i,
                    function_name,
                    bytes.len()
                );
                lambda::invoke_function(&function_name, &invocation_type, Some(bytes.into()))
                    .await?;
            }
        }
    }

    Ok(())
}
