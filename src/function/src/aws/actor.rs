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

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Partitioning::RoundRobinBatch;
use driver::deploy::common::*;
use futures::executor::block_on;
use hashring::HashRing;
use lazy_static::lazy_static;
use log::{info, warn};
use rayon::prelude::*;
use runtime::executor::LambdaExecutor as executor;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{InvokeAsyncRequest, Lambda, LambdaClient};
use rusoto_s3::{GetObjectRequest, S3};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};

lazy_static! {
    static ref CONCURRENCY: usize = FLOCK_CONF["lambda"]["concurrency"]
        .parse::<usize>()
        .unwrap();
}

/// The generic function executor.
///
/// This function is invoked by the datafusion runtime. It is responsible for
/// executing the physical plan. It is also responsible for collecting the
/// results of the execution. After the execution is finished, the results are
/// written to the output. The results are written to the output in the form of
/// Arrow RecordBatch.
///
/// ## Arguments
/// * `ctx` - The runtime context of the function.
/// * `r1_records` - The input record batches for the first relation.
/// * `r2_records` - The input record batches for the second relation.
///
/// ## Returns
/// A vector of Arrow RecordBatch.
pub async fn collect(
    ctx: &mut ExecutionContext,
    partitions: Vec<Vec<Vec<RecordBatch>>>,
) -> Result<Vec<RecordBatch>> {
    let inputs = Arc::new(Mutex::new(vec![]));

    info!("Repartitioning the input data before execution.");
    let tasks = partitions
        .into_iter()
        .map(|batches| {
            let input = inputs.clone();
            tokio::spawn(async move {
                if !(batches.is_empty() || batches.iter().all(|r| r.is_empty())) {
                    if batches.len() != 1 {
                        let output = executor::repartition(batches, RoundRobinBatch(1)).await?;
                        assert_eq!(1, output.len());
                        info!(
                            "Input record size: {}.",
                            output[0].par_iter().map(|r| r.num_rows()).sum::<usize>()
                        );
                        let output = executor::coalesce_batches(output, 1024).await?;
                        let output = executor::repartition(output, RoundRobinBatch(16)).await?;
                        input.lock().unwrap().push(output);
                    } else {
                        info!(
                            "Input record size: {}.",
                            batches[0].par_iter().map(|r| r.num_rows()).sum::<usize>()
                        );
                        let output = executor::repartition(batches, RoundRobinBatch(16)).await?;
                        input.lock().unwrap().push(output);
                    }
                }
                Ok(())
            })
        })
        .collect::<Vec<tokio::task::JoinHandle<Result<()>>>>();

    futures::future::join_all(tasks).await;

    let input_partitions = Arc::try_unwrap(inputs).unwrap().into_inner().unwrap();

    info!("Executing the physical plan.");
    if input_partitions.is_empty() {
        Ok(vec![])
    } else {
        ctx.feed_data_sources(&input_partitions).await?;
        let output = ctx.execute().await?;
        info!("[OK] The execution is finished.");
        if !output.is_empty() {
            info!("[Ok] Output schema: {:?}", output[0].schema());
            info!("[Ok] Output row count: {}", output[0].num_rows());
        }
        Ok(output)
    }
}

/// Read the payload from S3 via the S3 bucket and the key.
async fn read_payload_from_s3(bucket: String, key: String) -> Result<Payload> {
    let body = FLOCK_S3_CLIENT
        .get_object(GetObjectRequest {
            bucket,
            key,
            ..Default::default()
        })
        .await
        .map_err(|e| FlockError::AWS(e.to_string()))?
        .body
        .take()
        .expect("body is empty");
    let payload: Payload = tokio::task::spawn_blocking(move || {
        let mut buf = Vec::new();
        body.into_blocking_read().read_to_end(&mut buf).unwrap();
        serde_json::from_slice(&buf).unwrap()
    })
    .await
    .expect("failed to load payload from S3");
    Ok(payload)
}

/// The endpoint for worker function invocations. The worker function
/// invocations are invoked by the data source generator or the former stage of
/// the dataflow pipeline.
///
/// # Arguments
/// * `ctx` - The runtime context of the function.
/// * `arena` - The global memory arena for the function across invocations.
/// * `payload` - The payload of the function invocation.
///
/// # Returns
/// A JSON object that contains the return value of the function invocation.
pub async fn handler(
    ctx: &mut ExecutionContext,
    arena: &mut Arena,
    event: Payload,
) -> Result<Value> {
    info!("Receiving a data packet: {:?}", event.uuid);
    let tid = event.uuid.tid.clone();

    let input_partitions = {
        if let Some((bucket, key)) = infer_s3_mode(&event.metadata) {
            info!("Reading payload from S3...");
            let payload = read_payload_from_s3(bucket, key).await?;
            info!("[OK] Received payload from S3.");

            info!("Parsing payload to input partitions...");
            let (r1, r2, _) = payload.to_record_batch();
            info!("[OK] Parsed payload.");

            vec![vec![r1], vec![r2]]
        } else if match &ctx.next {
            CloudFunction::Sink(..) | CloudFunction::Lambda(..) => true,
            CloudFunction::Group(..) => false,
        } {
            // ressemble data packets to a single window.
            let (ready, uuid) = arena.reassemble(event);
            if ready {
                info!("Received all data packets for the window: {:?}", uuid.tid);
                arena.batches(uuid.tid)
            } else {
                let response = "Window data collection has not been completed.".to_string();
                info!("{}", response);
                return Ok(json!({ "response": response }));
            }
        } else {
            // data packet is an individual event for the current function.
            let (r1, r2, _) = event.to_record_batch();
            vec![vec![r1], vec![r2]]
        }
    };

    let output = collect(ctx, input_partitions).await?;

    match &ctx.next {
        CloudFunction::Sink(sink_type) => {
            info!("[Ok] Sinking data to {:?}", sink_type);
            if !output.is_empty() && DataSinkType::Empty != *sink_type {
                DataSink::new(ctx.name.clone(), output, Encoding::default())
                    .write(sink_type.clone())
                    .await
            } else {
                Ok(json!({ "response": "No data to sink." }))
            }
        }
        _ => {
            let mut batches = executor::coalesce_batches(
                vec![output],
                FLOCK_CONF["lambda"]["payload_batch_size"]
                    .parse::<usize>()
                    .unwrap(),
            )
            .await?;
            assert_eq!(1, batches.len());
            // call the next stage of the dataflow graph.
            invoke_next_functions(ctx, &mut batches[0])?;
            Ok(json!({"name": &ctx.name, "tid": tid}))
        }
    }
}

/// Invoke functions in the next stage of the data flow.
fn invoke_next_functions(ctx: &ExecutionContext, batches: &mut Vec<RecordBatch>) -> Result<()> {
    // retrieve the next lambda function names
    let next_func = LambdaExecutor::next_function(ctx)?;

    // create uuid builder to assign id to each payload
    let uuid_builder = UuidBuilder::new(&ctx.name, batches.len());

    let client = &LambdaClient::new(Region::default());
    batches.into_par_iter().enumerate().for_each(|(i, batch)| {
        // call the lambda function asynchronously until it succeeds.
        loop {
            let uuid = uuid_builder.get(i);
            let request = InvokeAsyncRequest {
                function_name: next_func.clone(),
                invoke_args:   to_bytes(batch, uuid, Encoding::default()),
            };

            if let Ok(reponse) = block_on(client.invoke_async(request)) {
                if let Some(code) = reponse.status {
                    // A success response (202 Accepted) indicates that the request
                    // is queued for invocation.
                    if code == 202 {
                        break;
                    } else {
                        warn!("Unknown invoke error: {}, retry ... ", code);
                    }
                }
            }
        }
    });

    Ok(())
}

/// Infer the invocation mode of the function.
pub fn infer_invocation_type(metadata: &Option<HashMap<String, String>>) -> Result<bool> {
    let mut sync = true;
    if let Some(metadata) = metadata {
        if let Some(invocation_type) = metadata.get("invocation_type") {
            if invocation_type.parse::<String>().unwrap() == "async" {
                sync = false;
            }
        }
    }
    Ok(sync)
}

/// Infer the S3 communucation mode of the function.
pub fn infer_s3_mode(metadata: &Option<HashMap<String, String>>) -> Option<(String, String)> {
    if let Some(metadata) = metadata {
        if let (Some(bucket), Some(key)) = (metadata.get("s3_bucket"), metadata.get("s3_key")) {
            let bucket = bucket.parse::<String>().unwrap();
            let key = key.parse::<String>().unwrap();
            if !bucket.is_empty() && !key.is_empty() {
                return Some((bucket, key));
            }
        }
    }
    None
}

/// Infer the actor information for the function invocation.
pub fn infer_actor_info(
    metadata: &Option<HashMap<String, String>>,
) -> Result<(HashRing<String>, String)> {
    #[allow(unused_assignments)]
    let mut next_function = CloudFunction::default();
    if let Some(metadata) = metadata {
        next_function = serde_json::from_str(
            metadata
                .get(&"workers".to_string())
                .expect("workers is missing."),
        )?;
    } else {
        return Err(FlockError::Execution("metadata is missing.".to_owned()));
    }

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

    Ok((ring, group_name))
}
