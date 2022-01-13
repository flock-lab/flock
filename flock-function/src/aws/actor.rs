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

use datafusion::arrow::csv::reader::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Partitioning::RoundRobinBatch;
use flock::aws::s3;
use flock::prelude::*;
use hashring::HashRing;
use lazy_static::lazy_static;
use log::info;
use rayon::prelude::*;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;
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
) -> Result<Vec<Vec<RecordBatch>>> {
    let inputs = Arc::new(Mutex::new(vec![]));

    info!("Repartitioning the input data before execution.");
    let tasks = partitions
        .into_iter()
        .map(|batches| {
            let input = inputs.clone();
            tokio::spawn(async move {
                if !(batches.is_empty() || batches.iter().all(|r| r.is_empty())) {
                    if batches.len() != 1 {
                        let output = repartition(batches, RoundRobinBatch(1)).await?;
                        assert_eq!(1, output.len());
                        info!(
                            "Input record size: {}.",
                            output[0].par_iter().map(|r| r.num_rows()).sum::<usize>()
                        );
                        let output = coalesce_batches(output, 1024).await?;
                        let output = repartition(output, RoundRobinBatch(16)).await?;
                        input.lock().unwrap().push(output);
                    } else {
                        info!(
                            "Input record size: {}.",
                            batches[0].par_iter().map(|r| r.num_rows()).sum::<usize>()
                        );
                        let output = repartition(batches, RoundRobinBatch(16)).await?;
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
        if !output.is_empty() && !output[0].is_empty() {
            info!("[Ok] Output schema: {:?}", output[0][0].schema());
            info!("[Ok] Output row count: {}", output[0][0].num_rows());
        }
        Ok(output)
    }
}

/// Read the payload from S3 via the S3 bucket and the key.
async fn read_payload_from_s3(bucket: String, key: String) -> Result<Payload> {
    let body = s3::get_object(&bucket, &key).await?;
    let payload: Payload = serde_json::from_slice(&body)?;
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

    let mut input = vec![];
    if let Ok(batch) = infer_side_input(&event.metadata).await {
        input.push(vec![batch]);
    }

    if let Some((bucket, key)) = infer_s3_mode(&event.metadata) {
        info!("Reading payload from S3...");
        let payload = read_payload_from_s3(bucket, key).await?;
        info!("[OK] Received payload from S3.");

        info!("Parsing payload to input partitions...");
        let (r1, r2, _) = payload.to_record_batch();
        info!("[OK] Parsed payload.");

        input.push(vec![r1]);
        input.push(vec![r2]);
    } else if match &ctx.next {
        CloudFunction::Sink(..) | CloudFunction::Lambda(..) => true,
        CloudFunction::Group(..) => false,
    } {
        // ressemble data packets to a single window.
        let (ready, uuid) = arena.reassemble(event);
        if ready {
            info!("Received all data packets for the window: {:?}", uuid.tid);
            arena
                .batches(uuid.tid)
                .into_iter()
                .for_each(|b| input.push(b));
        } else {
            let response = "Window data collection has not been completed.".to_string();
            info!("{}", response);
            return Ok(json!({ "response": response }));
        }
    } else {
        // data packet is an individual event for the current function.
        let (r1, r2, _) = event.to_record_batch();
        input.push(vec![r1]);
        input.push(vec![r2]);
    }

    let output = collect(ctx, input).await?;

    match &ctx.next {
        CloudFunction::Sink(sink_type) => {
            info!("[Ok] Sinking data to {:?}", sink_type);
            let output = output.into_iter().flatten().collect::<Vec<_>>();
            if !output.is_empty() && DataSinkType::Blackhole != *sink_type {
                DataSink::new(ctx.name.clone(), output, Encoding::default())
                    .write(sink_type.clone(), DataSinkFormat::SerdeBinary)
                    .await
            } else {
                Ok(json!({ "response": "No data to sink." }))
            }
        }
        _ => {
            let mut batches = coalesce_batches(
                output,
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
fn invoke_next_functions(_: &ExecutionContext, _: &mut Vec<RecordBatch>) -> Result<()> {
    unimplemented!();
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

pub async fn infer_side_input(
    metadata: &Option<HashMap<String, String>>,
) -> Result<Vec<RecordBatch>> {
    if let Some(metadata) = metadata {
        if let Some(key) = metadata.get("side_input_s3_key") {
            let key = key.parse::<String>().unwrap();
            let bytes = s3::get_object(&FLOCK_S3_BUCKET, &key).await?;

            let format = metadata
                .get("side_input_format")
                .expect("side_input_format is missing")
                .as_str();

            let schema = schema_from_bytes(&base64::decode(
                metadata
                    .get("side_input_schema")
                    .expect("side_input_schema is missing")
                    .as_str(),
            )?)?;

            let mut batches = vec![];
            match format {
                "csv" => {
                    let mut batch_reader = ReaderBuilder::new()
                        .with_schema(schema)
                        .has_header(true)
                        .with_delimiter(b',')
                        .with_batch_size(1024)
                        .build(Cursor::new(bytes))?;
                    loop {
                        match batch_reader.next() {
                            Some(Ok(batch)) => {
                                batches.push(batch);
                            }
                            None => {
                                break;
                            }
                            Some(Err(e)) => {
                                return Err(FlockError::Execution(format!(
                                    "Error reading batch from side input: {}",
                                    e
                                )));
                            }
                        }
                    }
                }
                _ => unimplemented!(),
            }
            return Ok(batches);
        }
    }
    Err(FlockError::AWS(
        "Side Input's S3 key is not specified".to_string(),
    ))
}

/// Infer group keys for session windows (used in NEXMark Q11 and Q12).
pub fn infer_session_keys(metadata: &Option<HashMap<String, String>>) -> Result<(String, String)> {
    if let Some(metadata) = metadata {
        if let (Some(key), Some(name)) = (metadata.get("session_key"), metadata.get("session_name"))
        {
            let key = key.parse::<String>().unwrap();
            let name = name.parse::<String>().unwrap();
            if !key.is_empty() && !name.is_empty() {
                return Ok((key, name));
            }
        }
    }
    Err(FlockError::Internal(
        "Failed to infer session group key.".to_string(),
    ))
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

/// This function is only used for NEXMark Q12 to add the process time field to
/// the input data.
pub fn infer_add_process_time_query(metadata: &Option<HashMap<String, String>>) -> Result<String> {
    if let Some(metadata) = metadata {
        if let Some(plan) = metadata.get("add_process_time_query") {
            let plan = plan.parse::<String>().unwrap();
            return Ok(plan);
        }
    }
    Err(FlockError::Execution(
        "Failed to infer plan for adding process time field to the input data.".to_string(),
    ))
}
