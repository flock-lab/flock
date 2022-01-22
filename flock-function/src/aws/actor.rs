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

use crate::{consistent_hash_context, ConsistentHashContext, CONSISTENT_HASH_CONTEXT};
use chrono::Utc;
use datafusion::arrow::csv::reader::ReaderBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use flock::aws::lambda;
use flock::aws::s3;
use flock::prelude::*;
use lazy_static::lazy_static;
use log::info;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;
use std::time::Duration;

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
/// * `streams` - The input streams of the function.
///
/// ## Returns
/// The output stream of the function.
pub async fn collect(
    ctx: &mut ExecutionContext,
    streams: Vec<Vec<Vec<RecordBatch>>>,
) -> Result<Vec<Vec<RecordBatch>>> {
    let num_streams = streams.len();
    let input_streams = streams
        .into_iter()
        .filter(|r| {
            r.par_iter().any(|s| s.par_iter().any(|b| b.num_rows() > 0))
        })
        .collect::<Vec<_>>();

    info!("Executing the physical plan.");
    let output = if input_streams.is_empty() || input_streams.len() != num_streams {
        vec![]
    } else {
        ctx.feed_data_sources(input_streams).await?;
        if ctx.is_shuffling().await? {
            let output = ctx.execute_partitioned().await?;
            assert!(output.len() == 1);
            output.into_iter().next().unwrap()
        } else {
            ctx.execute().await?
        }
    };
    ctx.clean_data_sources().await?;
    info!("[OK] The execution is finished.");

    info!(
        "[INFO] The number of rows in the output is {}.",
        output
            .par_iter()
            .map(|s| s.par_iter().map(|b| b.num_rows()).sum::<usize>())
            .sum::<usize>()
    );

    Ok(output)
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

    let query_number = event.query_number;
    let metadata = event.metadata.clone();
    let uuid = event.uuid.clone();

    let input = prepare_data_sources(ctx, arena, event).await?;
    if input.is_empty() {
        let info = format!(
            "[Ok] Function {}: aggregation has not yet been completed.",
            ctx.name
        );
        info!("{}", info);
        return Ok(json!({ "response": info }));
    }

    let output = collect(ctx, input).await?;
    invoke_next_functions(ctx, query_number, uuid, metadata, output).await
}

/// Get the S3 key's prefix for the current query stage
fn s3_key_prefix(ctx: &ExecutionContext, event: &Payload) -> String {
    // function name format: <query code>-<plan index>-<group index>
    let mut name_parts = ctx.name.split('-');
    name_parts.next(); // skip the query code
    let plan_index = name_parts.next().unwrap();
    format!("{:02}/{:02}", plan_index, event.get_shuffle_id())
}

/// Prepare the data sources to the executor in the current function.
///
/// # Arguments
/// * `ctx` - The runtime context of the current function.
/// * `arena` - The global memory arena for the function across invocations.
/// * `event` - The payload of the current function invocation.
///
/// # Returns
/// The input data for the executor in the current function.
async fn prepare_data_sources(
    ctx: &mut ExecutionContext,
    arena: &mut Arena,
    event: Payload,
) -> Result<Vec<Vec<Vec<RecordBatch>>>> {
    let uuid = event.uuid.clone();
    let s3_key_prefix = s3_key_prefix(ctx, &event);
    let window_id = event.get_window_id();

    let mut input = vec![];
    if let Ok(batch) = infer_side_input(&event.metadata).await {
        input.push(vec![batch]);
    }

    if let Some((bucket, key)) = infer_s3_mode(&event.metadata) {
        info!("Reading payload from S3...");
        let payload = read_payload_from_s3(bucket, key).await?;
        info!("[OK] Received payload from S3.");

        info!("Parsing payload to input partitions...");
        let (r1, r2) = payload.to_record_batch();
        info!("[OK] Parsed payload.");

        input.push(vec![r1]);
        input.push(vec![r2]);
    } else if ctx.is_aggregate() {
        // ressemble data packets to a single window.
        if arena.collect(event) {
            info!("Received all data packets for the window: {:?}", window_id);
            arena
                .take_batches(&window_id)
                .into_iter()
                .for_each(|b| input.push(b));
        } else {
            // Aggregation has not yet been completed. We can also check the query states in
            // the corresponding S3 buckets. If some states exist in S3, Flock can bring the
            // states to the current function directly to reduce the query's latency. This
            // is because the aggregation states are not saved by its own, but are saved by
            // the former stage of the dataflow pipeline. Since aggregator's ancestors are
            // default Lambda functions with much higher concurrency, all of them can write
            // the partial aggregation states to the S3 buckets in parallel.
            if let Some(bitmap) = arena.get_bitmap(&window_id) {
                if ctx
                    .state_backend
                    .as_any()
                    .downcast_ref::<S3StateBackend>()
                    .is_some()
                {
                    let state_backend: &S3StateBackend = ctx
                        .state_backend
                        .as_any()
                        .downcast_ref::<S3StateBackend>()
                        .unwrap();
                    let keys = state_backend
                        .new_s3_keys(&uuid.qid, &s3_key_prefix, bitmap)
                        .await?;
                    if !keys.is_empty() {
                        state_backend
                            .read(uuid.qid.clone(), keys)
                            .await?
                            .into_iter()
                            .for_each(|payload| {
                                arena.collect(payload);
                            });
                        if arena.is_complete(&window_id) {
                            info!("Received all data packets for the window: {:?}", window_id);
                            arena
                                .take_batches(&window_id)
                                .into_iter()
                                .for_each(|b| input.push(b));
                        }
                    }
                }
            }

            return Ok(vec![]);
        }
    } else {
        // data packet is an individual event for the current function.
        let (r1, r2) = event.to_record_batch();
        input.push(vec![r1]);
        input.push(vec![r2]);
    }

    // If state backend is enabled, we need to check if the states are stored in the
    // S3 buckets before returning the input and starting the executor. If not, we
    // have to wait until the states are ready to make fault tolerance work.
    if ctx.is_aggregate()
        && ctx
            .state_backend
            .as_any()
            .downcast_ref::<S3StateBackend>()
            .is_some()
    {
        let state_backend: &S3StateBackend = ctx
            .state_backend
            .as_any()
            .downcast_ref::<S3StateBackend>()
            .unwrap();

        let mut retries = 0;
        loop {
            if state_backend
                .get_s3_key_num(&uuid.qid, &s3_key_prefix)
                .await?
                == uuid.seq_len
            {
                break;
            }
            // back-off strategy: exponential back-off withoout upper bound
            tokio::time::sleep(Duration::from_millis(2_u64.pow(retries) * 100)).await;
            retries += 1;
        }
    }

    Ok(input)
}

/// Invoke the next functions in the dataflow pipeline.
///
/// # Arguments
/// * `ctx` - The runtime context of the current function.
/// * `query_num` - The query number of the current request (for testing).
/// * `uuid` - The UUID of the current payload.
/// * `metadata` - The metadata of the current request.
/// * `output` - The output of the current function.
///
/// # Returns
/// A JSON object that contains the return value of the current function.
async fn invoke_next_functions(
    ctx: &mut ExecutionContext,
    query_number: Option<usize>,
    uuid: Uuid,
    metadata: Option<HashMap<String, String>>,
    output: Vec<Vec<RecordBatch>>,
) -> Result<Value> {
    let (ring, _) = consistent_hash_context!();
    let sync = infer_invocation_type(&metadata)?;
    let invocation_type = if sync {
        FLOCK_LAMBDA_SYNC_CALL.to_string()
    } else {
        FLOCK_LAMBDA_ASYNC_CALL.to_string()
    };

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
        CloudFunction::Lambda(group_name) => {
            if ctx.is_aggregate() {
                // If the current function is an aggregator, which means its output
                // can be repartitioned to multiple partitions, and each partition
                // can be executed by a single lambda function for the next stage of the
                // dataflow pipeline.
                let output = Box::new(output);
                let size = output.len();
                let mut uuid_builder =
                    UuidBuilder::new_with_ts(group_name, Utc::now().timestamp(), size);
                let tasks = (0..size)
                    .map(|i| {
                        let data = output.clone();
                        let function_name = group_name.clone();
                        let meta = metadata.clone();
                        let invoke_type = invocation_type.clone();
                        let uuid = uuid_builder.next_uuid();
                        tokio::spawn(async move {
                            let mut payload = to_payload(&data[i], &[], uuid, sync);
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
                            .await?;
                            Ok(())
                        })
                    })
                    .collect::<Vec<tokio::task::JoinHandle<Result<()>>>>();
                futures::future::join_all(tasks).await;
            } else {
                // If the current function is not an aggregator, which means its
                // output CANNOT be repartitioned to multiple partitions,
                // otherwise the future aggregator CANNOT ganuantee the
                // correctness of the result. Therefore, we have to reuse the
                // uuid of the current payload to the next function.
                let mut payload = to_payload(
                    &output.into_iter().flatten().collect::<Vec<_>>(),
                    &[],
                    uuid,
                    sync,
                );
                payload.query_number = query_number;
                payload.metadata = metadata;
                let bytes = serde_json::to_vec(&payload)?;

                info!(
                    "[OK] {} function's payload bytes: {}",
                    group_name,
                    bytes.len()
                );
                lambda::invoke_function(group_name, &invocation_type, Some(bytes.into())).await?;
            }
            Ok(json!({
                "response": format!("next function: {}", group_name)
            }))
        }
        CloudFunction::Group((group_name, _)) => {
            if !ctx.is_shuffling().await? {
                let next_function = ring.get(&uuid.qid).expect("hash ring failure.").to_string();
                let mut payload = to_payload(
                    &output.into_iter().flatten().collect::<Vec<_>>(),
                    &[],
                    uuid,
                    sync,
                );
                payload.query_number = query_number;
                payload.metadata = metadata;
                let bytes = serde_json::to_vec(&payload)?;

                info!(
                    "[OK] {} function's payload bytes: {}",
                    next_function,
                    bytes.len()
                );

                let state_backend = ctx.state_backend.clone();
                let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = vec![];

                if state_backend
                    .as_any()
                    .downcast_ref::<S3StateBackend>()
                    .is_some()
                {
                    let bytes_copy = bytes.clone();
                    let current_function = ctx.name.clone();
                    tasks.push(tokio::spawn(async move {
                        // function name format: <query code>-<plan index>-<group index>
                        let plan_index = current_function.split('-').collect::<Vec<_>>()[1]
                            .parse::<usize>()
                            .expect("parse the plan index error.");
                        let next_plan_index = plan_index + 1;
                        let shuffle_id = 0; // since the current function is not shuffling
                        let bucket = payload.uuid.qid;
                        let key = format!(
                            "{:02}/{:02}/{:02}",
                            next_plan_index, shuffle_id, payload.uuid.seq_num
                        );

                        // S3 state backend:
                        // - bucket equals to qid: <query code>-<timestamp>-<random string>
                        // - key: <plan index>-<shuffle id>-<sequence id>
                        state_backend.write(bucket, key, bytes_copy).await?;

                        Ok(())
                    }));
                }

                tasks.push(tokio::spawn(async move {
                    lambda::invoke_function(&next_function, &invocation_type, Some(bytes.into()))
                        .await?;
                    Ok(())
                }));

                futures::future::join_all(tasks).await;

                Ok(json!({
                    "response": format!("next function group: {}", group_name)
                }))
            } else {
                let mut rng = StdRng::seed_from_u64(0xDEAD); // Predictable RNG clutch
                let tasks = (0..output.len())
                    .map(|i| {
                        let my_output = output.clone();
                        let my_metadata = metadata.clone();
                        let my_uuid = uuid.clone();
                        let state_backend = ctx.state_backend.clone();
                        let current_function = ctx.name.clone();
                        let invoke_type = invocation_type.clone();

                        // Partitions at the same index position in different functions can get the
                        // same hash key. Therefore, they can be forwarded to the same lambda
                        // function.
                        //
                        // Function 0: data[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                        // Function 1: data[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                        // Function 2: data[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                        // ..
                        // Function n: data[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                        //
                        // F0[0], F1[0], F2[0] .. Fn[0] ---> lambda function x
                        // F0[1], F1[1], F2[1] .. Fn[1] ---> lambda function y
                        // F0[2], F1[2], F2[2] .. Fn[2] ---> lambda function z
                        // ..
                        // F0[n], F1[n], F2[n] .. Fn[n] ---> lambda function v
                        let mut arr = [0u8; 64];
                        rng.fill(&mut arr);
                        let next_function = ring.get(&arr).expect("hash ring failure.").to_string();

                        tokio::spawn(async move {
                            let mut payload = to_payload(&my_output[i], &[], my_uuid, sync);
                            payload.query_number = query_number;
                            payload.metadata = my_metadata;
                            // set shuffle id to each data partition since they will be aggregated
                            // at different functions.
                            payload.shuffle_id = Some(i);
                            let bytes = serde_json::to_vec(&payload)?;

                            info!(
                                "[OK] {} function's payload bytes: {}",
                                next_function,
                                bytes.len()
                            );

                            let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = vec![];

                            if state_backend
                                .as_any()
                                .downcast_ref::<S3StateBackend>()
                                .is_some()
                            {
                                let bytes_copy = bytes.clone();
                                tasks.push(tokio::spawn(async move {
                                    // function name format: <query code>-<plan index>-<group
                                    // index>
                                    let plan_index =
                                        current_function.split('-').collect::<Vec<_>>()[1]
                                            .parse::<usize>()
                                            .expect("parse the plan index error.");
                                    let next_plan_index = plan_index + 1;
                                    let shuffle_id = i; // since the current function is shuffling
                                    let bucket = payload.uuid.qid;
                                    let key = format!(
                                        "{:02}/{:02}/{:02}",
                                        next_plan_index, shuffle_id, payload.uuid.seq_num
                                    );

                                    // S3 state backend:
                                    // - bucket equals to qid: <query code>-<timestamp>-<random
                                    //   string>
                                    // - key: <plan index>-<shuffle id>-<sequence id>
                                    state_backend.write(bucket, key, bytes_copy).await?;

                                    Ok(())
                                }));
                            }

                            tasks.push(tokio::spawn(async move {
                                lambda::invoke_function(
                                    &next_function,
                                    &invoke_type,
                                    Some(bytes.into()),
                                )
                                .await?;
                                Ok(())
                            }));

                            futures::future::join_all(tasks).await;

                            Ok(())
                        })
                    })
                    .collect::<Vec<tokio::task::JoinHandle<Result<()>>>>();
                futures::future::join_all(tasks).await;

                Ok(json!({
                    "response": format!("next function group: {}", group_name)
                }))
            }
        }
    }
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
