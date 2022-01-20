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
use datafusion::physical_plan::Partitioning::RoundRobinBatch;
use flock::aws::lambda;
use flock::aws::s3;
use flock::prelude::*;
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
    let num_partitions = partitions.len();

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
    if input_partitions.is_empty() || input_partitions.len() != num_partitions {
        Ok(vec![])
    } else {
        ctx.feed_data_sources(input_partitions).await?;
        let output = ctx.execute().await?;
        ctx.clean_data_sources().await?;
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

/// Check the current function type.
///
/// If the function type is `lambda`, the function uses the default lambda
/// concurrency. If the function type is `group`, the function is an aggregator
/// and the concurrency is 1.
fn is_aggregate_function(ctx: &ExecutionContext) -> bool {
    // if the function name format is "<query code>-<plan index>-<group index>",
    // then it is an aggregate function.
    let dash_count = ctx.name.matches('-').count();
    if dash_count == 2 {
        true
    } else if dash_count == 1 {
        false
    } else {
        panic!("Invalid function name: {}", ctx.name);
    }
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
    } else if is_aggregate_function(ctx) {
        // ressemble data packets to a single window.
        let (ready, uuid) = arena.collect(event);
        if ready {
            info!("Received all data packets for the window: {:?}", uuid.tid);
            arena
                .take_batches(&uuid.tid)
                .into_iter()
                .for_each(|b| input.push(b));
        } else {
            // Aggregation has not yet been completed. We can also check the query states in
            // the corresponding S3 buckets. If some states exist in S3, Flock can bring the
            // states to the current function directly to reduce the network traffic. This
            // is because the aggregation states are not saved by its own, but are saved by
            // the former stage of the dataflow pipeline. Since aggregator's ancestors are
            // default Lambda functions with much higher concurrency, all of them can write
            // the partial aggregation states to the S3 buckets in parallel.
            match arena.get_bitmap(&uuid.tid) {
                Some(bitmap) => {
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
                        // function name format: <query code>-<plan index>-<group index>
                        let mut name_parts = ctx.name.split('-');
                        name_parts.next(); // skip the query code
                        let plan_index = name_parts.next().unwrap();
                        let keys = state_backend
                            .new_s3_keys(&uuid.tid, &plan_index, &bitmap)
                            .await?;
                        if !keys.is_empty() {
                            state_backend
                                .read(uuid.tid.clone(), keys)
                                .await?
                                .into_iter()
                                .for_each(|payload| {
                                    arena.collect(payload);
                                });
                            if arena.is_complete(&uuid.tid) {
                                info!("Received all data packets for the window: {:?}", uuid.tid);
                                arena
                                    .take_batches(&uuid.tid)
                                    .into_iter()
                                    .for_each(|b| input.push(b));
                            }
                        }
                    }
                }
                None => {}
            }

            return Ok(vec![]);
        }
    } else {
        // data packet is an individual event for the current function.
        let (r1, r2, _) = event.to_record_batch();
        input.push(vec![r1]);
        input.push(vec![r2]);
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
    ctx: &ExecutionContext,
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
            if is_aggregate_function(ctx) {
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
        CloudFunction::Group(..) => {
            let next_function = ring.get(&uuid.tid).expect("hash ring failure.").to_string();
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

            let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = vec![];
            let bytes_copy = bytes.clone();
            let state_backend = ctx.state_backend.clone();

            let current_function = ctx.name.clone();
            tasks.push(tokio::spawn(async move {
                if state_backend
                    .as_any()
                    .downcast_ref::<S3StateBackend>()
                    .is_some()
                {
                    // function name format: <query code>-<plan index>-<group index>
                    let plan_index = current_function.split('-').collect::<Vec<_>>()[1]
                        .parse::<usize>()
                        .expect("parse the plan index error.");
                    let next_plan_index = plan_index + 1;

                    let bucket = payload.uuid.tid;
                    let key = format!("{:02}/{:02}", next_plan_index, payload.uuid.seq_num);

                    // S3 state backend:
                    // - bucket equals to tid: <query code>-<timestamp>-<random string>
                    // - key: <plan index>-<sequence number>
                    state_backend.write(bucket, key, bytes_copy).await?;
                }
                Ok(())
            }));

            let next_func = next_function.clone();
            tasks.push(tokio::spawn(async move {
                lambda::invoke_function(&next_func, &invocation_type, Some(bytes.into())).await?;
                Ok(())
            }));

            futures::future::join_all(tasks).await;

            Ok(json!({
                "response": format!("next function: {}", next_function)
            }))
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
