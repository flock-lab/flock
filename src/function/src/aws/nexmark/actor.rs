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
use arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::Partitioning;
use futures::executor::block_on;
use lazy_static::lazy_static;
use log::{info, warn};
use rayon::prelude::*;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{InvokeAsyncRequest, Lambda, LambdaClient};
use serde_json::json;
use serde_json::Value;

lazy_static! {
    static ref CONCURRENCY: usize = FLOCK_CONF["lambda"]["concurrency"]
        .parse::<usize>()
        .unwrap();
}

/// The function invocation counter per lambda instance.
static mut INVOCATION_COUNTER_PER_INSTANCE: u32 = 0;

/// The function executor for the nexmark benchmark.
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
    r1_records: Vec<Vec<RecordBatch>>,
    r2_records: Vec<Vec<RecordBatch>>,
) -> Result<Vec<RecordBatch>> {
    let mut inputs = vec![];
    if !(r1_records.is_empty() || r1_records.iter().all(|r| r.is_empty())) {
        inputs.push(
            LambdaExecutor::repartition(r1_records, Partitioning::RoundRobinBatch(*CONCURRENCY))
                .await?,
        );
    }
    if !(r2_records.is_empty() || r2_records.iter().all(|r| r.is_empty())) {
        inputs.push(
            LambdaExecutor::repartition(r2_records, Partitioning::RoundRobinBatch(*CONCURRENCY))
                .await?,
        );
    }

    if inputs.is_empty() {
        return Ok(vec![]);
    } else {
        ctx.feed_data_sources(&inputs).await?;
        let output = ctx.execute().await?;
        info!("{}", pretty_format_batches(&output)?);
        unsafe {
            INVOCATION_COUNTER_PER_INSTANCE += 1;
            info!("# invocations: {}", INVOCATION_COUNTER_PER_INSTANCE);
        }
        Ok(output)
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
    let tid = event.uuid.tid.clone();

    let input_partitions = {
        if match &ctx.next {
            CloudFunction::None | CloudFunction::Lambda(..) => true,
            CloudFunction::Group(..) => false,
        } {
            // ressemble data packets to a single window.
            let (ready, uuid) = arena.reassemble(event);
            if ready {
                info!("Received all data packets for the window: {:?}", uuid.tid);
                arena.batches(uuid.tid)
            } else {
                let response = format!("Window data collection has not been completed.");
                info!("{}", response);
                return Ok(json!({ "response": response }));
            }
        } else {
            // data packet is an individual event for the current function.
            let (r1_records, r2_records, _) = event.to_record_batch();
            (vec![r1_records], vec![r2_records])
        }
    };

    let output = collect(ctx, input_partitions.0, input_partitions.1).await?;

    if ctx.next != CloudFunction::None {
        let mut batches = LambdaExecutor::coalesce_batches(
            vec![output],
            FLOCK_CONF["lambda"]["payload_batch_size"]
                .parse::<usize>()
                .unwrap(),
        )
        .await?;
        assert_eq!(1, batches.len());
        // call the next stage of the dataflow graph.
        invoke_next_functions(&ctx, &mut batches[0])?;
    }

    // TODO(gangliao): sink results to other cloud services.
    Ok(json!({"name": &ctx.name, "tid": tid}))
}

/// Invoke functions in the next stage of the data flow.
fn invoke_next_functions(ctx: &ExecutionContext, batches: &mut Vec<RecordBatch>) -> Result<()> {
    // retrieve the next lambda function names
    let next_func = LambdaExecutor::next_function(&ctx)?;

    // create uuid builder to assign id to each payload
    let uuid_builder = UuidBuilder::new(&ctx.name, batches.len());

    let client = &LambdaClient::new(Region::default());
    batches.into_par_iter().enumerate().for_each(|(i, batch)| {
        // call the lambda function asynchronously until it succeeds.
        loop {
            let uuid = uuid_builder.get(i);
            let request = InvokeAsyncRequest {
                function_name: next_func.clone(),
                invoke_args:   to_bytes(&batch, uuid, Encoding::default()),
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
