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

#[path = "../rainbow.rs"]
mod rainbow;

use super::create_ysb_source;
use super::ysb_query;
use crate::YSBBenchmarkOpt;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::ExecutionPlan;
use flock::aws::{cloudwatch, lambda};
use flock::prelude::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use rainbow::{rainbow_println, rainbow_string};
use rusoto_lambda::InvocationResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use ysb::register_ysb_tables;

lazy_static! {
    pub static ref YSB_SOURCE_LOG_GROUP: String = "/aws/lambda/flock_datasource".to_string();
}

pub async fn ysb_benchmark(opt: &mut YSBBenchmarkOpt) -> Result<()> {
    rainbow_println("================================================================");
    rainbow_println("                    Running the benchmark                       ");
    rainbow_println("================================================================");
    info!("Running the YSB benchmark with the following options:\n");
    rainbow_println(format!("{:#?}\n", opt));

    let ysb_conf = create_ysb_source(opt);
    let ctx = register_ysb_tables().await?;
    let plan = physical_plan(&ctx, &ysb_query()).await?;
    let root_actor = create_ysb_functions(opt, plan).await?;

    // The source generator function needs the metadata to determine the type of the
    // workers such as single function or a group. We don't want to keep this info
    // in the environment as part of the source function. Otherwise, we have to
    // *delete* and **recreate** the source function every time we change the query.
    let mut metadata = HashMap::new();
    metadata.insert("workers".to_string(), serde_json::to_string(&root_actor)?);
    metadata.insert(
        "invocation_type".to_string(),
        if opt.async_type {
            "async".to_string()
        } else {
            "sync".to_string()
        },
    );

    let tasks = (0..opt.generators)
        .into_iter()
        .map(|i| {
            let s = ysb_conf.clone();
            let m = metadata.clone();
            tokio::spawn(async move {
                info!(
                    "[OK] Invoking YSB source function: {} by generator {}\n",
                    rainbow_string(&*FLOCK_DATA_SOURCE_FUNC_NAME),
                    i
                );
                let p = serde_json::to_vec(&Payload {
                    datasource: DataSource::YSBEvent(s),
                    metadata: Some(m),
                    ..Default::default()
                })?
                .into();
                lambda::invoke_function(
                    &FLOCK_DATA_SOURCE_FUNC_NAME,
                    &FLOCK_LAMBDA_ASYNC_CALL,
                    Some(p),
                )
                .await
            })
        })
        // this collect *is needed* so that the join below can switch between tasks.
        .collect::<Vec<JoinHandle<Result<InvocationResponse>>>>();

    futures::future::join_all(tasks).await;

    info!("Waiting for the current invocations to be logged.");
    tokio::time::sleep(parse_duration("5s").unwrap()).await;
    cloudwatch::fetch(&YSB_SOURCE_LOG_GROUP, parse_duration("1min").unwrap()).await?;

    let sink_type = DataSinkType::new(&opt.data_sink_type)?;
    if sink_type != DataSinkType::Blackhole {
        let data_sink =
            DataSink::read("ysb".to_string(), sink_type, DataSinkFormat::default()).await?;
        info!(
            "[OK] Received {} batches from the data sink.",
            data_sink.record_batches.len()
        );
        info!("[OK] Last data sink function: {}", data_sink.function_name);
        let function_log_group = format!("/aws/lambda/{}", data_sink.function_name);
        cloudwatch::fetch(&function_log_group, parse_duration("1min").unwrap()).await?;
        println!("{}", pretty_format_batches(&data_sink.record_batches)?);
    }

    Ok(())
}

/// Create lambda functions for a given YSB query.
/// The returned function is the worker group as a whole which will be executed
/// by the YSB data generator function.
async fn create_ysb_functions(
    opt: &YSBBenchmarkOpt,
    physcial_plan: Arc<dyn ExecutionPlan>,
) -> Result<CloudFunction> {
    let worker_func_name = "ysb-00".to_string();
    let next_func_name =
        CloudFunction::Group((worker_func_name.clone(), *FLOCK_FUNCTION_CONCURRENCY));

    let ysb_source_ctx = ExecutionContext {
        plan: CloudExecutionPlan::new(vec![FLOCK_EMPTY_PLAN.clone()], None),
        name: FLOCK_DATA_SOURCE_FUNC_NAME.clone(),
        next: next_func_name.clone(),
        ..Default::default()
    };

    let ysb_worker_ctx = ExecutionContext {
        plan: CloudExecutionPlan::new(vec![physcial_plan], None),
        name: worker_func_name.clone(),
        next: CloudFunction::Sink(DataSinkType::new(&opt.data_sink_type)?),
        ..Default::default()
    };

    // Create the function for the ysb source generator.
    info!(
        "Creating lambda function: {}",
        rainbow_string(FLOCK_DATA_SOURCE_FUNC_NAME.clone())
    );
    lambda::create_function(&ysb_source_ctx, 4096, &opt.architecture).await?;

    // Create the function for the ysb worker.
    match next_func_name.clone() {
        CloudFunction::Group((name, concurrency)) => {
            info!(
                "Creating lambda function group: {}",
                rainbow_string(format!("{:?}", ysb_source_ctx.next))
            );

            let tasks = (0..concurrency)
                .into_iter()
                .map(|i| {
                    let mut worker_ctx = ysb_worker_ctx.clone();
                    let group_name = name.clone();
                    let memory_size = opt.memory_size;
                    let architecture = opt.architecture.clone();
                    tokio::spawn(async move {
                        worker_ctx.name = format!("{}-{:02}", group_name, i);
                        info!(
                            "Creating function member: {}",
                            rainbow_string(&worker_ctx.name)
                        );
                        lambda::create_function(&worker_ctx, memory_size, &architecture).await?;
                        lambda::set_concurrency(&worker_ctx.name, 1).await
                    })
                })
                .collect::<Vec<JoinHandle<Result<()>>>>();
            futures::future::join_all(tasks).await;
        }
        _ => unreachable!(),
    }

    Ok(next_func_name)
}
