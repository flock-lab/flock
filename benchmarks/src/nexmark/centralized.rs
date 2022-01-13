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

use super::add_extra_metadata;
use super::create_nexmark_functions;
use super::create_nexmark_source;
use super::create_physical_plans;
use crate::NexmarkBenchmarkOpt;

use datafusion::arrow::util::pretty::pretty_format_batches;
use flock::aws::{cloudwatch, lambda};
use flock::prelude::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use nexmark::register_nexmark_tables;
use rainbow::{rainbow_println, rainbow_string};
use rusoto_lambda::InvocationResponse;
use std::collections::HashMap;
use tokio::task::JoinHandle;

lazy_static! {
    pub static ref NEXMARK_SOURCE_LOG_GROUP: String = "/aws/lambda/flock_datasource".to_string();
}

pub async fn nexmark_benchmark(opt: &mut NexmarkBenchmarkOpt) -> Result<()> {
    rainbow_println("================================================================");
    rainbow_println("                    Running the benchmark                       ");
    rainbow_println("================================================================");
    info!("Running the NEXMark benchmark with the following options:\n");
    rainbow_println(format!("{:#?}\n", opt));

    let query_number = opt.query_number;
    let nexmark_conf = create_nexmark_source(opt).await?;

    let mut ctx = register_nexmark_tables().await?;
    let plans = create_physical_plans(&mut ctx, query_number).await?;
    let worker = create_nexmark_functions(
        opt,
        nexmark_conf.window.clone(),
        plans.last().unwrap().clone(),
    )
    .await?;

    // The source generator function needs the metadata to determine the type of the
    // workers such as single function or a group. We don't want to keep this info
    // in the environment as part of the source function. Otherwise, we have to
    // *delete* and **recreate** the source function every time we change the query.
    let mut metadata = HashMap::new();
    metadata.insert("workers".to_string(), serde_json::to_string(&worker)?);
    add_extra_metadata(opt, &mut metadata).await?;

    let tasks = (0..opt.generators)
        .into_iter()
        .map(|i| {
            let s = nexmark_conf.clone();
            let m = metadata.clone();
            tokio::spawn(async move {
                info!(
                    "[OK] Invoking NEXMark source function: {} by generator {}\n",
                    rainbow_string(&*FLOCK_DATA_SOURCE_FUNC_NAME),
                    i
                );
                let p = serde_json::to_vec(&Payload {
                    datasource: DataSource::NEXMarkEvent(s),
                    query_number: Some(query_number),
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
    cloudwatch::fetch(&NEXMARK_SOURCE_LOG_GROUP, parse_duration("1min").unwrap()).await?;

    let sink_type = DataSinkType::new(&opt.data_sink_type)?;
    if sink_type != DataSinkType::Blackhole {
        let data_sink = DataSink::read(
            format!("q{}", opt.query_number),
            sink_type,
            DataSinkFormat::default(),
        )
        .await?;
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
