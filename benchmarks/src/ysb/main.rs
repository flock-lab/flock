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

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::ExecutionPlan;
use flock::aws::{cloudwatch, lambda};
use flock::prelude::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use rusoto_lambda::InvocationResponse;
use std::collections::HashMap;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::task::JoinHandle;
use ysb::event::{AdEvent, Campaign};
use ysb::register_ysb_tables;
use ysb::YSBSource;

lazy_static! {
    // YSB Benchmark
    static ref YSB_AD_EVENT: SchemaRef = Arc::new(AdEvent::schema());
    static ref YSB_CAMPAIGN: SchemaRef = Arc::new(Campaign::schema());
    static ref YSB_SOURCE_LOG_GROUP: String = "/aws/lambda/flock_datasource".to_string();
}

#[derive(Default, Clone, Debug, StructOpt)]
pub struct YSBBenchmarkOpt {
    /// Number of threads or generators of each test run
    #[structopt(short = "g", long = "generators", default_value = "1")]
    pub generators: usize,

    /// Number of threads to use for parallel execution
    #[structopt(short = "s", long = "seconds", default_value = "20")]
    pub seconds: usize,

    /// Number of events generated among generators per second
    #[structopt(short = "e", long = "events_per_second", default_value = "1000")]
    pub events_per_second: usize,

    /// The data sink type to use
    #[structopt(short = "d", long = "data_sink_type", default_value = "blackhole")]
    pub data_sink_type: String,

    /// The function invocation mode to use
    #[structopt(long = "async")]
    pub async_type: bool,

    /// The worker function's memory size
    #[structopt(short = "m", long = "memory_size", default_value = "128")]
    pub memory_size: i64,

    /// The system architecture to use
    #[structopt(short = "a", long = "arch", default_value = "x86_64")]
    pub architecture: String,
}

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<()> {
    env_logger::init();
    ysb_benchmark(YSBBenchmarkOpt::from_args()).await?;
    Ok(())
}

fn create_ysb_source(opt: &YSBBenchmarkOpt) -> YSBSource {
    let window = Window::Tumbling(Schedule::Seconds(10));
    YSBSource::new(opt.seconds, opt.generators, opt.events_per_second, window)
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
        FLOCK_DATA_SOURCE_FUNC_NAME.clone()
    );
    lambda::create_function(&ysb_source_ctx, 1024, &opt.architecture).await?;

    // Create the function for the ysb worker.
    match next_func_name.clone() {
        CloudFunction::Group((name, concurrency)) => {
            info!("Creating lambda function group: {:?}", ysb_source_ctx.next);

            let tasks = (0..concurrency)
                .into_iter()
                .map(|i| {
                    let mut worker_ctx = ysb_worker_ctx.clone();
                    let group_name = name.clone();
                    let memory_size = opt.memory_size;
                    let architecture = opt.architecture.clone();
                    tokio::spawn(async move {
                        worker_ctx.name = format!("{}-{:02}", group_name, i);
                        info!("Creating function member: {}", worker_ctx.name);
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

pub async fn ysb_benchmark(opt: YSBBenchmarkOpt) -> Result<()> {
    info!(
        "Running the YSB benchmark with the following options:\n{:#?}",
        opt
    );
    let ysb_conf = create_ysb_source(&opt);
    let ctx = register_ysb_tables().await?;
    let plan = physical_plan(&ctx, &ysb_query()).await?;
    let root_actor = create_ysb_functions(&opt, plan).await?;

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
                    "[OK] Invoking YSB source function: {} by generator {}",
                    *FLOCK_DATA_SOURCE_FUNC_NAME, i
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

/// Returns YSB query string.
fn ysb_query() -> String {
    include_str!("ysb.sql").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flock::transmute::event_bytes_to_batch;

    #[tokio::test]
    async fn ysb_sql_query() -> Result<()> {
        let opt = YSBBenchmarkOpt {
            generators: 1,
            seconds: 1,
            events_per_second: 100_000,
            ..Default::default()
        };
        let conf = create_ysb_source(&opt);

        let stream = Arc::new(conf.generate_data()?);
        let (campaigns, _) = stream.campaigns.clone();
        let (event, _) = stream.select(0, 0).expect("Failed to select event.");

        let sql = ysb_query();
        let ctx = register_ysb_tables().await?;
        let plan = physical_plan(&ctx, &sql).await?;
        let mut flock_ctx = ExecutionContext {
            plan: CloudExecutionPlan::new(vec![plan], None),
            ..Default::default()
        };

        flock_ctx
            .feed_data_sources(&[
                vec![event_bytes_to_batch(
                    &event.ad_events,
                    YSB_AD_EVENT.clone(),
                    1024,
                )],
                vec![event_bytes_to_batch(&campaigns, YSB_CAMPAIGN.clone(), 1024)],
            ])
            .await?;

        let output = flock_ctx.execute().await?;
        println!("{}", pretty_format_batches(&output[0])?);
        Ok(())
    }
}
