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

extern crate daggy;

#[path = "../rainbow.rs"]
mod rainbow;

use super::add_extra_metadata;
use super::create_nexmark_source;
use super::create_physical_plans;
use super::nexmark_query;
use crate::NexmarkBenchmarkOpt;
use daggy::NodeIndex;
use datafusion::execution::context::ExecutionConfig;
use flock::aws::lambda;
use flock::distributed_plan::QueryDag;
use flock::prelude::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use nexmark::register_nexmark_tables_with_config;
use rainbow::{rainbow_println, rainbow_string};
use rusoto_lambda::InvocationResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

lazy_static! {
    pub static ref NEXMARK_SOURCE_LOG_GROUP: String = "/aws/lambda/flock_datasource".to_string();
}

pub async fn nexmark_benchmark(opt: &mut NexmarkBenchmarkOpt) -> Result<()> {
    rainbow_println("================================================================");
    rainbow_println("                    Running the benchmark                       ");
    rainbow_println("================================================================");
    info!("Running the NEXMark benchmark with the following options:\n");
    println!("{:#?}\n", opt);

    let query_number = opt.query_number;
    let query_code = format!("q{}", opt.query_number);
    let nexmark_conf = create_nexmark_source(opt).await?;

    let config = ExecutionConfig::new().with_target_partitions(opt.target_partitions);
    let mut ctx = register_nexmark_tables_with_config(config).await?;

    let plans = create_physical_plans(&mut ctx, query_number).await?;
    let plan = plans.last().unwrap().clone();
    let sink_type = DataSinkType::new(&opt.data_sink_type)?;

    let state_backend: Arc<dyn StateBackend> = match opt.state_backend.as_str() {
        "hashmap" => Arc::new(HashMapStateBackend::new()),
        "s3" => Arc::new(S3StateBackend::new()),
        "efs" => Arc::new(EfsStateBackend::new()),
        _ => unreachable!(),
    };

    let mut launcher =
        AwsLambdaLauncher::try_new(query_code, plan, sink_type, state_backend).await?;
    launcher.create_cloud_contexts(*FLOCK_FUNCTION_CONCURRENCY)?;

    info!(
        "Streaming: {}",
        rainbow_string(format!("{:?}", nexmark_conf.window))
    );
    info!(
        "SQL query:\n\n{}",
        nexmark_query(query_number).last().unwrap()
    );

    let stages = launcher.dag.get_all_stages();
    for (i, stage) in stages.iter().enumerate() {
        info!("{}", rainbow_string(format!("=== Query Stage {} ===", i)));
        info!(
            "Current function type: {}",
            rainbow_string(format!("{:?}", stage.get_function_type()))
        );
        info!(
            "Next function name: {}",
            rainbow_string(format!("{:?}", stage.context.as_ref().unwrap().next))
        );
        info!("Physical Plan:\n{}", stage.get_plan_str());
    }

    let dag = &mut launcher.dag;
    create_nexmark_functions(dag, opt, *FLOCK_FUNCTION_CONCURRENCY).await?;

    let mut metadata = HashMap::new();
    add_extra_metadata(opt, &mut metadata).await?;

    let tasks = (0..opt.generators)
        .into_iter()
        .map(|i| {
            let s = nexmark_conf.clone();
            let m = metadata.clone();
            let f = format!("q{}-{:02}", opt.query_number, 0);
            tokio::spawn(async move {
                info!(
                    "[OK] Invoking NEXMark source function: {} by generator {}\n",
                    rainbow_string(&f),
                    i
                );
                let p = serde_json::to_vec(&Payload {
                    datasource: DataSource::NEXMarkEvent(s),
                    query_number: Some(query_number),
                    metadata: Some(m),
                    ..Default::default()
                })?
                .into();
                lambda::invoke_function(&f, &FLOCK_LAMBDA_ASYNC_CALL, Some(p)).await
            })
        })
        // this collect *is needed* so that the join below can switch between tasks.
        .collect::<Vec<JoinHandle<Result<InvocationResponse>>>>();

    futures::future::join_all(tasks).await;

    Ok(())
}

/// Create lambda functions for a given NexMark query.
async fn create_nexmark_functions(
    dag: &mut QueryDag,
    opt: &NexmarkBenchmarkOpt,
    group_size: usize,
) -> Result<()> {
    let count = dag.node_count();
    assert!(count < 100);

    let func_types = (0..count)
        .map(|i| dag.get_node(NodeIndex::new(i)).unwrap().get_function_type())
        .collect::<Vec<CloudFunctionType>>();

    for i in (0..count).rev() {
        let node = dag.get_node_mut(NodeIndex::new(i)).unwrap();
        if func_types[i] == CloudFunctionType::Group {
            let group_name = format!("q{}-{:02}", opt.query_number, count - 1 - i);
            info!(
                "Creating lambda function group: {}",
                rainbow_string(format!("({}, {})", group_name, group_size))
            );
            let tasks = (0..group_size)
                .into_iter()
                .map(|j| {
                    let mut ctx = node.context.clone().unwrap();
                    let name = group_name.clone();
                    let memory_size = opt.memory_size;
                    let architecture = opt.architecture.clone();
                    tokio::spawn(async move {
                        ctx.name = format!("{}-{:02}", name, j);
                        lambda::create_function(&ctx, memory_size, &architecture).await?;
                        info!("Created function member: {}", rainbow_string(&ctx.name));
                        lambda::set_concurrency(&ctx.name, 1).await
                    })
                })
                .collect::<Vec<JoinHandle<Result<()>>>>();
            futures::future::join_all(tasks).await;
            tokio::time::sleep(parse_duration("2s").unwrap()).await;
        } else {
            lambda::create_function(
                node.context.as_ref().unwrap(),
                opt.memory_size,
                &opt.architecture,
            )
            .await?;
            info!(
                "Created lambda function: {}",
                rainbow_string(format!("q{}-{:02}", opt.query_number, count - 1 - i))
            );
        }
    }

    Ok(())
}
