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

use super::create_ysb_source;
use super::ysb_query;
use crate::YSBBenchmarkOpt;

use daggy::NodeIndex;
use datafusion::execution::context::ExecutionConfig;
use flock::aws::lambda;
use flock::distributed_plan::QueryDag;
use flock::prelude::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use rainbow::{rainbow_println, rainbow_string};
use rusoto_lambda::InvocationResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use ysb::register_ysb_tables_with_config;

lazy_static! {
    pub static ref YSB_SOURCE_LOG_GROUP: String = "/aws/lambda/flock_datasource".to_string();
}

pub async fn ysb_benchmark(opt: &mut YSBBenchmarkOpt) -> Result<()> {
    rainbow_println("================================================================");
    rainbow_println("                    Running the benchmark                       ");
    rainbow_println("================================================================");
    info!("Running the YSB benchmark with the following options:\n");
    rainbow_println(format!("{:#?}\n", opt));

    let query_code = "ysb";
    let ysb_conf = create_ysb_source(opt);

    let config = ExecutionConfig::new().with_target_partitions(opt.target_partitions);
    let ctx = register_ysb_tables_with_config(config).await?;

    let plan = physical_plan(&ctx, &ysb_query()).await?;
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
        rainbow_string(format!("{:?}", ysb_conf.window))
    );
    info!("SQL query:\n\n{}", ysb_query());

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
    create_ysb_functions(dag, opt, *FLOCK_FUNCTION_CONCURRENCY).await?;

    let mut metadata = HashMap::new();
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
            let f = format!("ysb-{:02}", 0);
            tokio::spawn(async move {
                info!(
                    "[OK] Invoking YSB source function: {} by generator {}\n",
                    rainbow_string(&f),
                    i
                );
                let p = serde_json::to_vec(&Payload {
                    datasource: DataSource::YSBEvent(s),
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

/// Create lambda functions for a given YSB query.
async fn create_ysb_functions(
    dag: &mut QueryDag,
    opt: &YSBBenchmarkOpt,
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
            let group_name = format!("ysb-{:02}", count - 1 - i);
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
                rainbow_string(format!("ysb-{:02}", count - 1 - i))
            );
        }
    }

    Ok(())
}
