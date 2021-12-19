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

//! This is a FaaS baseline of the benchmark. It contains the client's
//! coordinator, and the functions communicate through S3.

#[path = "../nexmark/main.rs"]
mod nexmark_bench;
use driver::deploy::common::*;
use log::info;
use nexmark_bench::*;
use runtime::prelude::*;
use std::collections::HashMap;
use std::time::SystemTime;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    benchmark(&mut NexmarkBenchmarkOpt::from_args()).await?;
    Ok(())
}

pub fn set_nexmark_config(opt: &mut NexmarkBenchmarkOpt) -> Result<()> {
    opt.async_type = false;
    opt.generators = 1;
    match opt.query_number {
        0 | 1 | 2 | 3 | 4 | 6 | 9 | 13 => opt.seconds = 1, // ElementWise
        5 => opt.seconds = 10,                             // HoppingWindow
        7..=8 => opt.seconds = 10,                         // TumblingWindow
        _ => unreachable!(),
    };
    Ok(())
}

async fn benchmark(opt: &mut NexmarkBenchmarkOpt) -> Result<()> {
    set_nexmark_config(opt)?;
    info!(
        "Running the NEXMark benchmark [S3] with the following options: {:?}",
        opt
    );
    let nexmark_conf = create_nexmark_source(opt);
    let query_number = opt.query_number;

    let mut ctx = register_nexmark_tables().await?;
    let plan = physical_plan(&mut ctx, &nexmark_query(query_number))?;
    let root_actor =
        create_nexmark_functions(opt.clone(), nexmark_conf.window.clone(), plan).await?;

    // The source generator function needs the metadata to determine the type of the
    // workers such as single function or a group. We don't want to keep this info
    // in the environment as part of the source function. Otherwise, we have to
    // *delete* and **recreate** the source function every time we change the query.
    let mut metadata = HashMap::new();
    metadata.insert("workers".to_string(), serde_json::to_string(&root_actor)?);
    metadata.insert("invocation_type".to_string(), "sync".to_string());

    let start_time = SystemTime::now();
    info!(
        "[OK] Invoking NEXMark source function: {}",
        NEXMARK_SOURCE_FUNC_NAME.clone()
    );
    let payload = serde_json::to_vec(&Payload {
        datasource: DataSource::S3(nexmark_conf.clone()),
        query_number: Some(query_number),
        metadata: Some(metadata),
        ..Default::default()
    })?
    .into();

    let resp = serde_json::to_value(
        &invoke_lambda_function(
            NEXMARK_SOURCE_FUNC_NAME.clone(),
            Some(payload),
            FLOCK_LAMBDA_SYNC_CALL.to_string(),
        )
        .await?
        .payload
        .expect("No response"),
    )?;

    let function_name = resp["function"].as_str().unwrap().to_string();
    let sync = true;

    let mut metadata = HashMap::new();
    metadata.insert("s3_bucket".to_string(), resp["bucket"].as_str().unwrap());
    metadata.insert("s3_key".to_string(), resp["key"].as_str().unwrap());

    let payload = serde_json::to_vec(&Payload {
        query_number: Some(query_number),
        datasource: DataSource::Payload(sync),
        uuid: serde_json::from_str(resp["uuid"].as_str().unwrap())?,
        encoding: serde_json::from_str(resp["encoding"].as_str().unwrap())?,
        ..Default::default()
    })?
    .into();

    info!("[OK] Invoking NEXMark worker function: {}", function_name);
    let resp = serde_json::to_value(
        &invoke_lambda_function(
            function_name.clone(),
            Some(payload),
            FLOCK_LAMBDA_SYNC_CALL.to_string(),
        )
        .await?
        .payload
        .expect("No response"),
    )?;
    info!("[OK] Received response: {:?}", resp);
    let end_time = SystemTime::now();

    info!(
        "[OK] The NEXMark benchmark [S3] took {} milliseconds",
        end_time.duration_since(start_time).unwrap().as_millis()
    );
    Ok(())
}
