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

//! This is a macrobenchmark to measure the performance of relational operators
//! on x86_64 and arm64 architectures.
//!
//! The benchmark is run on a single cloud function. The operators we benchmark
//! are:
//!
//! * hash join
//! * projection and filter
//! * sort
//! * aggregation (group by)

#[path = "../rainbow.rs"]
mod rainbow;

use flock::aws::{cloudwatch, lambda};
use flock::prelude::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use rainbow::{rainbow_println, rainbow_string};
use structopt::StructOpt;

lazy_static! {
    pub static ref ARCH_SOURCE_LOG_GROUP: String = "/aws/lambda/flock_datasource".to_string();
}

#[derive(Default, Clone, Debug, StructOpt)]
pub struct ArchBenchmarkOpt {
    /// Number of events generated
    #[structopt(short = "e", long = "events", default_value = "1000")]
    pub events: usize,

    /// The worker function's memory size
    #[structopt(short = "m", long = "memory_size", default_value = "128")]
    pub memory_size: i64,

    /// The system architecture to use
    #[structopt(short = "a", long = "arch", default_value = "x86_64")]
    pub architecture: String,
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    arch_benchmark(&mut ArchBenchmarkOpt::from_args()).await?;
    Ok(())
}

pub async fn arch_benchmark(opt: &mut ArchBenchmarkOpt) -> Result<()> {
    rainbow_println("================================================================");
    rainbow_println("                    Running the benchmark                       ");
    rainbow_println("================================================================");
    info!("Running the ARCH benchmark with the following options:\n");
    rainbow_println(format!("{:#?}\n", opt));

    let arch_source_ctx = ExecutionContext {
        plan: CloudExecutionPlan::new(vec![FLOCK_EMPTY_PLAN.clone()], None),
        name: FLOCK_DATA_SOURCE_FUNC_NAME.clone(),
        next: CloudFunction::Sink(DataSinkType::Blackhole),
        ..Default::default()
    };

    // Create the function for the arch benchmark.
    info!(
        "Creating lambda function: {}",
        rainbow_string(FLOCK_DATA_SOURCE_FUNC_NAME.clone())
    );
    lambda::create_function(&arch_source_ctx, opt.memory_size, &opt.architecture).await?;

    let p = serde_json::to_vec(&Payload {
        datasource: DataSource::Arch(opt.events),
        ..Default::default()
    })?
    .into();
    lambda::invoke_function(
        &FLOCK_DATA_SOURCE_FUNC_NAME,
        &FLOCK_LAMBDA_ASYNC_CALL,
        Some(p),
    )
    .await?;

    info!("Waiting for the current invocations to be logged.");
    tokio::time::sleep(parse_duration("5s").unwrap()).await;
    cloudwatch::fetch(&ARCH_SOURCE_LOG_GROUP, parse_duration("1min").unwrap()).await?;

    Ok(())
}
