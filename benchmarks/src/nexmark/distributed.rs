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

use super::create_nexmark_source;
use super::create_physical_plans;
use crate::NexmarkBenchmarkOpt;

use flock::prelude::*;
use lazy_static::lazy_static;
use log::info;
use nexmark::register_nexmark_tables;
use rainbow::rainbow_println;

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
    let query_code = format!("q{}", opt.query_number);
    let _nexmark_conf = create_nexmark_source(opt).await?;

    let mut ctx = register_nexmark_tables().await?;
    let plans = create_physical_plans(&mut ctx, query_number).await?;
    let plan = plans.last().unwrap().clone();
    let sink_type = DataSinkType::new(&opt.data_sink_type)?;

    let mut launcher = AwsLambdaLauncher::try_new(query_code, plan, sink_type).await?;
    launcher.create_cloud_contexts()?;
    let _dag = launcher.dag;

    Ok(())
}
