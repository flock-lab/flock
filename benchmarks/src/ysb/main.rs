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

#[path = "./centralized.rs"]
mod centralized;

#[path = "./distributed.rs"]
mod distributed;

use datafusion::arrow::datatypes::SchemaRef;
use flock::prelude::*;
use lazy_static::lazy_static;
use std::sync::Arc;
use structopt::StructOpt;
use ysb::event::{AdEvent, Campaign};
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

    /// Distributed mode or not
    #[structopt(short = "d", long = "distributed")]
    pub distributed: bool,

    /// The state backend to use
    #[structopt(short = "b", long = "state_backend", default_value = "hashmap")]
    pub state_backend: String,

    /// The target partitions to use in Arrow DataFusion.
    /// This is only used in distributed mode.
    #[structopt(short = "p", long = "target_partitions", default_value = "8")]
    pub target_partitions: usize,
}

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<()> {
    env_logger::init();
    ysb_benchmark(&mut YSBBenchmarkOpt::from_args()).await?;
    Ok(())
}

fn create_ysb_source(opt: &YSBBenchmarkOpt) -> YSBSource {
    let window = Window::Tumbling(Schedule::Seconds(10));
    YSBSource::new(opt.seconds, opt.generators, opt.events_per_second, window)
}

pub async fn ysb_benchmark(opt: &mut YSBBenchmarkOpt) -> Result<()> {
    if opt.distributed {
        distributed::ysb_benchmark(opt).await
    } else {
        centralized::ysb_benchmark(opt).await
    }
}

/// Returns YSB query string.
fn ysb_query() -> String {
    include_str!("ysb.sql").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use flock::transmute::event_bytes_to_batch;
    use ysb::register_ysb_tables;

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
            .feed_data_sources(vec![
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
