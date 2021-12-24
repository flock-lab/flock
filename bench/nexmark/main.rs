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

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext as DataFusionExecutionContext;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use driver::deploy::common::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use nexmark::event::{Auction, Bid, Person};
use nexmark::NEXMarkSource;
use runtime::prelude::*;
use rusoto_core::{ByteStream, Region};
use rusoto_lambda::InvocationResponse;
use rusoto_s3::{ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use std::collections::HashMap;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::task::JoinHandle;

lazy_static! {
    pub static ref FLOCK_S3_KEY: String = FLOCK_CONF["flock"]["s3_key"].to_string();
    pub static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["flock"]["s3_bucket"].to_string();
    pub static ref FLOCK_S3_CLIENT: S3Client = S3Client::new(Region::default());

    pub static ref FLOCK_EMPTY_PLAN: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(false, Arc::new(Schema::empty())));
    pub static ref FLOCK_CONCURRENCY: usize = FLOCK_CONF["lambda"]["concurrency"].parse::<usize>().unwrap();

    // NEXMark Benchmark
    pub static ref NEXMARK_BID: SchemaRef = Arc::new(Bid::schema());
    pub static ref NEXMARK_PERSON: SchemaRef = Arc::new(Person::schema());
    pub static ref NEXMARK_AUCTION: SchemaRef = Arc::new(Auction::schema());
    pub static ref NEXMARK_SOURCE_FUNC_NAME: String = "flock_datasource".to_string();
    pub static ref NEXMARK_SOURCE_LOG_GROUP: String = "/aws/lambda/flock_datasource".to_string();
    pub static ref NEXMARK_Q4_S3_KEY: String = FLOCK_CONF["nexmark"]["q4_s3_key"].to_string();
    pub static ref NEXMARK_Q6_S3_KEY: String = FLOCK_CONF["nexmark"]["q6_s3_key"].to_string();
}

#[derive(Default, Clone, Debug, StructOpt)]
pub struct NexmarkBenchmarkOpt {
    /// Query number
    #[structopt(short = "q", long = "query_number", default_value = "1")]
    pub query_number: usize,

    /// Activate debug mode to see query results
    #[structopt(short, long)]
    pub debug: bool,

    /// Number of threads or generators of each test run
    #[structopt(short = "g", long = "generators", default_value = "100")]
    pub generators: usize,

    /// Number of threads to use for parallel execution
    #[structopt(short = "s", long = "seconds", default_value = "10")]
    pub seconds: usize,

    /// Number of events generated among generators per second
    #[structopt(short = "e", long = "events_per_second", default_value = "100000")]
    pub events_per_second: usize,

    /// The data sink type to use
    #[structopt(short = "d", long = "data_sink_type", default_value = "0")]
    pub data_sink_type: usize,

    /// The function invocation mode to use
    #[structopt(long = "async")]
    pub async_type: bool,

    /// The worker function's memory size
    #[structopt(short = "m", long = "memory_size", default_value = "128")]
    pub memory_size: i64,
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    benchmark(NexmarkBenchmarkOpt::from_args()).await?;
    Ok(())
}

pub async fn register_nexmark_tables() -> Result<DataFusionExecutionContext> {
    let mut ctx = DataFusionExecutionContext::new();
    let person_schema = Arc::new(Person::schema());
    let person_table = MemTable::try_new(
        person_schema.clone(),
        vec![vec![RecordBatch::new_empty(person_schema)]],
    )?;
    ctx.register_table("person", Arc::new(person_table))?;

    let auction_schema = Arc::new(Auction::schema());
    let auction_table = MemTable::try_new(
        auction_schema.clone(),
        vec![vec![RecordBatch::new_empty(auction_schema)]],
    )?;
    ctx.register_table("auction", Arc::new(auction_table))?;

    let bid_schema = Arc::new(Bid::schema());
    let bid_table = MemTable::try_new(
        bid_schema.clone(),
        vec![vec![RecordBatch::new_empty(bid_schema)]],
    )?;
    ctx.register_table("bid", Arc::new(bid_table))?;

    Ok(ctx)
}

pub fn create_nexmark_source(opt: &NexmarkBenchmarkOpt) -> NEXMarkSource {
    let window = match opt.query_number {
        0 | 1 | 2 | 3 | 4 | 6 | 9 | 13 => StreamWindow::ElementWise,
        5 => StreamWindow::HoppingWindow((10, 5)),
        7..=8 => StreamWindow::TumblingWindow(Schedule::Seconds(10)),
        _ => unreachable!(),
    };
    NEXMarkSource::new(opt.seconds, opt.generators, opt.events_per_second, window)
}

pub async fn plan_placement(
    query_number: usize,
    physcial_plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, Option<(String, String)>)> {
    match query_number {
        4 | 6 => {
            let (s3_bucket, s3_key) = match query_number {
                4 => (FLOCK_S3_BUCKET.clone(), NEXMARK_Q4_S3_KEY.clone()),
                6 => (FLOCK_S3_BUCKET.clone(), NEXMARK_Q6_S3_KEY.clone()),
                _ => unreachable!(),
            };
            if let Some(0) = FLOCK_S3_CLIENT
                .list_objects_v2(ListObjectsV2Request {
                    bucket: s3_bucket.clone(),
                    prefix: Some(s3_key.clone()),
                    max_keys: Some(1),
                    ..Default::default()
                })
                .await
                .map_err(|e| FlockError::Internal(e.to_string()))?
                .key_count
            {
                FLOCK_S3_CLIENT
                    .put_object(PutObjectRequest {
                        bucket: s3_bucket.clone(),
                        key: s3_key.clone(),
                        body: Some(ByteStream::from(serde_json::to_vec(&physcial_plan)?)),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| FlockError::Internal(e.to_string()))?;
            }
            Ok((FLOCK_EMPTY_PLAN.clone(), Some((s3_bucket, s3_key))))
        }
        _ => Ok((physcial_plan, None)),
    }
}

/// Create lambda functions for a given NexMark query.
/// The returned function is the worker group as a whole which will be executed
/// by the NexmarkBenchmark data generator function.
pub async fn create_nexmark_functions(
    opt: NexmarkBenchmarkOpt,
    window: StreamWindow,
    physcial_plan: Arc<dyn ExecutionPlan>,
) -> Result<CloudFunction> {
    let worker_func_name = format!("q{}-00", opt.query_number);

    let granule_size = if opt.async_type {
        *FLOCK_ASYNC_GRANULE_SIZE * 2
    } else {
        *FLOCK_SYNC_GRANULE_SIZE * 2
    };

    let next_func_name =
        if window != StreamWindow::ElementWise || opt.events_per_second > granule_size {
            CloudFunction::Group((worker_func_name.clone(), *FLOCK_CONCURRENCY))
        } else {
            CloudFunction::Lambda(worker_func_name.clone())
        };

    let (plan, s3) = plan_placement(opt.query_number, physcial_plan).await?;
    let nexmark_source_ctx = ExecutionContext {
        plan:        FLOCK_EMPTY_PLAN.clone(),
        plan_s3_idx: s3.clone(),
        name:        NEXMARK_SOURCE_FUNC_NAME.clone(),
        next:        next_func_name.clone(),
    };

    let mut nexmark_worker_ctx = ExecutionContext {
        plan,
        plan_s3_idx: s3.clone(),
        name: worker_func_name.clone(),
        next: CloudFunction::Sink(DataSinkType::new(opt.data_sink_type)?),
    };

    // Create the function for the nexmark source generator.
    info!(
        "Creating lambda function: {}",
        NEXMARK_SOURCE_FUNC_NAME.clone()
    );
    create_lambda_function(&nexmark_source_ctx, Some(2048 /* MB */), opt.debug).await?;

    // Create the function for the nexmark worker.
    match &next_func_name {
        CloudFunction::Lambda(name) => {
            info!("Creating lambda function: {}", name);
            create_lambda_function(&nexmark_worker_ctx, Some(opt.memory_size), opt.debug).await?;
        }
        CloudFunction::Group((name, concurrency)) => {
            info!(
                "Creating lambda function group: {:?}",
                nexmark_source_ctx.next
            );
            for i in 0..*concurrency {
                let group_member_name = format!("{}-{:02}", name.clone(), i);
                info!("Creating function member: {}", group_member_name);
                nexmark_worker_ctx.name = group_member_name;
                create_lambda_function(&nexmark_worker_ctx, Some(opt.memory_size), opt.debug)
                    .await?;
                set_lambda_concurrency(nexmark_worker_ctx.name, 1).await?;
            }
        }
        CloudFunction::Sink(_) => unreachable!(),
    }

    Ok(next_func_name)
}

#[allow(dead_code)]
async fn benchmark(opt: NexmarkBenchmarkOpt) -> Result<()> {
    info!(
        "Running the NEXMark benchmark with the following options: {:?}",
        opt
    );
    let nexmark_conf = create_nexmark_source(&opt);
    let query_number = opt.query_number;

    let mut ctx = register_nexmark_tables().await?;
    let plan = physical_plan(&mut ctx, &nexmark_query(query_number)).await?;
    let root_actor =
        create_nexmark_functions(opt.clone(), nexmark_conf.window.clone(), plan).await?;

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
            let f = NEXMARK_SOURCE_FUNC_NAME.clone();
            let s = nexmark_conf.clone();
            let m = metadata.clone();
            tokio::spawn(async move {
                info!(
                    "[OK] Invoking NEXMark source function: {} by generator {}",
                    f, i
                );
                let p = serde_json::to_vec(&Payload {
                    datasource: DataSource::NEXMarkEvent(s),
                    query_number: Some(query_number),
                    metadata: Some(m),
                    ..Default::default()
                })?
                .into();
                invoke_lambda_function(f, Some(p), FLOCK_LAMBDA_ASYNC_CALL.to_string()).await
            })
        })
        // this collect *is needed* so that the join below can switch between tasks.
        .collect::<Vec<JoinHandle<Result<InvocationResponse>>>>();

    futures::future::join_all(tasks).await;

    info!("Waiting for the current invocations to be logged.");
    tokio::time::sleep(parse_duration("5s").unwrap()).await;
    fetch_aws_watchlogs(&NEXMARK_SOURCE_LOG_GROUP, parse_duration("1min").unwrap()).await?;

    let sink_type = DataSinkType::new(opt.data_sink_type)?;
    if sink_type != DataSinkType::Empty {
        let data_sink = DataSink::read(format!("q{}", opt.query_number), sink_type).await?;
        let (last_function, batches) = data_sink.to_record_batch()?;
        info!(
            "[OK] Received {} batches from the data sink.",
            batches.len()
        );
        info!("[OK] Last data sink function: {}", last_function);
        let function_log_group = format!("/aws/lambda/{}", last_function);
        fetch_aws_watchlogs(&function_log_group, parse_duration("1min").unwrap()).await?;
        println!("{}", pretty_format_batches(&batches)?);
    }

    Ok(())
}

/// Returns Nextmark query strings based on the query number.
pub fn nexmark_query(query_number: usize) -> String {
    match query_number {
        0 => include_str!("query/q0.sql"),
        1 => include_str!("query/q1.sql"),
        2 => include_str!("query/q2.sql"),
        3 => include_str!("query/q3.sql"),
        4 => include_str!("query/q4.sql"),
        5 => include_str!("query/q5.sql"),
        6 => include_str!("query/q6.sql"),
        7 => include_str!("query/q7.sql"),
        8 => include_str!("query/q8.sql"),
        9 => include_str!("query/q9.sql"),
        _ => unreachable!(),
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::pretty::pretty_format_batches;

    #[tokio::test]
    async fn nexmark_sql_queries() -> Result<()> {
        let opt = NexmarkBenchmarkOpt {
            generators: 1,
            seconds: 5,
            events_per_second: 10_000,
            ..Default::default()
        };
        let conf = create_nexmark_source(&opt);
        let (event, _) = Arc::new(conf.generate_data()?)
            .select(1, 0)
            .expect("Failed to select event.");

        let sqls = vec![
            nexmark_query(2),
            nexmark_query(3),
            nexmark_query(4),
            nexmark_query(5),
            nexmark_query(6),
            nexmark_query(7),
            nexmark_query(8),
            nexmark_query(9),
        ];
        let mut ctx = register_nexmark_tables().await?;
        for sql in sqls {
            let plan = physical_plan(&mut ctx, &sql).await?;
            let mut flock_ctx = ExecutionContext {
                plan,
                ..Default::default()
            };

            flock_ctx
                .feed_data_sources(&vec![
                    vec![NEXMarkSource::to_batch(&event.bids, NEXMARK_BID.clone())],
                    vec![NEXMarkSource::to_batch(
                        &event.persons,
                        NEXMARK_PERSON.clone(),
                    )],
                    vec![NEXMarkSource::to_batch(
                        &event.auctions,
                        NEXMARK_AUCTION.clone(),
                    )],
                ])
                .await?;

            let output = flock_ctx.execute().await?;
            println!("{}", pretty_format_batches(&output)?);
        }
        Ok(())
    }
}
