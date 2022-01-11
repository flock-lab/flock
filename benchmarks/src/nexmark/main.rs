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
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext as DataFusionExecutionContext;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use flock::aws::{cloudwatch, efs, lambda, s3};
use flock::prelude::*;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use nexmark::event::{side_input_schema, Auction, Bid, Person};
use nexmark::register_nexmark_tables;
use nexmark::NEXMarkSource;
use rainbow::{rainbow_println, rainbow_string};
use rusoto_lambda::InvocationResponse;
use std::collections::HashMap;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::task::JoinHandle;

static SIDE_INPUT_DOWNLOAD_URL: &str = concat!(
    "https://gist.githubusercontent.com/gangliao/",
    "de6f544b8a93f26081036e0a7f8c1715/raw/",
    "586c88ad6f89d12c9f1753622eddf4788f6f0f9d/",
    "nexmark_q13_side_input.csv"
);

lazy_static! {
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
    pub static ref NEXMARK_Q13_S3_SIDE_INPUT_KEY: String = FLOCK_CONF["nexmark"]["q13_s3_side_input_key"].to_string();
}

#[derive(Default, Clone, Debug, StructOpt)]
pub struct NexmarkBenchmarkOpt {
    /// Query number
    #[structopt(short = "q", long = "query_number", default_value = "3")]
    pub query_number: usize,

    /// Number of threads or generators of each test run
    #[structopt(short = "g", long = "generators", default_value = "1")]
    pub generators: usize,

    /// Number of threads to use for parallel execution
    #[structopt(short = "s", long = "seconds", default_value = "10")]
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

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    nexmark_benchmark(&mut NexmarkBenchmarkOpt::from_args()).await?;
    Ok(())
}

pub async fn create_nexmark_source(opt: &mut NexmarkBenchmarkOpt) -> Result<NEXMarkSource> {
    let window = match opt.query_number {
        0..=4 | 6 | 9 | 10 | 13 => Window::ElementWise,
        5 => Window::Hopping((10, 5)),
        7..=8 => Window::Tumbling(Schedule::Seconds(10)),
        11 => Window::Session(Schedule::Seconds(10)),
        12 => Window::Global(Schedule::Seconds(10)),
        _ => unreachable!(),
    };

    if opt.query_number == 10 {
        opt.data_sink_type = "s3".to_string();
    }

    if opt.query_number == 13 {
        let data = reqwest::get(SIDE_INPUT_DOWNLOAD_URL)
            .await
            .map_err(|_| "Failed to download side input data")?
            .text_with_charset("utf-8")
            .await
            .map_err(|_| "Failed to read side input data")?;
        s3::put_object_if_missing(
            &FLOCK_S3_BUCKET,
            &NEXMARK_Q13_S3_SIDE_INPUT_KEY,
            data.as_bytes().to_vec(),
        )
        .await?;
    }

    Ok(NEXMarkSource::new(
        opt.seconds,
        opt.generators,
        opt.events_per_second,
        window,
    ))
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
            s3::put_object_if_missing(&s3_bucket, &s3_key, serde_json::to_vec(&physcial_plan)?)
                .await?;
            Ok((FLOCK_EMPTY_PLAN.clone(), Some((s3_bucket, s3_key))))
        }
        _ => Ok((physcial_plan, None)),
    }
}

/// Create lambda functions for a given NexMark query.
/// The returned function is the worker group as a whole which will be executed
/// by the NexmarkBenchmark data generator function.
pub async fn create_nexmark_functions(
    opt: &NexmarkBenchmarkOpt,
    window: Window,
    physcial_plan: Arc<dyn ExecutionPlan>,
) -> Result<CloudFunction> {
    let worker_func_name = format!("q{}-00", opt.query_number);

    let granule_size = if opt.async_type {
        *FLOCK_ASYNC_GRANULE_SIZE * 2
    } else {
        *FLOCK_SYNC_GRANULE_SIZE * 2
    };

    let next_func_name = if window != Window::ElementWise || opt.events_per_second > granule_size {
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

    let nexmark_worker_ctx = ExecutionContext {
        plan,
        plan_s3_idx: s3.clone(),
        name: worker_func_name.clone(),
        next: CloudFunction::Sink(DataSinkType::new(&opt.data_sink_type)?),
    };

    // Create the function for the nexmark source generator.
    info!(
        "Creating lambda function: {}",
        rainbow_string(NEXMARK_SOURCE_FUNC_NAME.clone())
    );
    lambda::create_function(&nexmark_source_ctx, 2048 /* MB */, &opt.architecture).await?;

    // Create the function for the nexmark worker.
    match next_func_name.clone() {
        CloudFunction::Lambda(name) => {
            info!("Creating lambda function: {}", rainbow_string(name));
            lambda::create_function(&nexmark_worker_ctx, opt.memory_size, &opt.architecture)
                .await?;
        }
        CloudFunction::Group((name, concurrency)) => {
            info!(
                "Creating lambda function group: {}",
                rainbow_string(format!("{:?}", nexmark_source_ctx.next))
            );

            let tasks = (0..concurrency)
                .into_iter()
                .map(|i| {
                    let mut worker_ctx = nexmark_worker_ctx.clone();
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
        CloudFunction::Sink(_) => unreachable!(),
    }

    Ok(next_func_name)
}

/// Create an Elastic file system access point for Flock.
#[allow(dead_code)]
async fn create_file_system() -> Result<String> {
    let mut efs_id = efs::create().await?;
    if efs_id.is_empty() {
        efs_id = efs::discribe().await?;
    }
    info!("[OK] Creating AWS Elastic File System: {}", efs_id);

    efs::create_mount_target(&efs_id).await?;
    info!("[OK] Creating AWS EFS Mount Target");

    let access_point_id = efs::create_access_point(&efs_id).await?;

    let access_point_arn = if access_point_id.is_empty() {
        efs::describe_access_point(None, Some(efs_id))
            .await
            .map_err(|e| FlockError::AWS(format!("{}", e)))?
    } else {
        efs::describe_access_point(Some(access_point_id), None)
            .await
            .map_err(|e| FlockError::AWS(format!("{}", e)))?
    };
    info!("[OK] Creating AWS EFS Access Point: {}", access_point_arn);

    Ok(access_point_arn)
}

/// Create the physical plans according to the given query number.
pub async fn create_physical_plans(
    ctx: &mut DataFusionExecutionContext,
    query_number: usize,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    let mut plans = vec![];
    plans.push(physical_plan(ctx, &nexmark_query(query_number)[0]).await?);

    if query_number == 12 {
        ctx.deregister_table("bid")?;
        let bid_schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::Int32, false),
            Field::new("bidder", DataType::Int32, false),
            Field::new("price", DataType::Int32, false),
            Field::new(
                "b_date_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "p_time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                false,
            ),
        ]));
        let bid_table = MemTable::try_new(
            bid_schema.clone(),
            vec![vec![RecordBatch::new_empty(bid_schema)]],
        )?;
        ctx.register_table("bid", Arc::new(bid_table))?;
        plans.push(physical_plan(ctx, &nexmark_query(query_number)[1]).await?);
    }

    Ok(plans)
}

pub async fn add_extra_metadata(
    opt: &NexmarkBenchmarkOpt,
    metadata: &mut HashMap<String, String>,
) -> Result<()> {
    metadata.insert(
        "invocation_type".to_string(),
        if opt.async_type {
            "async".to_string()
        } else {
            "sync".to_string()
        },
    );

    if opt.query_number == 12 {
        metadata.insert(
            "add_process_time_query".to_string(),
            nexmark_query(opt.query_number)[0].clone(),
        );
    }

    if opt.query_number == 11 || opt.query_number == 12 {
        metadata.insert("session_key".to_string(), "bidder".to_string());
        metadata.insert("session_name".to_string(), "bid".to_string());
    }

    if opt.query_number == 13 {
        metadata.insert(
            "side_input_s3_key".to_string(),
            NEXMARK_Q13_S3_SIDE_INPUT_KEY.clone(),
        );
        metadata.insert("side_input_format".to_string(), "csv".to_string());

        let side_input_schema = Arc::new(side_input_schema());
        metadata.insert(
            "side_input_schema".to_string(),
            base64::encode(schema_to_bytes(side_input_schema)),
        );
    }

    Ok(())
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
                    rainbow_string(&*NEXMARK_SOURCE_FUNC_NAME),
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
                    &NEXMARK_SOURCE_FUNC_NAME,
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

/// Returns Nextmark query strings based on the query number.
pub fn nexmark_query(query_number: usize) -> Vec<String> {
    match query_number {
        0 => vec![include_str!("query/q0.sql")],
        1 => vec![include_str!("query/q1.sql")],
        2 => vec![include_str!("query/q2.sql")],
        3 => vec![include_str!("query/q3.sql")],
        4 => vec![include_str!("query/q4.sql")],
        5 => vec![include_str!("query/q5.sql")],
        6 => vec![include_str!("query/q6.sql")],
        7 => vec![include_str!("query/q7.sql")],
        8 => vec![include_str!("query/q8.sql")],
        9 => vec![include_str!("query/q9.sql")],
        10 => vec![include_str!("query/q10.sql")],
        11 => vec![include_str!("query/q11.sql")],
        12 => include_str!("query/q12.sql").split(';').collect(),
        13 => vec![include_str!("query/q13.sql")],
        _ => unreachable!(),
    }
    .into_iter()
    .map(String::from)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use flock::transmute::event_bytes_to_batch;
    use std::fs::File;
    use std::io::Write;
    use std::time::Instant;

    #[tokio::test]
    async fn nexmark_sql_queries() -> Result<()> {
        let mut opt = NexmarkBenchmarkOpt {
            generators: 1,
            seconds: 5,
            events_per_second: 1000,
            ..Default::default()
        };
        let conf = create_nexmark_source(&mut opt).await?;
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
            nexmark_query(10),
            nexmark_query(11),
            nexmark_query(12),
            nexmark_query(13),
        ];
        let ctx = register_nexmark_tables().await?;
        for sql in sqls {
            let plan = physical_plan(&ctx, &sql[0]).await?;
            let mut flock_ctx = ExecutionContext {
                plan,
                ..Default::default()
            };

            flock_ctx
                .feed_data_sources(&[
                    vec![event_bytes_to_batch(&event.bids, NEXMARK_BID.clone(), 1024)],
                    vec![event_bytes_to_batch(
                        &event.persons,
                        NEXMARK_PERSON.clone(),
                        1024,
                    )],
                    vec![event_bytes_to_batch(
                        &event.auctions,
                        NEXMARK_AUCTION.clone(),
                        1024,
                    )],
                ])
                .await?;

            let output = flock_ctx.execute().await?;
            println!("{}", pretty_format_batches(&output)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn nexmark_display_graphviz() -> Result<()> {
        let sqls = vec![
            nexmark_query(0),
            nexmark_query(1),
            nexmark_query(2),
            nexmark_query(3),
            nexmark_query(4),
            nexmark_query(5),
            nexmark_query(6),
            nexmark_query(7),
            nexmark_query(8),
            nexmark_query(9),
            nexmark_query(10),
            nexmark_query(11),
            nexmark_query(13),
            // keep q12 as the last one since it has new field in the `bid` table.
            nexmark_query(12),
        ];
        let mut ctx = register_nexmark_tables().await?;

        for (i, sql) in sqls.iter().enumerate() {
            let mut query = &sql[0];
            let mut query_number = i;
            if i == 12 {
                query_number += 1;
            } else if i == 13 {
                query_number -= 1;
            }

            if query_number == 12 {
                query = &sql[1];
                ctx.deregister_table("bid")?;
                let bid_schema = Arc::new(Schema::new(vec![
                    Field::new("auction", DataType::Int32, false),
                    Field::new("bidder", DataType::Int32, false),
                    Field::new("price", DataType::Int32, false),
                    Field::new(
                        "b_date_time",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new(
                        "p_time",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                        false,
                    ),
                ]));
                let bid_table = MemTable::try_new(
                    bid_schema.clone(),
                    vec![vec![RecordBatch::new_empty(bid_schema)]],
                )?;
                ctx.register_table("bid", Arc::new(bid_table))?;
            }

            let logical_plan = ctx.create_logical_plan(query)?;
            let logical_plan = ctx.optimize(&logical_plan)?;

            let mut output = File::create(format!("/tmp/q{}_plan.dot", query_number))?;
            write!(output, "{}", logical_plan.display_graphviz())?;

            let mut output = File::create(format!("/tmp/q{}_plan.fmt", query_number))?;
            write!(output, "{}", logical_plan.display_indent_schema())?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn nexmark_gen_physical_plan_time() -> Result<()> {
        let sqls = vec![
            nexmark_query(0),
            nexmark_query(1),
            nexmark_query(2),
            nexmark_query(3),
            nexmark_query(4),
            nexmark_query(5),
            nexmark_query(6),
            nexmark_query(7),
            nexmark_query(8),
            nexmark_query(9),
            nexmark_query(10),
            nexmark_query(11),
            nexmark_query(13),
            // keep q12 as the last one since it has new field in the `bid` table.
            nexmark_query(12),
        ];
        let mut ctx = register_nexmark_tables().await?;

        for (i, sql) in sqls.iter().enumerate() {
            let mut query = &sql[0];
            let mut query_number = i;
            if i == 12 {
                query_number += 1;
            } else if i == 13 {
                query_number -= 1;
            }

            if query_number == 12 {
                query = &sql[1];
                ctx.deregister_table("bid")?;
                let bid_schema = Arc::new(Schema::new(vec![
                    Field::new("auction", DataType::Int32, false),
                    Field::new("bidder", DataType::Int32, false),
                    Field::new("price", DataType::Int32, false),
                    Field::new(
                        "b_date_time",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new(
                        "p_time",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                        false,
                    ),
                ]));
                let bid_table = MemTable::try_new(
                    bid_schema.clone(),
                    vec![vec![RecordBatch::new_empty(bid_schema)]],
                )?;
                ctx.register_table("bid", Arc::new(bid_table))?;
            }

            let logical_plan = ctx.create_logical_plan(query)?;
            let logical_plan = ctx.optimize(&logical_plan)?;

            let now = Instant::now();
            ctx.create_physical_plan(&logical_plan).await?;
            println!(
                "q{} generates physical plan in {:?} us",
                query_number,
                now.elapsed().as_micros()
            );
        }
        Ok(())
    }
}
