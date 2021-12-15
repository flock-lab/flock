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
use bytes::Bytes;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext as DataFusionExecutionContext;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use driver::deploy::lambda;
use driver::logwatch::tail;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use nexmark::event::{Auction, Bid, Person};
use nexmark::NexMarkSource;
use runtime::prelude::*;
use rusoto_core::{ByteStream, Region};
use rusoto_lambda::{
    CreateFunctionRequest, FunctionCode, GetFunctionRequest, InvocationRequest, InvocationResponse,
    Lambda, LambdaClient, PutFunctionConcurrencyRequest, UpdateFunctionCodeRequest,
};
use rusoto_logs::CloudWatchLogsClient;
use rusoto_s3::{ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use std::collections::HashMap;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::task::JoinHandle;

lazy_static! {
    // AWS Services
    static ref FLOCK_LAMBDA_ASYNC_CALL: String = "Event".to_string();
    static ref FLOCK_LAMBDA_SYNC_CALL: String = "RequestResponse".to_string();

    static ref FLOCK_S3_KEY: String = FLOCK_CONF["flock"]["s3_key"].to_string();
    static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["flock"]["s3_bucket"].to_string();

    static ref FLOCK_S3_CLIENT: S3Client = S3Client::new(Region::default());
    static ref FLOCK_LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
    static ref FLOCK_WATCHLOGS_CLIENT: CloudWatchLogsClient = CloudWatchLogsClient::new(Region::default());

    static ref FLOCK_EMPTY_PLAN: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(false, Arc::new(Schema::empty())));
    static ref FLOCK_CONCURRENCY: usize = FLOCK_CONF["lambda"]["concurrency"].parse::<usize>().unwrap();
    static ref FLOCK_GRANULE_SIZE: usize = FLOCK_CONF["lambda"]["granule"].parse::<usize>().unwrap();

    // NEXMark Benchmark
    static ref NEXMARK_BID: SchemaRef = Arc::new(Bid::schema());
    static ref NEXMARK_PERSON: SchemaRef = Arc::new(Person::schema());
    static ref NEXMARK_AUCTION: SchemaRef = Arc::new(Auction::schema());
    static ref NEXMARK_SOURCE_FUNC_NAME: String = "nexmark_datasource".to_string();
    static ref NEXMARK_SOURCE_LOG_GROUP: String = "/aws/lambda/nexmark_datasource".to_string();
    static ref NEXMARK_Q4_S3_KEY: String = FLOCK_CONF["nexmark"]["q4_s3_key"].to_string();
    static ref NEXMARK_Q6_S3_KEY: String = FLOCK_CONF["nexmark"]["q6_s3_key"].to_string();
}

#[derive(Default, Clone, Debug, StructOpt)]
struct NexmarkBenchmarkOpt {
    /// Query number
    #[structopt(short = "q", long = "query_number", default_value = "1")]
    query_number: usize,

    /// Activate debug mode to see query results
    #[structopt(short, long)]
    debug: bool,

    /// Number of threads or generators of each test run
    #[structopt(short = "g", long = "generators", default_value = "100")]
    generators: usize,

    /// Number of threads to use for parallel execution
    #[structopt(short = "s", long = "seconds", default_value = "10")]
    seconds: usize,

    /// Number of events generated among generators per second
    #[structopt(short = "e", long = "events_per_second", default_value = "100000")]
    events_per_second: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    benchmark(NexmarkBenchmarkOpt::from_args()).await?;
    Ok(())
}

async fn register_nexmark_tables() -> Result<DataFusionExecutionContext> {
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

fn create_nexmark_source(opt: &NexmarkBenchmarkOpt) -> NexMarkSource {
    let window = match opt.query_number {
        0 | 1 | 2 | 3 | 4 | 6 | 9 | 13 => StreamWindow::ElementWise,
        5 => StreamWindow::HoppingWindow((10, 5)),
        7..=8 => StreamWindow::TumblingWindow(Schedule::Seconds(10)),
        _ => unreachable!(),
    };
    NexMarkSource::new(opt.seconds, opt.generators, opt.events_per_second, window)
}

async fn plan_placement(
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
            match FLOCK_S3_CLIENT
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
                Some(0) => {
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
                _ => {}
            }
            Ok((FLOCK_EMPTY_PLAN.clone(), Some((s3_bucket, s3_key))))
        }
        _ => Ok((physcial_plan, None)),
    }
}

/// Create lambda functions for a given NexMark query.
/// The returned function is the worker group as a whole which will be executed
/// by the NexmarkBenchmark data generator function.
async fn create_nexmark_functions(
    opt: NexmarkBenchmarkOpt,
    window: StreamWindow,
    physcial_plan: Arc<dyn ExecutionPlan>,
) -> Result<CloudFunction> {
    let worker_func_name = format!("q{}-00", opt.query_number);

    let next_func_name =
        if window != StreamWindow::ElementWise || opt.events_per_second > *FLOCK_GRANULE_SIZE * 2 {
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
        datasource:  DataSource::NexMarkEvent(NexMarkSource::default()),
    };

    let mut nexmark_worker_ctx = ExecutionContext {
        plan:        plan,
        plan_s3_idx: s3.clone(),
        name:        worker_func_name.clone(),
        next:        CloudFunction::None,
        datasource:  DataSource::Payload,
    };

    // Create the function for the nexmark source generator.
    info!(
        "Creating lambda function: {}",
        NEXMARK_SOURCE_FUNC_NAME.clone()
    );
    create_lambda_function(&nexmark_source_ctx, opt.debug).await?;

    // Create the function for the nexmark worker.
    match &next_func_name {
        CloudFunction::Lambda(name) => {
            info!("Creating lambda function: {}", name);
            create_lambda_function(&nexmark_worker_ctx, opt.debug).await?;
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
                create_lambda_function(&nexmark_worker_ctx, opt.debug).await?;
                set_lambda_concurrency(nexmark_worker_ctx.name, 1).await?;
            }
        }
        CloudFunction::None => unreachable!(),
    }

    Ok(next_func_name)
}

async fn benchmark(opt: NexmarkBenchmarkOpt) -> Result<()> {
    info!("Running benchmarks with the following options: {:?}", opt);
    let nexmark_conf = create_nexmark_source(&opt);
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
    metadata.insert(format!("workers"), serde_json::to_string(&root_actor)?);

    let tasks = (0..opt.generators)
        .into_iter()
        .map(|i| {
            let f = NEXMARK_SOURCE_FUNC_NAME.clone();
            let s = nexmark_conf.clone();
            let m = metadata.clone();
            tokio::spawn(async move {
                info!("[OK] Invoke function: {} {}", f, i);
                let p = serde_json::to_vec(&Payload {
                    datasource: Some(DataSource::NexMarkEvent(s)),
                    query_number: Some(query_number),
                    metadata: Some(m),
                    ..Default::default()
                })?
                .into();
                Ok(invoke_lambda_function(f, Some(p)).await?)
            })
        })
        // this collect *is needed* so that the join below can switch between tasks.
        .collect::<Vec<JoinHandle<Result<InvocationResponse>>>>();

    for task in tasks {
        let response = task.await.expect("Lambda function execution failed.")?;
        info!("[OK] Received status from function. {:?}", response);
    }

    info!("Waiting for the current invocations to be logged.");
    tokio::time::sleep(parse_duration("5s").unwrap()).await;
    fetch_watchlogs(&NEXMARK_SOURCE_LOG_GROUP, parse_duration("20s").unwrap()).await?;

    Ok(())
}

async fn fetch_watchlogs(group: &String, mtime: std::time::Duration) -> Result<()> {
    let mut logged = false;
    let timeout = parse_duration("1min").unwrap();
    let sleep_for = parse_duration("5s").ok();
    let mut token: Option<String> = None;
    let mut req = tail::create_filter_request(&group, mtime, None, token);
    loop {
        if logged {
            break;
        }

        match tail::fetch_logs(&FLOCK_WATCHLOGS_CLIENT, req, timeout)
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?
        {
            tail::AWSResponse::Token(x) => {
                info!("Got a Token response");
                logged = true;
                token = Some(x);
                req = tail::create_filter_request(&group, mtime, None, token);
            }
            tail::AWSResponse::LastLog(t) => match sleep_for {
                Some(x) => {
                    info!("Got a lastlog response");
                    token = None;
                    req = tail::create_filter_from_timestamp(&group, t, None, token);
                    info!("Waiting {:?} before requesting logs again...", x);
                    tokio::time::sleep(x).await;
                }
                None => break,
            },
        };
    }

    Ok(())
}

/// Invoke the lambda function with the nexmark events.
async fn invoke_lambda_function(
    function_name: String,
    payload: Option<Bytes>,
) -> Result<InvocationResponse> {
    match FLOCK_LAMBDA_CLIENT
        .invoke(InvocationRequest {
            function_name,
            payload,
            invocation_type: Some(FLOCK_LAMBDA_ASYNC_CALL.clone()),
            ..Default::default()
        })
        .await
    {
        Ok(response) => return Ok(response),
        Err(err) => {
            return Err(FlockError::Execution(format!(
                "Lambda function execution failure: {}",
                err
            )))
        }
    }
}

/// Set the lambda function's concurrency.
/// <https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html>
async fn set_lambda_concurrency(function_name: String, concurrency: i64) -> Result<()> {
    let request = PutFunctionConcurrencyRequest {
        function_name,
        reserved_concurrent_executions: concurrency,
    };
    let concurrency = FLOCK_LAMBDA_CLIENT
        .put_function_concurrency(request)
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?;
    assert_ne!(concurrency.reserved_concurrent_executions, Some(0));
    Ok(())
}

/// Creates a single lambda function using bootstrap.zip in Amazon S3.
async fn create_lambda_function(ctx: &ExecutionContext, debug: bool) -> Result<String> {
    let func_name = ctx.name.clone();
    if FLOCK_LAMBDA_CLIENT
        .get_function(GetFunctionRequest {
            function_name: ctx.name.clone(),
            ..Default::default()
        })
        .await
        .is_ok()
    {
        let conf = FLOCK_LAMBDA_CLIENT
            .update_function_code(UpdateFunctionCodeRequest {
                function_name: func_name.clone(),
                s3_bucket: Some(FLOCK_S3_BUCKET.clone()),
                s3_key: Some(FLOCK_S3_KEY.clone()),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?;
        conf.function_name
            .ok_or_else(|| FlockError::Internal("No function name!".to_string()))
    } else {
        let conf = FLOCK_LAMBDA_CLIENT
            .create_function(CreateFunctionRequest {
                code: FunctionCode {
                    s3_bucket: Some(FLOCK_S3_BUCKET.clone()),
                    s3_key: Some(FLOCK_S3_KEY.clone()),
                    ..Default::default()
                },
                function_name: func_name.clone(),
                handler: lambda::handler(),
                role: lambda::role().await,
                runtime: lambda::runtime(),
                environment: lambda::environment(&ctx, debug),
                timeout: Some(900),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?;
        conf.function_name
            .ok_or_else(|| FlockError::Internal("No function name!".to_string()))
    }
}

/// Returns Nextmark query strings based on the query number.
fn nexmark_query(query_number: usize) -> String {
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
            let plan = physical_plan(&mut ctx, &sql)?;
            let mut flock_ctx = ExecutionContext {
                plan,
                ..Default::default()
            };

            flock_ctx
                .feed_data_sources(&vec![
                    vec![NexMarkSource::to_batch(&event.bids, NEXMARK_BID.clone())],
                    vec![NexMarkSource::to_batch(
                        &event.persons,
                        NEXMARK_PERSON.clone(),
                    )],
                    vec![NexMarkSource::to_batch(
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
