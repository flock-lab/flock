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
use lazy_static::lazy_static;
use log::info;
use nexmark::event::{Auction, Bid, Person};
use nexmark::NexMarkSource;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{
    CreateFunctionRequest, FunctionCode, GetFunctionRequest, InvocationRequest, InvocationResponse,
    Lambda, LambdaClient, PutFunctionConcurrencyRequest, UpdateFunctionCodeRequest,
};
use std::sync::Arc;
use structopt::StructOpt;
use tokio::task::JoinHandle;

#[allow(dead_code)]
static LAMBDA_SYNC_CALL: &str = "RequestResponse";
#[allow(dead_code)]
static LAMBDA_ASYNC_CALL: &str = "Event";
static NEXMARK_SOURCE_FUNCTION_NAME: &str = "nexmark_datasource";

lazy_static! {
    static ref PERSON: SchemaRef = Arc::new(Person::schema());
    static ref AUCTION: SchemaRef = Arc::new(Auction::schema());
    static ref BID: SchemaRef = Arc::new(Bid::schema());
    static ref LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
    static ref PARALLELISM: usize = globals["lambda"]["parallelism"].parse::<usize>().unwrap();
    static ref S3_NEXMARK_BUCKET: String = globals["lambda"]["s3_bucket"].to_string();
    static ref S3_NEXMARK_Q6_PLAN_KEY: String = globals["lambda"]["s3_nexmark_q6_plan_key"].to_string();
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

fn plan_placement(
    query_number: usize,
    physcial_plan: Arc<dyn ExecutionPlan>,
) -> (Arc<dyn ExecutionPlan>, Option<(String, String)>) {
    match query_number {
        6 => (
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            Some((S3_NEXMARK_BUCKET.clone(), S3_NEXMARK_Q6_PLAN_KEY.clone())),
        ),
        _ => (physcial_plan, None),
    }
}

async fn create_nexmark_functions(
    opt: NexmarkBenchmarkOpt,
    nexmark_conf: NexMarkSource,
    physcial_plan: Arc<dyn ExecutionPlan>,
) -> Result<()> {
    let worker_func_name = format!("q{}-00", opt.query_number);

    let mut next_func_name = CloudFunction::Lambda(worker_func_name.clone());
    if nexmark_conf.window != StreamWindow::ElementWise {
        next_func_name = CloudFunction::Group((worker_func_name.clone(), *PARALLELISM));
    };

    let (plan, s3) = plan_placement(opt.query_number, physcial_plan);
    let nexmark_source_ctx = ExecutionContext {
        plan:         plan.clone(),
        plan_s3_idx:  s3.clone(),
        name:         NEXMARK_SOURCE_FUNCTION_NAME.to_string(),
        next:         next_func_name.clone(),
        datasource:   DataSource::NexMarkEvent(NexMarkSource::default()),
        query_number: Some(opt.query_number),
        debug:        opt.debug,
    };

    let mut nexmark_worker_ctx = ExecutionContext {
        plan:         plan,
        plan_s3_idx:  s3.clone(),
        name:         worker_func_name.clone(),
        next:         CloudFunction::None,
        datasource:   DataSource::Payload,
        query_number: Some(opt.query_number),
        debug:        opt.debug,
    };

    // Create the function for the nexmark source generator.
    info!("Creating lambda function: {}", NEXMARK_SOURCE_FUNCTION_NAME);
    create_lambda_function(&nexmark_source_ctx).await?;

    // Create the function for the nexmark worker.
    if nexmark_conf.window == StreamWindow::ElementWise {
        info!("Creating lambda function: {}", nexmark_worker_ctx.name);
        create_lambda_function(&nexmark_worker_ctx).await?;
    } else {
        info!(
            "Creating lambda function group: {:?}",
            nexmark_source_ctx.next
        );
        for i in 0..*PARALLELISM {
            let group_member_name = format!("{}-{:02}", worker_func_name.clone(), i);
            info!("Creating function member: {}", group_member_name);
            nexmark_worker_ctx.name = group_member_name;
            create_lambda_function(&nexmark_worker_ctx).await?;
            set_lambda_concurrency(nexmark_worker_ctx.name, 1).await?;
        }
    }

    Ok(())
}

async fn benchmark(opt: NexmarkBenchmarkOpt) -> Result<()> {
    info!("Running benchmarks with the following options: {:?}", opt);
    let nexmark_conf = create_nexmark_source(&opt);

    let mut ctx = register_nexmark_tables().await?;
    let plan = physical_plan(&mut ctx, &nexmark_query(opt.query_number))?;
    create_nexmark_functions(opt.clone(), nexmark_conf.clone(), plan).await?;

    let tasks = (0..opt.generators)
        .into_iter()
        .map(|i| {
            let f = NEXMARK_SOURCE_FUNCTION_NAME.to_string();
            let s = nexmark_conf.clone();
            tokio::spawn(async move {
                info!("[OK] Invoke function: {} {}", f, i);
                let p = serde_json::to_vec(&Payload {
                    datasource: Some(DataSource::NexMarkEvent(s)),
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

    Ok(())
}

/// Invoke the lambda function with the nexmark events.
async fn invoke_lambda_function(
    function_name: String,
    payload: Option<Bytes>,
) -> Result<InvocationResponse> {
    match LAMBDA_CLIENT
        .invoke(InvocationRequest {
            function_name,
            payload,
            invocation_type: Some(LAMBDA_ASYNC_CALL.to_string()),
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
    let concurrency = LAMBDA_CLIENT
        .put_function_concurrency(request)
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?;
    assert_ne!(concurrency.reserved_concurrent_executions, Some(0));
    Ok(())
}

/// Creates a single lambda function using bootstrap.zip in Amazon S3.
async fn create_lambda_function(ctx: &ExecutionContext) -> Result<String> {
    let s3_bucket = globals["lambda"]["s3_bucket"].to_string();
    let s3_key = globals["lambda"]["s3_nexmark_key"].to_string();
    let func_name = ctx.name.clone();
    if LAMBDA_CLIENT
        .get_function(GetFunctionRequest {
            function_name: ctx.name.clone(),
            ..Default::default()
        })
        .await
        .is_ok()
    {
        let conf = LAMBDA_CLIENT
            .update_function_code(UpdateFunctionCodeRequest {
                function_name: func_name.clone(),
                s3_bucket: Some(s3_bucket.clone()),
                s3_key: Some(s3_key.clone()),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?;
        conf.function_name
            .ok_or_else(|| FlockError::Internal("No function name!".to_string()))
    } else {
        let conf = LAMBDA_CLIENT
            .create_function(CreateFunctionRequest {
                code: FunctionCode {
                    s3_bucket: Some(s3_bucket.clone()),
                    s3_key: Some(s3_key.clone()),
                    ..Default::default()
                },
                function_name: func_name.clone(),
                handler: lambda::handler(),
                role: lambda::role().await,
                runtime: lambda::runtime(),
                environment: lambda::environment(&ctx),
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
        let event = Arc::new(conf.generate_data()?)
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

            flock_ctx.feed_data_sources(&vec![
                vec![NexMarkSource::to_batch(&event.bids, BID.clone())],
                vec![NexMarkSource::to_batch(&event.persons, PERSON.clone())],
                vec![NexMarkSource::to_batch(&event.auctions, AUCTION.clone())],
            ]);

            let output = flock_ctx.execute().await?;
            println!("{}", pretty_format_batches(&output)?);
        }
        Ok(())
    }
}
