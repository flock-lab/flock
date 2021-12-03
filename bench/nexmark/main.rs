// Copyright (c) 2021 UMD Database Group. All Rights Reserved.
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

#[macro_use]
extern crate itertools;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::datasource::MemTable;
use driver::deploy::lambda;
use lazy_static::lazy_static;
use log::info;
use nexmark::event::{Auction, Bid, Person};
use nexmark::{NexMarkSource, NexMarkStream};
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{
    CreateFunctionRequest, FunctionCode, GetFunctionRequest, InvocationRequest, InvocationResponse,
    Lambda, LambdaClient, PutFunctionConcurrencyRequest, UpdateFunctionCodeRequest,
};
use std::sync::Arc;
use structopt::StructOpt;

#[allow(dead_code)]
static LAMBDA_SYNC_CALL: &str = "RequestResponse";
#[allow(dead_code)]
static LAMBDA_ASYNC_CALL: &str = "Event";

lazy_static! {
    static ref PERSON: SchemaRef = Arc::new(Person::schema());
    static ref AUCTION: SchemaRef = Arc::new(Auction::schema());
    static ref BID: SchemaRef = Arc::new(Bid::schema());
    static ref LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
}

#[derive(Debug, StructOpt)]
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

async fn benchmark(opt: NexmarkBenchmarkOpt) -> Result<()> {
    info!("Running benchmarks with the following options: {:?}", opt);
    let nexmark = NexMarkSource::new(
        opt.seconds,
        opt.generators,
        opt.events_per_second,
        StreamWindow::ElementWise,
    );

    let mut ctx = datafusion::execution::context::ExecutionContext::new();
    {
        // register tables
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
    }

    // construct query plan into the cloud environment
    let sqls = nexmark_query(opt.query_number);
    if sqls.len() > 1 {
        unimplemented!();
    }
    let lambda_ctx = ExecutionContext {
        plan:         physical_plan(&mut ctx, &sqls[0])?,
        name:         format!("q{}", opt.query_number),
        next:         CloudFunction::None,
        datasource:   DataSource::default(),
        query_number: Some(opt.query_number),
        debug:        opt.debug,
    };

    let function_name = create_lambda_function(&lambda_ctx).await?;
    if opt.debug {
        info!("[OK] Create lambda function: {}.", function_name);
    }

    let events = Arc::new(nexmark.generate_data()?);
    info!("[OK] Generate nexmark events.");

    #[allow(unused_assignments)]
    let mut tasks = vec![];

    if let StreamWindow::ElementWise = nexmark.window {
        tasks = iproduct!(0..opt.seconds, 0..opt.generators)
            .map(|(t, g)| {
                let e = events.clone();
                let q = opt.query_number;
                let f = function_name.clone();
                tokio::spawn(async move {
                    info!("[OK] Send nexmark event (time: {}, source: {}).", t, g);
                    let p = serde_json::to_vec(&nexmark_event_to_payload(e, t, g, q)?)?.into();
                    Ok(vec![invoke_lambda_function(f, Some(p)).await?])
                })
            })
            // this collect *is needed* so that the join below can switch between tasks.
            .collect::<Vec<tokio::task::JoinHandle<Result<Vec<InvocationResponse>>>>>();
    } else {
        set_lambda_concurrency(function_name.clone(), 1).await?;
        tasks = (0..opt.generators)
            .map(|g| {
                let seconds = opt.seconds;
                let e = events.clone();
                let q = opt.query_number;
                let f = function_name.clone();
                tokio::spawn(async move {
                    let mut response = vec![];
                    for t in 0..seconds {
                        info!("[OK] Send nexmark event (time: {}, source: {}).", t, g);
                        let p = serde_json::to_vec(&nexmark_event_to_payload(e.clone(), t, g, q)?)?
                            .into();
                        response.push(invoke_lambda_function(f.clone(), Some(p)).await?);
                    }
                    Ok(response)
                })
            })
            // this collect *is needed* so that the join below can switch between tasks.
            .collect::<Vec<tokio::task::JoinHandle<Result<Vec<InvocationResponse>>>>>();
    }

    for task in tasks {
        let res_vec = task.await.expect("Lambda function execution failed.")?;
        if opt.debug {
            res_vec.into_iter().for_each(|response| {
                info!(
                    "[OK] Received status from async lambda function. {:?}",
                    response
                );
            });
        }
    }

    Ok(())
}

fn nexmark_event_to_payload(
    events: Arc<NexMarkStream>,
    time: usize,
    generator: usize,
    query_number: usize,
) -> Result<Payload> {
    let event = events
        .select(time, generator)
        .expect("Failed to select event.");

    if event.persons.is_empty() && event.auctions.is_empty() && event.bids.is_empty() {
        return Err(FlockError::Execution("No Nexmark input!".to_owned()));
    }

    match query_number {
        0 | 1 | 2 => {
            let uuid = UuidBuilder::new(&format!("q{}", query_number), 1).next();
            Ok(to_payload(
                &NexMarkSource::to_batch(&event.bids, BID.clone()),
                &vec![],
                uuid,
            ))
        }
        3 => {
            let uuid = UuidBuilder::new(&format!("q{}", query_number), 1).next();
            Ok(to_payload(
                &NexMarkSource::to_batch(&event.persons, PERSON.clone()),
                &NexMarkSource::to_batch(&event.auctions, AUCTION.clone()),
                uuid,
            ))
        }
        4 => {
            let uuid = UuidBuilder::new(&format!("q{}", query_number), 1).next();
            Ok(to_payload(
                &NexMarkSource::to_batch(&event.auctions, AUCTION.clone()),
                &NexMarkSource::to_batch(&event.bids, BID.clone()),
                uuid,
            ))
        }
        _ => unimplemented!(),
    }
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
        match LAMBDA_CLIENT
            .update_function_code(UpdateFunctionCodeRequest {
                function_name: func_name.clone(),
                s3_bucket: Some(s3_bucket.clone()),
                s3_key: Some(s3_key.clone()),
                ..Default::default()
            })
            .await
        {
            Ok(config) => {
                return config.function_name.ok_or_else(|| {
                    FlockError::Internal("Unable to find lambda function arn.".to_string())
                })
            }
            Err(err) => {
                return Err(FlockError::Internal(format!(
                    "Failed to update lambda function: S3 Bucket: {}. S3 Key: {}. {}",
                    s3_bucket, s3_key, err
                )))
            }
        }
    } else {
        match LAMBDA_CLIENT
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
                ..Default::default()
            })
            .await
        {
            Ok(config) => {
                return config.function_name.ok_or_else(|| {
                    FlockError::Internal("Unable to find lambda function arn.".to_string())
                })
            }
            Err(err) => {
                return Err(FlockError::Internal(format!(
                    "Failed to create lambda function: {}",
                    err
                )))
            }
        }
    }
}

/// Returns Nextmark query strings based on the query number.
fn nexmark_query(query: usize) -> Vec<String> {
    match query {
        0 => vec!["SELECT * FROM bid"],
        1 => vec!["SELECT auction, bidder, 0.908 * price as price, b_date_time FROM bid"],
        2 => vec!["SELECT auction, price FROM bid WHERE auction % 123 = 0"],
        3 => vec![concat!(
            "SELECT ",
            "    name, city, state, a_id ",
            "FROM ",
            "    auction INNER JOIN person on seller = p_id ",
            "WHERE ",
            "    category = 10 and (state = 'or' OR state = 'id' OR state = 'ca');"
        )],
        4 => vec![concat!(
            "SELECT ",
            "    category, ",
            "    AVG(final) ",
            "FROM ( ",
            "    SELECT MAX(price) AS final, category ",
            "    FROM auction INNER JOIN bid on a_id = auction ",
            "    WHERE b_date_time BETWEEN a_date_time AND expires ",
            "    GROUP BY a_id, category ",
            ") as Q ",
            "GROUP BY category;"
        )],
        5 => vec![concat!(
            "SELECT auction, num ",
            "FROM ( ",
            "  SELECT ",
            "    auction, ",
            "    count(*) AS num ",
            "  FROM bid ",
            "  GROUP BY auction ",
            ") AS AuctionBids ",
            "INNER JOIN ( ",
            "  SELECT ",
            "    max(num) AS maxn ",
            "  FROM ( ",
            "    SELECT ",
            "      auction, ",
            "      count(*) AS num ",
            "    FROM bid ",
            "    GROUP BY ",
            "      auction ",
            "    ) AS CountBids ",
            ") AS MaxBids ",
            "ON num = maxn;"
        )],
        6 => vec![
            concat!(
                "SELECT COUNT(DISTINCT seller) ",
                "FROM auction INNER JOIN bid ON a_id = auction ",
                "WHERE b_date_time between a_date_time and expires ",
            ),
            concat!(
                "SELECT seller, MAX(price) AS final ",
                "FROM auction INNER JOIN bid ON a_id = auction ",
                "WHERE b_date_time between a_date_time and expires ",
                "GROUP BY a_id, seller ORDER by seller"
            ),
            "SELECT seller, AVG(final) FROM Q GROUP BY seller",
        ],
        7 => vec![concat!(
            "SELECT auction, price, bidder, b_date_time ",
            "FROM bid ",
            "JOIN ( ",
            "    SELECT MAX(price) AS maxprice ",
            "    FROM bid ",
            ") AS B1 ",
            "ON price = maxprice;"
        )],
        8 => vec![concat!(
            "SELECT p_id, name ",
            "FROM ( ",
            "  SELECT p_id, name FROM person ",
            "  GROUP BY p_id, name ",
            ") AS P ",
            "JOIN ( ",
            "  SELECT seller FROM auction ",
            "  GROUP BY seller ",
            ") AS A ",
            "ON p_id = seller; "
        )],
        9 => vec![concat!(
            "SELECT auction, bidder, price, b_date_time ",
            "FROM bid ",
            "JOIN ( ",
            "  SELECT a_id as id, MAX(price) AS final ",
            "  FROM auction INNER JOIN bid on a_id = auction ",
            "  WHERE b_date_time BETWEEN a_date_time AND expires ",
            "  GROUP BY a_id ",
            ") ON auction = id and price = final;"
        )],
        _ => unreachable!(),
    }
    .into_iter()
    .map(String::from)
    .collect()
}
