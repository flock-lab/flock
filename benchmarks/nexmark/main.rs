// Copyright 2021 UMD Database Group. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[macro_use]
extern crate itertools;

use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use driver::deploy::lambda;
use lazy_static::lazy_static;
use log::info;
use nexmark::config::Config;
use nexmark::event::{Auction, Bid, Person};
use nexmark::NexMarkSource;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{
    CreateFunctionRequest, DeleteFunctionRequest, GetFunctionRequest, InvocationRequest,
    InvocationResponse, Lambda, LambdaClient,
};
use serde_json::Value;
use std::sync::Arc;
use structopt::StructOpt;

lazy_static! {
    static ref LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
}

#[derive(Debug, StructOpt)]
struct NexmarkBenchmarkOpt {
    /// Query number
    #[structopt(short, long)]
    query: usize,

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
    #[structopt(short = "eps", long = "events_per_second", default_value = "100000")]
    events_per_second: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    benchmark(NexmarkBenchmarkOpt::from_args()).await?;
    Ok(())
}

async fn benchmark(opt: NexmarkBenchmarkOpt) -> Result<()> {
    println!("Running benchmarks with the following options: {:?}", opt);
    let mut config = Config::new();
    config.insert("threads", opt.generators.to_string());
    config.insert("seconds", opt.seconds.to_string());
    config.insert("events-per-second", opt.events_per_second.to_string());
    let nexmark = NexMarkSource {
        config,
        ..Default::default()
    };

    let mut ctx = datafusion::execution::context::ExecutionContext::new();
    {
        // register tables
        let person_schema = Arc::new(Person::schema());
        let person_table = MemTable::try_new(
            person_schema.clone(),
            vec![vec![RecordBatch::new_empty(person_schema)]],
        )?;
        ctx.register_table("person", Arc::new(person_table));

        let auction_schema = Arc::new(Auction::schema());
        let auction_table = MemTable::try_new(
            auction_schema.clone(),
            vec![vec![RecordBatch::new_empty(auction_schema)]],
        )?;
        ctx.register_table("auction", Arc::new(auction_table));

        let bid_schema = Arc::new(Bid::schema());
        let bid_table = MemTable::try_new(
            bid_schema.clone(),
            vec![vec![RecordBatch::new_empty(bid_schema)]],
        )?;
        ctx.register_table("bid", Arc::new(bid_table));
    }

    // marshal physical plan into cloud environment
    let sqls = query(opt.query);
    if sqls.len() > 1 {
        unimplemented!();
    }
    let lambda_ctx = ExecutionContext {
        plan:         physical_plan(&mut ctx, &sqls[0])?,
        name:         format!("q{}", opt.query),
        next:         CloudFunction::None,
        datasource:   DataSource::default(),
        query_number: Some(opt.query),
    };

    // create lambda function based on the generic lambda function code on AWS S3.
    let func_arn = create_lambda_function(&lambda_ctx).await?;
    info!("[OK] Create lambda function {}.", func_arn);

    let events = nexmark.generate_data()?;
    info!("[OK] Generate nexmark events.");

    if let StreamWindow::None = nexmark.window {
        let tasks = iproduct!(0..opt.seconds, 0..opt.generators)
            .map(|(i, j)| {
                let func_arn = func_arn.clone();
                let event = events.select(i, j).unwrap();
                tokio::spawn(async move {
                    info!("[OK] Send nexmark event (time: {}, source: {}).", i, j);
                    invoke_lambda_function(func_arn.clone(), serde_json::to_vec(&event)?).await
                })
            })
            // this collect *is needed* so that the join below can switch between tasks.
            .collect::<Vec<_>>();

        for task in tasks {
            let response = task.await.expect("Lambda function execution failed.")?;
            if opt.debug {
                println!(
                    "{:?}",
                    serde_json::from_slice::<Value>(&response.payload.unwrap())?
                );
            }
        }
    } else {
        unimplemented!();
    }

    Ok(())
}

/// Invoke the lambda function with the nexmark events.
async fn invoke_lambda_function(
    function_name: String,
    events: Vec<u8>,
) -> Result<InvocationResponse> {
    let request = InvocationRequest {
        function_name,
        payload: Some(events.into()),
        invocation_type: Some("RequestResponse".to_string()),
        ..Default::default()
    };

    match LAMBDA_CLIENT.invoke(request).await {
        Ok(response) => return Ok(response),
        Err(err) => {
            return Err(SquirtleError::Execution(format!(
                "Lambda function execution failure: {}",
                err
            )))
        }
    }
}

/// Creates a single lambda function using bootstrap.zip in Amazon S3.
async fn create_lambda_function(ctx: &ExecutionContext) -> Result<String> {
    if LAMBDA_CLIENT
        .get_function(GetFunctionRequest {
            function_name: ctx.name.clone(),
            ..Default::default()
        })
        .await
        .is_ok()
    {
        // Delete previous lambda function to avoid the outdated code in S3.
        LAMBDA_CLIENT
            .delete_function(DeleteFunctionRequest {
                function_name: ctx.name.clone(),
                ..Default::default()
            })
            .await
            .map_err(|e| SquirtleError::Internal(e.to_string()))?;
    }

    match LAMBDA_CLIENT
        .create_function(CreateFunctionRequest {
            code: lambda::nexmark_function_code(),
            environment: lambda::environment(&ctx),
            function_name: ctx.name.clone(),
            handler: lambda::handler(),
            memory_size: lambda::memory_size(&ctx),
            role: lambda::role().await,
            runtime: lambda::runtime(),
            ..Default::default()
        })
        .await
    {
        Ok(config) => {
            return config.function_arn.ok_or_else(|| {
                SquirtleError::Internal("Unable to find lambda function arn.".to_string())
            })
        }
        Err(err) => {
            return Err(SquirtleError::Internal(format!(
                "Failed to create lambda function: {}",
                err
            )))
        }
    }
}

/// Returns Nextmark query strings based on the query number.
fn query(query: usize) -> Vec<String> {
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
