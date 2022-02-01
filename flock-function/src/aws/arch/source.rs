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

//! The entry point for the arch benchmark on cloud functions.

use flock::datasource::nexmark::event::{Auction, Bid};
use flock::datasource::nexmark::register_nexmark_tables;
use flock::datasource::nexmark::NEXMarkSource;
use flock::prelude::*;
use log::info;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

macro_rules! eval_operator {
    ($CTX: ident, $EVENTS: ident, $OPERATOR:literal, $PATH:literal) => {
        let auction_schema = Arc::new(Auction::schema());
        let bid_schema = Arc::new(Bid::schema());
        let auctions = event_bytes_to_batch(&$EVENTS.auctions, auction_schema, 1024);
        let bids = event_bytes_to_batch(&$EVENTS.bids, bid_schema, 1024);

        let df_ctx = register_nexmark_tables().await?;
        let sql = include_str!($PATH);
        let plan = df_ctx.create_logical_plan(sql.as_ref())?;
        let plan = df_ctx.optimize(&plan)?;
        let plan = df_ctx.create_physical_plan(&plan).await?;

        $CTX.set_plan(CloudExecutionPlan::new(vec![plan], None))
            .await;
        $CTX.feed_data_sources(vec![vec![bids.clone()], vec![auctions.clone()]])
            .await?;

        let mut times = vec![];
        for _ in 0..10 {
            let start = Instant::now();
            $CTX.execute().await?;
            let end = start.elapsed();
            times.push(end);
        }
        let avg_time = times
            .iter()
            .fold(std::time::Duration::new(0, 0), |acc, x| acc + *x)
            .as_secs_f64()
            / times.len() as f64;

        times
            .into_iter()
            .enumerate()
            .for_each(|(i, x)| info!("#{}: {}", i, x.as_secs_f64()));
        info!(
            "[OK] {} operator finished in {} seconds.",
            $OPERATOR, avg_time
        );
    };
}

/// The endpoint of the data source generator function invocation. The data
/// source generator function is responsible for generating the data packets for
/// the query no matter what type of query it is.
///
/// # Arguments
/// * `ctx` - The runtime context of the function.
/// * `payload` - The payload of the function.
///
/// # Returns
/// A JSON object that contains the return value of the function invocation.
pub async fn handler(ctx: &mut ExecutionContext, payload: Payload) -> Result<Value> {
    let events_per_second = match payload.datasource.clone() {
        DataSource::Arch(events) => events,
        _ => unreachable!(),
    };

    let seconds = 1;
    let threads = 1;
    let nexmark = NEXMarkSource::new(seconds, threads, events_per_second, Window::ElementWise);

    let stream = nexmark.generate_data()?;
    let (events, (persons_num, auctions_num, bids_num)) =
        stream.select(0, 0).expect("Failed to select event.");

    info!(
        "Selecting events for epoch {}: {} persons, {} auctions, {} bids.",
        0, persons_num, auctions_num, bids_num
    );

    info!("[OK] Generated nexmark events.");

    // 1. filter operators
    eval_operator!(ctx, events, "filter", "./ops/filter.sql");

    // 2. join operators
    eval_operator!(ctx, events, "join", "./ops/join.sql");

    // 3. aggregate operators
    eval_operator!(ctx, events, "aggregate", "./ops/group-by.sql");

    // 4. sort operators
    eval_operator!(ctx, events, "sort", "./ops/sort.sql");

    Ok(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_arch_benchmark() -> Result<()> {
        let mut ctx = ExecutionContext {
            plan: CloudExecutionPlan::new(vec![FLOCK_EMPTY_PLAN.clone()], None),
            name: FLOCK_DATA_SOURCE_FUNC_NAME.clone(),
            next: CloudFunction::Sink(DataSinkType::Blackhole),
            ..Default::default()
        };

        let payload = Payload {
            datasource: DataSource::Arch(5000),
            ..Default::default()
        };

        handler(&mut ctx, payload).await?;

        Ok(())
    }
}
