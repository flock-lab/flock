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

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::nexmark::event::{Auction, Bid, Date, Person};
    use crate::datasource::nexmark::{NexMarkEvents, NexMarkSource};
    use crate::error::Result;
    use crate::executor::plan::physical_plan;
    use crate::query::StreamWindow;
    use arrow::json;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use std::io::BufReader;
    use std::io::Write;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_query_5() -> Result<()> {
        // benchmark configuration
        let seconds = 5;
        let threads = 1;
        let event_per_second = 1000;

        // hopping window
        let window = 3;
        let hop = 2;
        let nex = NexMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::HoppingWindow((window, hop)),
        );

        // data source generation
        let events = nex.generate_data()?;

        let sql = concat!(
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
        );

        let bid_schema = Arc::new(Bid::schema());
        let (window, hop) = match nex.window {
            StreamWindow::HoppingWindow((window, hop)) => (window, hop),
            _ => unreachable!(),
        };

        // sequential processing
        let mut bids_batches = vec![];
        for i in (0..seconds).step_by(hop) {
            // moves the hopping window
            let mut d = 0;
            if !bids_batches.is_empty() {
                bids_batches = bids_batches.drain(hop..).collect();
                d = window - hop;
            }

            // updates the hopping window
            for j in i + d..i + window {
                if j >= seconds {
                    break;
                }
                let bm = events.bids.get(&Date::new(j)).unwrap();
                let (bids, _) = bm.get(&0).unwrap();
                bids_batches.push(NexMarkSource::to_batch(&bids, bid_schema.clone()));
            }

            // register the memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let bid_table = MemTable::try_new(bid_schema.clone(), bids_batches)?;
            ctx.register_table("bid", Arc::new(bid_table));

            // optimize the query plan and execute it
            let physical_plan = physical_plan(&mut ctx, &sql)?;
            let batches = collect(physical_plan).await?;

            // show output
            let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
            println!("{}", formatted);

            unsafe {
                bids_batches = Arc::get_mut_unchecked(&mut ctx.deregister_table("bid").unwrap())
                    .as_mut_any()
                    .downcast_mut::<MemTable>()
                    .unwrap()
                    .batches();
            }
        }

        Ok(())
    }
}
