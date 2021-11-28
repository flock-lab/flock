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

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::nexmark::event::{Auction, Bid, Date, Person};
    use crate::datasource::nexmark::{NexMarkSource, NexMarkStream};
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
    async fn local_query_4() -> Result<()> {
        // benchmark configuration
        let seconds = 2;
        let threads = 1;
        let event_per_second = 1000;
        let nex = NexMarkSource::new(seconds, threads, event_per_second, StreamWindow::None);

        // data source generation
        let events = nex.generate_data()?;

        let sql = concat!(
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
        );

        let auction_schema = Arc::new(Auction::schema());
        let bid_schema = Arc::new(Bid::schema());

        // sequential processing
        for i in 0..seconds {
            // events to record batches
            let am = events.auctions.get(&Date::new(i)).unwrap();
            let (auctions, _) = am.get(&0).unwrap();
            let auctions_batches = NexMarkSource::to_batch(&auctions, auction_schema.clone());

            let bm = events.bids.get(&Date::new(i)).unwrap();
            let (bids, _) = bm.get(&0).unwrap();
            let bids_batches = NexMarkSource::to_batch(&bids, bid_schema.clone());

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let auction_table = MemTable::try_new(auction_schema.clone(), vec![auctions_batches])?;
            ctx.register_table("auction", Arc::new(auction_table))?;

            let bid_table = MemTable::try_new(bid_schema.clone(), vec![bids_batches])?;
            ctx.register_table("bid", Arc::new(bid_table))?;

            // optimize query plan and execute it
            let physical_plan = physical_plan(&mut ctx, &sql)?;
            let batches = collect(physical_plan).await?;

            // show output
            let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
            println!("{}", formatted);
        }

        Ok(())
    }
}