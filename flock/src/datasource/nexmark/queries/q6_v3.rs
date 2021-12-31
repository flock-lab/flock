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

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use crate::datasource::epoch::Epoch;
    use crate::datasource::nexmark::event::{Auction, Bid};
    use crate::datasource::nexmark::NEXMarkSource;
    use crate::error::Result;
    use crate::runtime::executor::plan::physical_plan;
    use crate::runtime::query::StreamWindow;
    use crate::transmute::event_bytes_to_batch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use indoc::indoc;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_query_6_v3() -> Result<()> {
        // benchmark configuration
        let seconds = 2;
        let threads = 1;
        let event_per_second = 1000;
        let nex = NEXMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::ElementWise,
        );

        // data source generation
        let events = nex.generate_data()?;

        // https://stackoverflow.com/questions/7747327/sql-rank-versus-row-number

        // ROW_NUMBER : Returns a unique number for each row starting with 1. For rows
        // that have duplicate values, numbers are arbitarily assigned.

        // Rank : Assigns a unique number for each row starting with 1, except for rows
        // that have duplicate values, in which case the same ranking is assigned and a
        // gap appears in the sequence for each duplicate ranking.

        let sql = indoc! {"
            SELECT  seller,
                    Avg(price)
            FROM   (SELECT  seller,
                            price,
                            b_date_time,
                            ROW_NUMBER()
                            OVER (
                                partition BY seller
                                ORDER BY b_date_time DESC) AS time_rank
                    FROM   (SELECT  seller,
                                    a_id,
                                    price,
                                    b_date_time,
                                    ROW_NUMBER()
                                    OVER (
                                        partition BY a_id
                                        ORDER BY price DESC) AS price_rank
                            FROM    auction
                                    INNER JOIN bid
                                            ON a_id = auction
                            WHERE   b_date_time BETWEEN a_date_time AND expires
                            ORDER   BY a_id,
                                    price DESC) AS Q
                    WHERE   price_rank = 1) AS R
            WHERE   time_rank <= 10
            GROUP   BY seller
        "};

        let auction_schema = Arc::new(Auction::schema());
        let bid_schema = Arc::new(Bid::schema());

        // sequential processing
        for i in 0..seconds {
            // events to record batches
            let am = events.auctions.get(&Epoch::new(i)).unwrap();
            let (auctions, _) = am.get(&0).unwrap();
            let auctions_batches = event_bytes_to_batch(auctions, auction_schema.clone(), 1024);

            let bm = events.bids.get(&Epoch::new(i)).unwrap();
            let (bids, _) = bm.get(&0).unwrap();
            let bids_batches = event_bytes_to_batch(bids, bid_schema.clone(), 1024);

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let auction_table = MemTable::try_new(auction_schema.clone(), vec![auctions_batches])?;
            ctx.deregister_table("auction")?;
            ctx.register_table("auction", Arc::new(auction_table))?;

            let bid_table = MemTable::try_new(bid_schema.clone(), vec![bids_batches])?;
            ctx.deregister_table("bid")?;
            ctx.register_table("bid", Arc::new(bid_table))?;

            // optimize query plan and execute it
            let plan = physical_plan(&ctx, sql).await?;
            let output_partitions = collect(plan).await?;

            // show output
            let formatted = arrow::util::pretty::pretty_format_batches(&output_partitions).unwrap();
            println!("{}", formatted);
        }

        Ok(())
    }
}
