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
    use crate::datasource::date::DateTime;
    use crate::datasource::nexmark::event::Bid;
    use crate::datasource::nexmark::NEXMarkSource;
    use crate::error::Result;
    use crate::runtime::executor::plan::physical_plan;
    use crate::runtime::query::Schedule;
    use crate::runtime::query::StreamWindow;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use indoc::indoc;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_query_7() -> Result<()> {
        // benchmark configuration
        let seconds = 4;
        let threads = 1;
        let event_per_second = 1000;
        let nex = NEXMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::TumblingWindow(Schedule::Seconds(2)),
        );

        // data source generation
        let events = nex.generate_data()?;

        let sql = indoc! {"
            SELECT  auction,
                    price,
                    bidder,
                    b_date_time
            FROM    bid
                    JOIN (SELECT Max(price) AS maxprice
                          FROM   bid) AS B1
                      ON price = maxprice;
        "};

        let schema = Arc::new(Bid::schema());
        let window_size = match nex.window {
            StreamWindow::TumblingWindow(Schedule::Seconds(sec)) => sec,
            _ => unreachable!(),
        };

        // sequential processing
        for j in 0..seconds / window_size {
            let mut batches = vec![];
            let d = j * window_size;
            // moves the tumbling window
            for i in d..d + window_size {
                let bm = events.bids.get(&DateTime::new(i)).unwrap();
                let (bids, _) = bm.get(&0).unwrap();
                batches.push(NEXMarkSource::to_batch(bids, schema.clone()));
            }

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let table = MemTable::try_new(schema.clone(), batches)?;
            ctx.deregister_table("bid")?;
            ctx.register_table("bid", Arc::new(table))?;

            // optimize query plan and execute it
            let physical_plan = physical_plan(&mut ctx, sql).await?;
            let batches = collect(physical_plan).await?;

            // show output
            let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
            println!("{}", formatted);
        }

        Ok(())
    }
}
