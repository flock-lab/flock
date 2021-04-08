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
    use crate::query::{Schedule, StreamWindow};
    use arrow::json;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use std::io::BufReader;
    use std::io::Write;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_query_7() -> Result<()> {
        // benchmark configuration
        let seconds = 4;
        let threads = 1;
        let event_per_second = 1000;
        let nex = NexMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::TumblingWindow(Schedule::Seconds(2)),
        );

        // data source generation
        let events = nex.generate_data()?;

        let sql = concat!(
            "SELECT auction, price, bidder, b_date_time ",
            "FROM bid ",
            "JOIN ( ",
            "    SELECT MAX(price) AS maxprice ",
            "    FROM bid ",
            ") AS B1 ",
            "ON price = maxprice;"
        );

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
                let bm = events.bids.get(&Date::new(i)).unwrap();
                let (bids, _) = bm.get(&0).unwrap();
                batches.push(NexMarkSource::to_batch(&bids, schema.clone()));
            }

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let table = MemTable::try_new(schema.clone(), batches)?;
            ctx.register_table("bid", Arc::new(table));

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
