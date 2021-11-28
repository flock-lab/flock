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
    use crate::query::{Schedule, StreamWindow};
    use arrow::json;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use std::io::BufReader;
    use std::io::Write;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_query_8() -> Result<()> {
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
        );

        let auction_schema = Arc::new(Auction::schema());
        let person_schema = Arc::new(Person::schema());
        let window_size = match nex.window {
            StreamWindow::TumblingWindow(Schedule::Seconds(sec)) => sec,
            _ => unreachable!(),
        };

        // sequential processing
        for j in 0..seconds / window_size {
            let mut auctions_batches = vec![];
            let mut person_batches = vec![];
            let d = j * window_size;
            // moves the tumbling window
            for i in d..d + window_size {
                let am = events.auctions.get(&Date::new(i)).unwrap();
                let (auctions, _) = am.get(&0).unwrap();
                auctions_batches.push(NexMarkSource::to_batch(&auctions, auction_schema.clone()));

                let pm = events.persons.get(&Date::new(i)).unwrap();
                let (persons, _) = pm.get(&0).unwrap();
                person_batches.push(NexMarkSource::to_batch(&persons, person_schema.clone()));
            }

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let auction_table = MemTable::try_new(auction_schema.clone(), auctions_batches)?;
            ctx.register_table("auction", Arc::new(auction_table))?;

            let person_table = MemTable::try_new(person_schema.clone(), person_batches)?;
            ctx.register_table("person", Arc::new(person_table))?;

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