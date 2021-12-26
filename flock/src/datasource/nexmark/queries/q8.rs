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
    use crate::datasource::nexmark::event::{Auction, Person};
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
    async fn local_query_8() -> Result<()> {
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
            SELECT  p_id,
                    name
            FROM   (SELECT p_id,
                           name
                    FROM   person
                    GROUP  BY p_id,
                              name) AS P
                    JOIN (SELECT seller
                          FROM   auction
                          GROUP  BY seller) AS A
                      ON p_id = seller;
        "};

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
                let am = events.auctions.get(&Epoch::new(i)).unwrap();
                let (auctions, _) = am.get(&0).unwrap();
                auctions_batches.push(NEXMarkSource::to_batch(auctions, auction_schema.clone()));

                let pm = events.persons.get(&Epoch::new(i)).unwrap();
                let (persons, _) = pm.get(&0).unwrap();
                person_batches.push(NEXMarkSource::to_batch(persons, person_schema.clone()));
            }

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let auction_table = MemTable::try_new(auction_schema.clone(), auctions_batches)?;
            ctx.deregister_table("auction")?;
            ctx.register_table("auction", Arc::new(auction_table))?;

            let person_table = MemTable::try_new(person_schema.clone(), person_batches)?;
            ctx.deregister_table("person")?;
            ctx.register_table("person", Arc::new(person_table))?;

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
