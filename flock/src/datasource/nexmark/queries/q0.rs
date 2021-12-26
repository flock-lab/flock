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
    use crate::datasource::nexmark::event::Bid;
    use crate::datasource::nexmark::NEXMarkSource;
    use crate::error::Result;
    use crate::runtime::executor::plan::physical_plan;
    use crate::runtime::query::StreamWindow;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_query_0() -> Result<()> {
        // benchmark configuration
        let nex = NEXMarkSource::new(3, 1, 200, StreamWindow::ElementWise);

        // data source generation
        let events = nex.generate_data()?;

        let sql = "SELECT * FROM bid;";
        let schema = Arc::new(Bid::schema());

        // sequential processing
        for i in 0..events.bids.len() {
            // events to record batches
            let bm = events.bids.get(&Epoch::new(i)).unwrap();
            let (bids, _) = bm.get(&0).unwrap();
            let batches = NEXMarkSource::to_batch(bids, schema.clone());

            // register memory table
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let table = MemTable::try_new(schema.clone(), vec![batches])?;
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
