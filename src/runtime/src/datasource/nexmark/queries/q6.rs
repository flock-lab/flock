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
    use crate::datasource::nexmark::event::{Auction, Bid, Date};
    use crate::datasource::nexmark::NexMarkSource;
    use crate::error::Result;
    use crate::executor::plan::physical_plan;
    use crate::query::StreamWindow;
    use arrow::array::UInt64Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::limit::truncate_batch;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{collect, collect_partitioned};
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use futures::stream::StreamExt;
    use std::sync::Arc;

    async fn repartition(
        input_partitions: Vec<Vec<RecordBatch>>,
        partitioning: Partitioning,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        // create physical plan
        let exec = MemoryExec::try_new(&input_partitions, input_partitions[0][0].schema(), None)?;
        let exec = RepartitionExec::try_new(Arc::new(exec), partitioning)?;

        // execute and collect results
        let mut output_partitions = vec![];
        for i in 0..exec.partitioning().partition_count() {
            // execute this *output* partition and collect all batches
            let mut stream = exec.execute(i).await?;
            let mut batches = vec![];
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            output_partitions.push(batches);
        }
        Ok(output_partitions)
    }

    #[tokio::test]
    async fn local_query_6() -> Result<()> {
        // benchmark configuration
        let seconds = 2;
        let threads = 1;
        let event_per_second = 1000;
        let nex = NexMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::ElementWise,
        );

        // data source generation
        let events = nex.generate_data()?;

        let sql1 = concat!(
            "SELECT COUNT(DISTINCT seller) ",
            "FROM auction INNER JOIN bid ON a_id = auction ",
            "WHERE b_date_time between a_date_time and expires ",
        );

        let sql2 = concat!(
            "SELECT seller, MAX(price) AS final ",
            "FROM auction INNER JOIN bid ON a_id = auction ",
            "WHERE b_date_time between a_date_time and expires ",
            "GROUP BY a_id, seller ORDER by seller;"
        );

        let sql3 = "SELECT seller, AVG(final) FROM Q GROUP BY seller;";

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

            // 1. get the total distinct sellers during the epoch
            let plan = physical_plan(&mut ctx, &sql1)?;
            let batches = collect(plan).await?;
            let total_distinct_sellers = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);

            // 2. get the max price of auctions for each seller
            let plan = physical_plan(&mut ctx, &sql2)?;
            let batches = collect_partitioned(plan).await?;
            let batches = repartition(
                batches,
                Partitioning::HashDiff(
                    vec![Arc::new(Column::new(&"seller", 0))],
                    total_distinct_sellers as usize,
                ),
            )
            .await?;

            // 3. simulate `Partition By 10 recent rows for each seller`
            let output_partitions = batches
                .iter()
                .map(|v| {
                    assert_eq!(v.len(), 1);
                    truncate_batch(&v[0], 10)
                })
                .collect::<Vec<RecordBatch>>();

            // 4. the average selling price per seller for their last 10 closed auctions.
            let q_table =
                MemTable::try_new(output_partitions[0].schema(), vec![output_partitions])?;
            ctx.register_table("Q", Arc::new(q_table))?;
            let plan = physical_plan(&mut ctx, &sql3)?;
            let output_partitions = collect(plan).await?;

            // show output
            let formatted = arrow::util::pretty::pretty_format_batches(&output_partitions).unwrap();
            println!("{}", formatted);
        }

        Ok(())
    }
}
