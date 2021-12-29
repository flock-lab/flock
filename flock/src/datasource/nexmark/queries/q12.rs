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
    use crate::runtime::query::{Schedule, StreamWindow};
    use crate::transmute::*;
    use arrow::array::{Int32Array, TimestampNanosecondArray, UInt64Array};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use datafusion::datasource::MemTable;
    use datafusion::logical_plan::{col, count_distinct};
    use datafusion::physical_plan::expressions::col as expr_col;
    use datafusion::physical_plan::Partitioning;
    use datafusion::physical_plan::{collect, collect_partitioned};
    use indoc::indoc;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn find_tumbling_windows(
        map: &HashMap<usize, Vec<Vec<RecordBatch>>>,
        interval: usize,
    ) -> Result<Vec<usize>> {
        let mut to_remove = vec![];

        map.iter().for_each(|(bidder, batches)| {
            // get the first bid's prcessing time
            let first_timestamp = batches[0][0]
                .column(4 /* p_time field */)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0);
            let first_process_time = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(first_timestamp / 1000 / 1000 / 1000, 0),
                Utc,
            );
            if Utc::now().signed_duration_since(first_process_time)
                > chrono::Duration::seconds(interval as i64)
            {
                to_remove.push(bidder.to_owned());
            }
        });

        Ok(to_remove)
    }

    #[tokio::test]
    async fn local_query_12() -> Result<()> {
        // benchmark configuration
        let seconds = 20;
        let threads = 1;
        let event_per_second = 50;
        let nex = NEXMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::TumblingWindow(Schedule::Seconds(4)),
        );

        // data source generation
        let events = nex.generate_data()?;

        let sql1 = "SELECT *, now() as p_time FROM bid";

        let sql2 = indoc! {"
            SELECT  bidder,
                    Count(*)        AS bid_count,
                    Min(p_time)     AS start_time,
                    Max(p_time)     AS end_time
            FROM    bid
            GROUP BY bidder;
        "};

        let schema = Arc::new(Bid::schema());
        let interval = match nex.window {
            StreamWindow::TumblingWindow(Schedule::Seconds(interval)) => interval,
            _ => unreachable!(),
        };

        #[allow(unused_assignments)]
        let mut new_schema = Arc::new(Bid::schema());
        let mut map: HashMap<usize, Vec<Vec<RecordBatch>>> = HashMap::new();

        // sequential processing
        for i in 0..seconds {
            let bm = events.bids.get(&Epoch::new(i)).unwrap();
            let (bids, _) = bm.get(&0).unwrap();
            let batches = vec![event_bytes_to_batch(bids, schema.clone(), 1024)];

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let table = MemTable::try_new(schema.clone(), batches)?;
            ctx.deregister_table("bid")?;
            ctx.register_table("bid", Arc::new(table))?;

            // 0. add process time field to the input
            let plan = physical_plan(&mut ctx, sql1).await?;
            let output = collect_partitioned(plan).await?;
            new_schema = output[0][0].schema().clone();

            // 1. get the total distinct bidders during the epoch
            let total_distinct_bidders = ctx
                .table("bid")?
                .aggregate(vec![], vec![count_distinct(col("bidder"))])?
                .collect()
                .await?[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);

            // 2. get the distinct partition for each bidder
            let partitions = repartition(
                output,
                Partitioning::HashDiff(
                    vec![expr_col("bidder", &schema)?],
                    total_distinct_bidders as usize,
                ),
            )
            .await?;

            // 3. add each partition to the map based on the column `bidder`
            let mut windows: Vec<Vec<Vec<RecordBatch>>> = partitions
                .into_iter()
                .filter(|x| !x.is_empty())
                .map(|partition| {
                    let mut window = vec![];
                    let bidder = partition[0]
                        .column(1 /* bidder field */)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0);
                    let current_timestamp = partition[0]
                        .column(4 /* p_time field */)
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(0);
                    let current_process_time = DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp(current_timestamp / 1000 / 1000 / 1000, 0),
                        Utc,
                    );

                    if !map.contains_key(&(bidder as usize)) {
                        map.entry(bidder as usize)
                            .or_insert_with(Vec::new)
                            .push(partition);
                    } else {
                        // get the first batch
                        let batch = map
                            .get(&(bidder as usize))
                            .unwrap()
                            .first()
                            .unwrap()
                            .first()
                            .unwrap();
                        // get the first bid's prcessing time
                        let first_timestamp = batch
                            .column(4 /* p_time field */)
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap()
                            .value(0);
                        let first_process_time = DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp(first_timestamp / 1000 / 1000 / 1000, 0),
                            Utc,
                        );

                        // If the current process time isn't 10 seconds later than the beginning of
                        // the entry, then we can add the current batch to the map. Otherwise, we
                        // have a new tumbling window.
                        if current_process_time.signed_duration_since(first_process_time)
                            > chrono::Duration::seconds(interval as i64)
                        {
                            window = map.remove(&(bidder as usize)).unwrap();
                        }
                        map.entry(bidder as usize)
                            .or_insert_with(Vec::new)
                            .push(partition);
                    }
                    window
                })
                .collect();

            // 4. iterate over the map and find the new tumble windows
            let to_remove = find_tumbling_windows(&map, interval)?;
            to_remove.iter().for_each(|bidder| {
                windows.push(map.remove(bidder).unwrap());
            });

            let windows: Vec<Vec<RecordBatch>> = windows.into_iter().flatten().collect();
            if !windows.is_empty() {
                let table = MemTable::try_new(new_schema.clone(), windows)?;
                ctx.deregister_table("bid")?;
                ctx.register_table("bid", Arc::new(table))?;
                let output = collect(physical_plan(&mut ctx, sql2).await?).await?;
                println!("{}", pretty_format_batches(&output).unwrap());
            }

            // 5. sleep for 1 second to ensure that the processing time is different
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }

        Ok(())
    }
}
