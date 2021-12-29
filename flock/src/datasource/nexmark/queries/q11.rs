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
    use crate::error::{FlockError, Result};
    use crate::runtime::executor::plan::physical_plan;
    use crate::runtime::query::{Schedule, StreamWindow};
    use crate::transmute::*;
    use arrow::array::{Int32Array, TimestampMillisecondArray, UInt64Array};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext as DataFusionExecutionContext;
    use datafusion::logical_plan::{col, count_distinct};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::expressions::col as expr_col;
    use datafusion::physical_plan::Partitioning;
    use indoc::indoc;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// 2015-07-15 00:00:00
    const BASE_TIME: i64 = 1_436_918_400_000;

    /// get back the input data from the registered table after the query is
    /// executed
    fn get_input_from_table(ctx: &mut DataFusionExecutionContext) -> Result<Vec<Vec<RecordBatch>>> {
        Ok(unsafe {
            Arc::get_mut_unchecked(
                &mut ctx
                    .deregister_table("bid")
                    .map_err(FlockError::DataFusion)?
                    .ok_or_else(|| {
                        FlockError::Internal("Failed to deregister table `bid`".to_string())
                    })?,
            )
            .as_mut_any()
            .downcast_mut::<MemTable>()
            .unwrap()
            .batches()
        })
    }

    fn find_session_windows(
        map: &HashMap<usize, Vec<Vec<RecordBatch>>>,
        interval: usize,
        i: usize,
    ) -> Result<Vec<usize>> {
        let mut to_remove = vec![];

        map.iter().for_each(|(bidder, batches)| {
            // get the last batch
            let batch = batches.last().unwrap().last().unwrap();

            // get the last bid's date time
            let last_timestamp = batch
                .column(3 /* b_date_time field */)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(batch.num_rows() - 1);

            let last_date_time = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(last_timestamp / 1000, 0),
                Utc,
            );

            let epoch_gap_time = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(BASE_TIME / 1000 + i as i64, 0),
                Utc,
            );

            if epoch_gap_time.signed_duration_since(last_date_time)
                > chrono::Duration::seconds(interval as i64)
            {
                to_remove.push(bidder.to_owned());
            }
        });

        Ok(to_remove)
    }

    #[tokio::test]
    async fn local_query_11() -> Result<()> {
        // benchmark configuration
        let seconds = 40;
        let threads = 1;
        let event_per_second = 100;
        let nex = NEXMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::SessionWindow(Schedule::Seconds(4)),
        );

        // data source generation
        let events = nex.generate_data()?;

        // let sql1 = "SELECT COUNT(DISTINCT bidder) FROM bid;";
        // let plan = physical_plan(&mut ctx, sql1).await?;
        // let output = collect(plan).await?;

        let sql2 = indoc! {"
            SELECT  bidder,
                    Count(*)         AS bid_count,
                    Min(b_date_time) AS start_time,
                    Max(b_date_time) AS end_time
            FROM    bid
            GROUP BY bidder;
        "};

        let schema = Arc::new(Bid::schema());
        let interval = match nex.window {
            StreamWindow::SessionWindow(Schedule::Seconds(interval)) => interval,
            _ => unreachable!(),
        };

        let mut map: HashMap<usize, Vec<Vec<RecordBatch>>> = HashMap::new();

        // sequential processing
        for i in 0..seconds {
            println!("Epoch {}", i);
            let bm = events.bids.get(&Epoch::new(i)).unwrap();
            let (bids, _) = bm.get(&0).unwrap();
            let batches = vec![event_bytes_to_batch(bids, schema.clone(), 1024)];

            // register memory tables
            let mut ctx = DataFusionExecutionContext::new();
            let table = MemTable::try_new(schema.clone(), batches)?;
            ctx.deregister_table("bid")?;
            ctx.register_table("bid", Arc::new(table))?;

            // 1. get the total distinct bidders during the epoch
            let output = ctx
                .table("bid")?
                .aggregate(vec![], vec![count_distinct(col("bidder"))])?
                .collect()
                .await?;

            let total_distinct_bidders = output[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);

            // 2. get the distinct partition for each bidder
            let partitions = repartition(
                get_input_from_table(&mut ctx)?,
                Partitioning::HashDiff(
                    vec![expr_col("bidder", &schema)?],
                    total_distinct_bidders as usize,
                ),
            )
            .await?;

            // 3. add each partition to the map based on the column `bidder`
            let mut sessions: Vec<Vec<Vec<RecordBatch>>> = partitions
                .into_iter()
                .filter(|x| !x.is_empty())
                .map(|partition| {
                    let mut session = vec![];
                    let bidder = partition[0]
                        .column(1 /* bidder field */)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0);
                    let current_timestamp = partition[0]
                        .column(3 /* b_date_time field */)
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(0);
                    let curr_date_time = DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp(current_timestamp / 1000, 0),
                        Utc,
                    );

                    if !map.contains_key(&(bidder as usize)) {
                        map.entry(bidder as usize)
                            .or_insert_with(Vec::new)
                            .push(partition);
                    } else {
                        // get the last batch
                        let batch = map
                            .get(&(bidder as usize))
                            .unwrap()
                            .last()
                            .unwrap()
                            .last()
                            .unwrap();
                        // get the last bid's date time
                        let last_timestamp = batch
                            .column(3 /* b_date_time field */)
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap()
                            .value(batch.num_rows() - 1);
                        let last_date_time = DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp(last_timestamp / 1000, 0),
                            Utc,
                        );

                        // if the current date time isn't 10 seconds later than the last date time,
                        // then we can add the current batch to the map. Otherwise, we have a new
                        // session window.
                        if curr_date_time.signed_duration_since(last_date_time)
                            > chrono::Duration::seconds(interval as i64)
                        {
                            session = map.remove(&(bidder as usize)).unwrap();
                        }
                        map.entry(bidder as usize)
                            .or_insert_with(Vec::new)
                            .push(partition);
                    }
                    session
                })
                .collect();

            // 4. iterate over the map and find the new tumble windows
            let to_remove = find_session_windows(&map, interval, i)?;
            to_remove.iter().for_each(|bidder| {
                sessions.push(map.remove(bidder).unwrap());
            });

            let sessions: Vec<Vec<RecordBatch>> = sessions.into_iter().flatten().collect();
            if !sessions.is_empty() {
                let table = MemTable::try_new(schema.clone(), sessions)?;
                ctx.deregister_table("bid")?;
                ctx.register_table("bid", Arc::new(table))?;
                let output = collect(physical_plan(&mut ctx, sql2).await?).await?;
                println!("{}", pretty_format_batches(&output).unwrap());
            }
        }

        Ok(())
    }
}
