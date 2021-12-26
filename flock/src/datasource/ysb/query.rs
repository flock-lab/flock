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

//! This is a local test program for the YSB query.

#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use crate::datasource::epoch::Epoch;
    use crate::datasource::ysb::event::{AdEvent, Campaign};
    use crate::datasource::ysb::YSBSource;
    use crate::error::Result;
    use crate::runtime::executor::plan::physical_plan;
    use crate::runtime::query::{Schedule, StreamWindow};
    use crate::transmute::event_bytes_to_batch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use indoc::indoc;
    use std::sync::Arc;

    #[tokio::test]
    async fn local_ysb_query() -> Result<()> {
        // benchmark configuration
        let seconds = 20;
        let threads = 1;
        let event_per_second = 1000;
        let window_size = 10;
        let ysb = YSBSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::TumblingWindow(Schedule::Seconds(window_size)),
        );

        // data source generation
        let stream = ysb.generate_data()?;

        let sql = indoc! {"
            SELECT campaign_id,
                   Count(*)
            FROM   ad_events
                   INNER JOIN campaigns
                           ON ad_id = c_ad_id
            WHERE  event_type = 'view'
            GROUP  BY campaign_id
        "};

        let ad_event_schema = Arc::new(AdEvent::schema());
        let campaign_schema = Arc::new(Campaign::schema());
        let (campaigns, _) = stream.campaigns.clone();
        let campaign_batches = event_bytes_to_batch(&campaigns, campaign_schema.clone(), 1024);

        for i in 0..seconds / window_size {
            let mut batches = vec![];
            let d = i * window_size;
            // moves the tumbling window
            for i in d..d + window_size {
                let m = stream.events.get(&Epoch::new(i)).unwrap();
                let (ad_events, _) = m.get(&0).unwrap();
                batches.push(event_bytes_to_batch(
                    ad_events,
                    ad_event_schema.clone(),
                    1024,
                ));
            }

            // register memory tables
            let mut ctx = datafusion::execution::context::ExecutionContext::new();
            let ad_event_table = MemTable::try_new(ad_event_schema.clone(), batches)?;
            ctx.register_table("ad_events", Arc::new(ad_event_table))?;

            let campaign_table =
                MemTable::try_new(campaign_schema.clone(), vec![campaign_batches.clone()])?;
            ctx.register_table("campaigns", Arc::new(campaign_table))?;

            // optimize query plan and execute it
            let physical_plan = physical_plan(&mut ctx, sql).await?;
            let output = collect(physical_plan).await?;

            // show output
            println!(
                "{}",
                arrow::util::pretty::pretty_format_batches(&output).unwrap()
            );
        }

        Ok(())
    }
}
