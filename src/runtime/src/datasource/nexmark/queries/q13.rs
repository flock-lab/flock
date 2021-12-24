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
    use crate::executor::plan::physical_plan;
    use crate::query::StreamWindow;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext as DataFusionExecutionContext;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::CsvReadOptions;
    use indoc::indoc;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;

    static SIDE_INPUT_DOWNLOAD_URL: &str = concat!(
        "https://gist.githubusercontent.com/gangliao/",
        "de6f544b8a93f26081036e0a7f8c1715/raw/",
        "586c88ad6f89d12c9f1753622eddf4788f6f0f9d/",
        "nexmark_q13_side_input.csv"
    );

    static SIDE_INPUT_DIRECTORY: &str = "/tmp/data";
    static SIDE_INPUT_FILE_NAME: &str = "nexmark_q13_side_input.csv";
    static SIDE_INPUT_TABLE_NAME: &str = "side_input";

    #[tokio::test]
    async fn local_query_13() -> Result<()> {
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

        let sql = indoc! {"
            SELECT  auction,
                    bidder,
                    price,
                    b_date_time,
                    value
            FROM    bid
                    JOIN side_input
                        ON auction = key;
        "};

        // 1. Downloading the side input data from github gist
        let data = reqwest::get(SIDE_INPUT_DOWNLOAD_URL)
            .await
            .map_err(|_| "Failed to download side input data")?
            .text_with_charset("utf-8")
            .await
            .map_err(|_| "Failed to read side input data")?;

        std::fs::create_dir_all(SIDE_INPUT_DIRECTORY).unwrap();
        let local_path = Path::new(SIDE_INPUT_DIRECTORY).join(SIDE_INPUT_FILE_NAME);
        let mut file = File::create(local_path.clone())?;
        file.write_all(data.as_bytes())?;

        // 2. Creating the table
        let mut ctx = DataFusionExecutionContext::new();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]);
        ctx.register_csv(
            SIDE_INPUT_TABLE_NAME,
            &local_path.into_os_string().to_string_lossy(),
            CsvReadOptions::new().schema(&schema).has_header(true),
        )
        .await?;

        let bid_schema = Arc::new(Bid::schema());

        // sequential processing
        for i in 0..seconds {
            // events to record batches
            let bm = events.bids.get(&DateTime::new(i)).unwrap();
            let (bids, _) = bm.get(&0).unwrap();
            let bids_batches = NEXMarkSource::to_batch(bids, bid_schema.clone());

            // register memory tables
            let bid_table = MemTable::try_new(bid_schema.clone(), vec![bids_batches])?;
            ctx.deregister_table("bid")?;
            ctx.register_table("bid", Arc::new(bid_table))?;

            // optimize query plan and execute it
            let physical_plan = physical_plan(&mut ctx, sql).await?;
            let batches = collect(physical_plan).await?;

            // show output
            println!("{}", pretty_format_batches(&batches)?);
        }

        Ok(())
    }
}
