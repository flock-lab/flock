// Copyright 2020 UMD Database Group. All Rights Reserved.
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

use abomonation::decode;
use nexmark::event::{Auction, Bid, Person};
use nexmark::{NexMarkEvent, NexMarkSource};
use runtime::prelude::*;
use std::sync::Arc;
use umd_lambda_runtime::{handler_fn, Context};

async fn handler(event: Vec<u8>, _: Context) -> Result<Vec<u8>> {
    // decompression and deserialization
    unsafe {
        let mut bytes = Encoding::Zstd.decompress(&event);
        if let Some((de_events, _)) = decode::<NexMarkEvent>(&mut bytes) {
            let person_schema = Arc::new(Person::schema());
            let batches = NexMarkSource::to_batch(&(*de_events).persons, person_schema);
            let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
            println!("{}", formatted);

            let auction_schema = Arc::new(Auction::schema());
            let batches = NexMarkSource::to_batch(&(*de_events).auctions, auction_schema);
            let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
            println!("{}", formatted);

            let bid_schema = Arc::new(Bid::schema());
            let batches = NexMarkSource::to_batch(&(*de_events).bids, bid_schema);
            let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
            println!("{}", formatted);
        }
    }

    Ok(event)
}

#[tokio::main]
async fn main() -> Result<()> {
    umd_lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexmark::config::Config;

    #[tokio::test]
    async fn umd_lambda_serialization() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10.to_string());
        config.insert("seconds", 1.to_string());
        config.insert("events-per-second", 100.to_string());
        let nex = NexMarkSource {
            config,
            ..Default::default()
        };
        let events = nex.generate_data()?;
        let events = events.select(0, 1).unwrap();

        // serialization and compression
        let en_events = events.encode(Encoding::Zstd);

        let ret_events = handler(en_events, Context::default()).await?;

        // decompression and deserialization
        unsafe {
            let mut bytes = Encoding::Zstd.decompress(&ret_events);
            if let Some((de_events, _)) = decode::<NexMarkEvent>(&mut bytes) {
                let person_schema = Arc::new(Person::schema());
                let batches = NexMarkSource::to_batch(&(*de_events).persons, person_schema);
                let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
                println!("{}", formatted);

                let auction_schema = Arc::new(Auction::schema());
                let batches = NexMarkSource::to_batch(&(*de_events).auctions, auction_schema);
                let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
                println!("{}", formatted);

                let bid_schema = Arc::new(Bid::schema());
                let batches = NexMarkSource::to_batch(&(*de_events).bids, bid_schema);
                let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
                println!("{}", formatted);
            }
        }

        Ok(())
    }
}
