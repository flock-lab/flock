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

use lambda_runtime::{handler_fn, Context};
use nexmark::event::{Auction, Bid, Person};
use nexmark::{NexMarkEvent, NexMarkSource};
use runtime::prelude::*;
use std::sync::Arc;

async fn handler(event: NexMarkEvent, _: Context) -> Result<NexMarkEvent> {
    let person_schema = Arc::new(Person::schema());
    let batches = NexMarkSource::to_batch(&event.persons, person_schema);
    let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    println!("{}", formatted);

    let auction_schema = Arc::new(Auction::schema());
    let batches = NexMarkSource::to_batch(&event.auctions, auction_schema);
    let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    println!("{}", formatted);

    let bid_schema = Arc::new(Bid::schema());
    let batches = NexMarkSource::to_batch(&event.bids, bid_schema);
    let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    println!("{}", formatted);

    Ok(event)
}

#[tokio::main]
async fn main() -> Result<()> {
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexmark::config::Config;
    use rusoto_core::Region;
    use rusoto_lambda::{InvocationRequest, Lambda, LambdaClient};

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

        let ret_events = handler(events, Context::default()).await?;

        let person_schema = Arc::new(Person::schema());
        let batches = NexMarkSource::to_batch(&ret_events.persons, person_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let auction_schema = Arc::new(Auction::schema());
        let batches = NexMarkSource::to_batch(&ret_events.auctions, auction_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let bid_schema = Arc::new(Bid::schema());
        let batches = NexMarkSource::to_batch(&ret_events.bids, bid_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        Ok(())
    }

    #[tokio::test]
    async fn umd_serde_on_cloud() -> Result<()> {
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
        let en_events = serde_json::to_vec(&events).unwrap();

        let client = LambdaClient::new(Region::UsEast1);
        let request = InvocationRequest {
            function_name: "arn:aws:lambda:us-east-1:942368842860:function:umd_runtime".to_string(),
            payload: Some(en_events.into()),
            invocation_type: Some("RequestResponse".to_string()),
            ..Default::default()
        };
        let response = client.invoke(request).await.unwrap();

        // decompression and deserialization
        let de_events: NexMarkEvent = serde_json::from_slice(&response.payload.unwrap()).unwrap();
        let person_schema = Arc::new(Person::schema());
        let batches = NexMarkSource::to_batch(&de_events.persons, person_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let auction_schema = Arc::new(Auction::schema());
        let batches = NexMarkSource::to_batch(&de_events.auctions, auction_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let bid_schema = Arc::new(Bid::schema());
        let batches = NexMarkSource::to_batch(&de_events.bids, bid_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        Ok(())
    }
}
