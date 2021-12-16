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

use lambda_runtime::{handler_fn, Context};
use nexmark::event::{Auction, Bid, Person};
use nexmark::{NEXMarkEvent, NEXMarkSource};
use runtime::prelude::*;
use std::sync::Arc;

async fn handler(event: NEXMarkEvent, _: Context) -> Result<NEXMarkEvent> {
    let person_schema = Arc::new(Person::schema());
    let batches = NEXMarkSource::to_batch(&event.persons, person_schema);
    let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    println!("{}", formatted);

    let auction_schema = Arc::new(Auction::schema());
    let batches = NEXMarkSource::to_batch(&event.auctions, auction_schema);
    let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    println!("{}", formatted);

    let bid_schema = Arc::new(Bid::schema());
    let batches = NEXMarkSource::to_batch(&event.bids, bid_schema);
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
    use runtime::datasource::config::Config;
    use rusoto_core::Region;
    use rusoto_lambda::{InvocationRequest, Lambda, LambdaClient};

    #[tokio::test]
    async fn umd_lambda_serialization() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10.to_string());
        config.insert("seconds", 1.to_string());
        config.insert("events-per-second", 100.to_string());
        let nex = NEXMarkSource {
            config,
            ..Default::default()
        };
        let events = nex.generate_data()?;
        let (events, _) = events.select(0, 1).unwrap();

        let ret_events = handler(events, Context::default()).await?;

        let person_schema = Arc::new(Person::schema());
        let batches = NEXMarkSource::to_batch(&ret_events.persons, person_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let auction_schema = Arc::new(Auction::schema());
        let batches = NEXMarkSource::to_batch(&ret_events.auctions, auction_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let bid_schema = Arc::new(Bid::schema());
        let batches = NEXMarkSource::to_batch(&ret_events.bids, bid_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn umd_serde_on_cloud() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10.to_string());
        config.insert("seconds", 1.to_string());
        config.insert("events-per-second", 100.to_string());
        let nex = NEXMarkSource {
            config,
            ..Default::default()
        };
        let events = nex.generate_data()?;
        let (events, _) = events.select(0, 1).unwrap();

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
        let de_events: NEXMarkEvent = serde_json::from_slice(&response.payload.unwrap()).unwrap();
        let person_schema = Arc::new(Person::schema());
        let batches = NEXMarkSource::to_batch(&de_events.persons, person_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let auction_schema = Arc::new(Auction::schema());
        let batches = NEXMarkSource::to_batch(&de_events.auctions, auction_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let bid_schema = Arc::new(Bid::schema());
        let batches = NEXMarkSource::to_batch(&de_events.bids, bid_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        Ok(())
    }
}
