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

//! NEXMark events for testing.

use crate::datasource::nexmark::event::{Auction, Bid, Person};
use crate::datasource::nexmark::NEXMarkEvent;
use crate::error::Result;
use crate::transmute::event_bytes_to_batch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use lambda_runtime::{service_fn, LambdaEvent};
use std::sync::Arc;

#[allow(dead_code)]
async fn handler(event: LambdaEvent<NEXMarkEvent>) -> Result<NEXMarkEvent> {
    let payload = event.payload;
    let person_schema = Arc::new(Person::schema());
    let batches = event_bytes_to_batch(&payload.persons, person_schema, 1024);
    println!("{}", pretty_format_batches(&batches)?);

    let auction_schema = Arc::new(Auction::schema());
    let batches = event_bytes_to_batch(&payload.auctions, auction_schema, 1024);
    println!("{}", pretty_format_batches(&batches)?);

    let bid_schema = Arc::new(Bid::schema());
    let batches = event_bytes_to_batch(&payload.bids, bid_schema, 1024);
    println!("{}", pretty_format_batches(&batches)?);

    Ok(payload)
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<()> {
    lambda_runtime::run(service_fn(handler)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::config::Config;
    use crate::datasource::nexmark::NEXMarkSource;
    use http::{HeaderMap, HeaderValue};
    use lambda_runtime::Context;
    use rusoto_core::Region;
    use rusoto_lambda::{InvocationRequest, Lambda, LambdaClient};

    #[tokio::test]
    async fn umd_lambda_serialization() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10u32.to_string());
        config.insert("seconds", 1u32.to_string());
        config.insert("events-per-second", 100u32.to_string());
        let nex = NEXMarkSource {
            config,
            ..Default::default()
        };
        let events = nex.generate_data()?;
        let (events, _) = events.select(0, 1).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            "lambda-runtime-aws-request-id",
            HeaderValue::from_static("my-id"),
        );
        headers.insert(
            "lambda-runtime-deadline-ms",
            HeaderValue::from_static("123"),
        );
        headers.insert(
            "lambda-runtime-invoked-function-arn",
            HeaderValue::from_static("arn::myarn"),
        );
        headers.insert(
            "lambda-runtime-trace-id",
            HeaderValue::from_static("arn::myarn"),
        );
        let tried = Context::try_from(headers);
        assert!(tried.is_ok());

        let ret_events = handler(LambdaEvent::new(events, tried.unwrap())).await?;

        let person_schema = Arc::new(Person::schema());
        let batches = event_bytes_to_batch(&ret_events.persons, person_schema, 1024);
        println!("{}", pretty_format_batches(&batches)?);

        let auction_schema = Arc::new(Auction::schema());
        let batches = event_bytes_to_batch(&ret_events.auctions, auction_schema, 1024);
        println!("{}", pretty_format_batches(&batches)?);

        let bid_schema = Arc::new(Bid::schema());
        let batches = event_bytes_to_batch(&ret_events.bids, bid_schema, 1024);
        println!("{}", pretty_format_batches(&batches)?);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn umd_serde_on_cloud() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10u32.to_string());
        config.insert("seconds", 1u32.to_string());
        config.insert("events-per-second", 100u32.to_string());
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
        let batches = event_bytes_to_batch(&de_events.persons, person_schema, 1024);
        println!("{}", pretty_format_batches(&batches)?);

        let auction_schema = Arc::new(Auction::schema());
        let batches = event_bytes_to_batch(&de_events.auctions, auction_schema, 1024);
        println!("{}", pretty_format_batches(&batches)?);

        let bid_schema = Arc::new(Bid::schema());
        let batches = event_bytes_to_batch(&de_events.bids, bid_schema, 1024);
        println!("{}", pretty_format_batches(&batches)?);

        Ok(())
    }
}
