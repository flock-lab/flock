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

use crate::config::FLOCK_CONF;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use lazy_static::lazy_static;
use log::info;
use nexmark::event::{Auction, Bid, Person};
use nexmark::{NexMarkSource, NexMarkStream};
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{InvocationRequest, InvocationResponse, Lambda, LambdaClient};
use std::sync::Arc;

lazy_static! {
    static ref PERSON_SCHEMA: SchemaRef = Arc::new(Person::schema());
    static ref AUCTION_SCHEMA: SchemaRef = Arc::new(Auction::schema());
    static ref BID_SCHEMA: SchemaRef = Arc::new(Bid::schema());
    static ref LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
    static ref BATCH_SIZE: usize = FLOCK_CONF["lambda"]["granule"].parse::<usize>().unwrap();
}

#[allow(dead_code)]
static LAMBDA_SYNC_CALL: &str = "RequestResponse";

#[allow(dead_code)]
static LAMBDA_ASYNC_CALL: &str = "Event";

/// A wrapper around a lambda function that can be called asynchronously.
pub async fn invoke_lambda_function(
    function_name: String,
    payload: Option<Bytes>,
) -> Result<InvocationResponse> {
    match LAMBDA_CLIENT
        .invoke(InvocationRequest {
            function_name,
            payload,
            invocation_type: Some(format!("{}", LAMBDA_ASYNC_CALL)),
            ..Default::default()
        })
        .await
    {
        Ok(response) => return Ok(response),
        Err(err) => {
            return Err(FlockError::Execution(format!(
                "Lambda function execution failure: {}",
                err
            )))
        }
    }
}

/// Converts a NexMark stream into the Flock's Payload.
///
/// ## Arguments
/// * `events` - A stream of NexMark events.
/// * `time` - The time of the event.
/// * `generator` - The name of the generator.
/// * `query number` - The id of the nexmark query.
/// * `uuid` - The uuid of the produced payload.
///
/// ## Returns
/// A Flock's Payload.
pub fn nexmark_event_to_payload(
    events: Arc<NexMarkStream>,
    time: usize,
    generator: usize,
    query_number: usize,
    uuid: Uuid,
) -> Result<Payload> {
    let (event, (persons_num, auctions_num, bids_num)) = events
        .select(time, generator)
        .expect("Failed to select event.");

    if event.persons.is_empty() && event.auctions.is_empty() && event.bids.is_empty() {
        return Err(FlockError::Execution("No Nexmark input!".to_owned()));
    }

    info!(
        "Epoch {}: {} persons, {} auctions, {} bids.",
        time, persons_num, auctions_num, bids_num
    );

    match query_number {
        0 | 1 | 2 | 5 | 7 => Ok(to_payload(
            &NexMarkSource::to_batch(&event.bids, BID_SCHEMA.clone()),
            &vec![],
            uuid,
        )),
        3 | 8 => Ok(to_payload(
            &NexMarkSource::to_batch(&event.persons, PERSON_SCHEMA.clone()),
            &NexMarkSource::to_batch(&event.auctions, AUCTION_SCHEMA.clone()),
            uuid,
        )),
        4 | 6 | 9 => Ok(to_payload(
            &NexMarkSource::to_batch(&event.auctions, AUCTION_SCHEMA.clone()),
            &NexMarkSource::to_batch(&event.bids, BID_SCHEMA.clone()),
            uuid,
        )),
        _ => unimplemented!(),
    }
}

/// Converts a NexMark stream into record batches for Arrow.
///
/// ## Arguments
/// * `events` - A stream of NexMark events.
/// * `time` - The time of the event.
/// * `generator` - The name of the generator.
/// * `query number` - The id of the nexmark query.
///
/// ## Returns
/// A Flock's Payload.
pub fn nexmark_event_to_batches(
    events: Arc<NexMarkStream>,
    time: usize,
    generator: usize,
    query_number: usize,
) -> Result<(Vec<Vec<RecordBatch>>, Vec<Vec<RecordBatch>>)> {
    let (event, (persons_num, auctions_num, bids_num)) = events
        .select(time, generator)
        .expect("Failed to select event.");

    if event.persons.is_empty() && event.auctions.is_empty() && event.bids.is_empty() {
        return Err(FlockError::Execution("No Nexmark input!".to_owned()));
    }

    info!(
        "Epoch {}: {} persons, {} auctions, {} bids.",
        time, persons_num, auctions_num, bids_num
    );

    let (r1, r2) = match query_number {
        0 | 1 | 2 | 5 | 7 => (
            NexMarkSource::to_batch_v2(&event.bids, BID_SCHEMA.clone(), *BATCH_SIZE * 2),
            vec![],
        ),
        3 | 8 => (
            NexMarkSource::to_batch_v2(&event.persons, PERSON_SCHEMA.clone(), *BATCH_SIZE / 2),
            NexMarkSource::to_batch_v2(&event.auctions, AUCTION_SCHEMA.clone(), *BATCH_SIZE / 2),
        ),
        4 | 6 | 9 => (
            NexMarkSource::to_batch_v2(&event.auctions, AUCTION_SCHEMA.clone(), *BATCH_SIZE / 2),
            NexMarkSource::to_batch_v2(&event.bids, BID_SCHEMA.clone(), *BATCH_SIZE / 2),
        ),
        _ => unimplemented!(),
    };

    let step = if r2.is_empty() { 2 } else { 1 };

    let batch_partition = |batch: Vec<RecordBatch>| {
        (0..batch.len())
            .step_by(step)
            .map(|start| {
                let end = if start + step > batch.len() {
                    batch.len()
                } else {
                    start + step
                };
                batch[start..end].to_vec()
            })
            .collect::<Vec<_>>()
    };

    if r2.is_empty() {
        Ok((batch_partition(r1), vec![]))
    } else {
        Ok((batch_partition(r1), batch_partition(r2)))
    }
}
