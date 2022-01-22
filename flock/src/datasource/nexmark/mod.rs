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

//! NEXMark is an evolution of the XMark benchmark. XMark was initially designed
//! for relational databases and defines a small schema for an online auction
//! house. NEXMark builds on this idea and presents a schema of three concrete
//! tables, and a set of queries to run in a streaming sense. NEXMark attempts
//! to provide a bench- mark that is both extensive in its use of operators, and
//! close to a real-world appli- cation by being grounded in a well-known
//! problem.
//!
//! The original benchmark proposed by Tucker et al. was adopted and extended by
//! the Apache Foundation for their use in Beam, a system intended to provide a
//! general API for a variety of streaming systems. We will follow the [Beam
//! implementation](https://beam.apache.org/documentation/sdks/java/testing/nexmark/),
//! as it is the most widely adopted one, despite having several differences to
//! the benchmark originally. NEXMark as implemented by Beam does not concern
//! itself with questions of scaling, load bearing, and fault tolerance,
//! focusing solely on the latency aspect.
//!
//! The queries are based on three types of events that can enter the system:
//! `Person`, `Auction`, and `Bid`. Their fields are as follows:
//!
//! # Person
//!
//! - id: A person-unique integer ID.
//! - name: A string for the person’s full name.
//! - email_address: The person’s email address as a string.
//! - credit_card: The credit card number as a 19-letter string.
//! - city: One of several US city names as a string.
//! - state: One of several US states as a two-letter string.
//! - date_time: A millisecond timestamp for the event origin.
//!
//! # Auction
//!
//! - id: An auction-unique integer ID.
//! - item_name: The name of the item being auctioned.
//! - description: A short description of the item.
//! - initial_bid: The initial bid price in cents.
//! - reserve: The minimum price for the auction to succeed.
//! - date_time: A millisecond timestamp for the event origin.
//! - expires: A UNIX epoch timestamp for the expiration date of the auction.
//! - seller: The ID of the person that created this auction.
//! - category: The ID of the category this auction belongs to.
//!
//! # Bid
//!
//! - auction: The ID of the auction this bid is for.
//! - bidder: The ID of the person that placed this bid.
//! - price: The price in cents that the person bid for.
//! - date_time: A millisecond timestamp for the event origin.
//!
//! # Reference
//!
//! Pete Tucker, Kristin Tufte, Vassilis Papadimos, David Maier.
//! NEXMark – A Benchmark for Queries over Data Streams. June 2010.
//! <http://datalab.cs.pdx.edu/niagara/pstream/nexmark.pdf>.

pub mod config;
pub mod event;
pub mod generator;
pub mod nexmark;

mod queries;

pub use self::config::NEXMarkConfig;
pub use self::event::{side_input_schema, Auction, Bid, Person};
pub use self::nexmark::{NEXMarkEvent, NEXMarkSource, NEXMarkStream};
use crate::configs::FLOCK_TARGET_PARTITIONS;
use crate::error::Result;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

/// The NEXMark tables.
pub const NEXMARK_TABLES: &[&str] = &["person", "auction", "bid", "side_input"];

/// Get the schema for a given NEXMark table.
pub fn get_nexmark_schema(table: &str) -> Schema {
    match table {
        "person" => Person::schema(),
        "auction" => Auction::schema(),
        "bid" => Bid::schema(),
        "side_input" => side_input_schema(),
        _ => unimplemented!(),
    }
}

/// Register the NEXMark tables with empty data.
pub async fn register_nexmark_tables_with_config(
    config: ExecutionConfig,
) -> Result<ExecutionContext> {
    let mut ctx = ExecutionContext::with_config(config);
    let person_schema = Arc::new(Person::schema());
    let person_table = MemTable::try_new(
        person_schema.clone(),
        vec![vec![RecordBatch::new_empty(person_schema)]],
    )?;
    ctx.register_table("person", Arc::new(person_table))?;

    let auction_schema = Arc::new(Auction::schema());
    let auction_table = MemTable::try_new(
        auction_schema.clone(),
        vec![vec![RecordBatch::new_empty(auction_schema)]],
    )?;
    ctx.register_table("auction", Arc::new(auction_table))?;

    let bid_schema = Arc::new(Bid::schema());
    let bid_table = MemTable::try_new(
        bid_schema.clone(),
        vec![vec![RecordBatch::new_empty(bid_schema)]],
    )?;
    ctx.register_table("bid", Arc::new(bid_table))?;

    // For NEXMark Q13
    let side_input_schema = Arc::new(side_input_schema());
    let side_input_table = MemTable::try_new(
        side_input_schema.clone(),
        vec![vec![RecordBatch::new_empty(side_input_schema)]],
    )?;
    ctx.register_table("side_input", Arc::new(side_input_table))?;

    Ok(ctx)
}

/// Register the NEXMark tables with empty data.
pub async fn register_nexmark_tables() -> Result<ExecutionContext> {
    let config = ExecutionConfig::new().with_target_partitions(*FLOCK_TARGET_PARTITIONS);
    register_nexmark_tables_with_config(config).await
}
