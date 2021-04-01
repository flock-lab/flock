// Copyright 2021 UMD Database Group. All Rights Reserved.
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
//! Pete Tucker et al. NEXMark–A Benchmark for Queries over Data Streams
//! (DRAFT). Tech. rep. Technical report, Septembers, 2008.
//! <http://datalab.cs.pdx.edu/niagara/pstream/nexmark.pdf>.

#![warn(missing_docs)]
// Clippy lints, some should be disabled incrementally
#![allow(
    clippy::float_cmp,
    clippy::module_inception,
    clippy::new_without_default,
    clippy::ptr_arg,
    clippy::type_complexity,
    clippy::wrong_self_convention,
    clippy::should_implement_trait
)]
#![feature(get_mut_unchecked)]

extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;

pub mod config;
pub mod event;
pub mod generator;
