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

//! The Yahoo Streaming Benchmark is a single dataflow benchmark created by
//! Yahoo in 2015. The original implementation includes support for Storm,
//! Spark, Flink, and Apex. The benchmark only focuses on the latency aspect of
//! a streaming system, ignoring other important factors such as scaling, fault
//! tolerance, and load bearing.
//!
//!
//! The dataflow used in the benchmark is illustrated as following.
//!
//! ```text
//!    Map --> Filter --> Map --> Map --> Window --> Reduce
//! ```
//!
//! Its purpose is to count ad hits for each ad campaign. Events arrive from
//! Kafka in JSON string format, where each event is a flat object with the
//! following fields:
//!
//! * `user_id` - A UUID identifying the user that caused the event.
//! * `page_id` - A UUID identifying the page on which the event occurred.
//! * `ad_id` - A UUID for the specific advertisement that was interacted with.
//! * `ad_type` - A string, one of "banner", "modal", "sponsored-search",
//!   "mail", and "mobile".
//! * `event_type` - A string, one of "view", "click", and "purchase".
//! * `event_time` - An integer timestamp in milliseconds of the time the event
//!   occurred.
//! * `ip_address` - A string of the user's IP address.
//!
//!
//! The dataflow proceeds as follows: the first operator parses the JSON string
//! into an internal object. Irrelevant events are then filtered out, and only
//! ones with an `event_type` of "view" are retained. Next, all fields except
//! for `ad_id` and `event_time` are dropped. Then, a lookup in a table mapping
//! `ad_id` to `campaign_id` is done to retrieve the relevant `campaign_id`.
//! Yahoo describes this step as a **join**, as one end of this `join` is
//! streamed, whereas the other is present as a table stored in Redis.
//! Next the events are put through a **ten seconds** large hopping window. The
//! number of occurrences of each `campaign_id` within each window are finally
//! counted and stored back into Redis.
//!
//!
//! Compared to the queries shown in NEXMark, the Yahoo Streaming Benchmark is
//! exceedingly simple. It also includes some rather odd requirements that were
//! most likely simply specific to Yahoo's internal use-case, rather than born
//! out of consideration for what would make a good benchmark. Most notably
//! the lookup in Redis would present a significant bottleneck for most modern
//! streaming systems, and the required JSON deserialisation step will lead to
//! the benchmark mostly testing the JSON library's speed, rather than the
//! streaming system's actual performance in processing the data. Thus we
//! believe it is neither representative of typical streaming systems
//! applications, nor extensive enough in testing the system's expressiveness or
//! capabilities.

pub mod event;
pub mod generator;
pub mod query;
pub mod ysb;
