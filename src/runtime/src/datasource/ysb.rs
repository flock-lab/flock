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
//!     --> Map --> Filter --> Map --> Map --> Window --> Reduce
//!
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
//! `ad_ids` to `campaign_ids` is done to retrieve the relevant `campaign_id`.
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

use crate::datasource::config::Config;
use rand::prelude::SliceRandom;
use rand::{self, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use uuid::Uuid;

/// Advertising event types.
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Event {
    /// A UUID identifying the user that caused the event.
    user_id:    String,
    /// A UUID identifying the page on which the event occurred.
    page_id:    String,
    /// A UUID for the specific advertisement that was interacted with.
    ad_id:      String,
    /// A string, one of "banner", "modal", "sponsored-search", "mail", and
    /// "mobile".
    ad_type:    String,
    /// A string, one of "view", "click", and "purchase".
    event_type: String,
    /// An integer timestamp in milliseconds of the time the event occurred.
    event_time: usize,
    /// A string of the user's IP address.
    ip_address: String,
}

/// A generator for the Yahoo Streaming Benchmark.
#[derive(Clone)]
pub struct YSBGenerator {
    /// A campaign map from ad_id to campaign_id.
    pub map:      HashMap<String, String>,
    /// The total time of the generator.
    pub time:     f64,
    /// The time step for the generator.
    pub timestep: f64,
    /// The maximum time for the generator.
    pub max_time: f64,
}

impl YSBGenerator {
    /// Creates a new YSB generator.
    pub fn new(config: &Config) -> Self {
        let index = config.get_as_or("worker-index", 0);
        let threads = config.get_as_or("threads", 1);
        let campaigns = config.get_as_or("campaigns", 100);
        let ads = config.get_as_or("ads", 10);
        let seconds = config.get_as_or("seconds", 10);
        let events_per_second = config.get_as_or("events-per-second", 1000);
        let timestep = (1000 * threads) as f64 / events_per_second as f64;
        // Generate campaigns map
        let mut map = HashMap::new();
        for _ in 0..campaigns {
            let campaign_id = format!("{}", Uuid::new_v4());
            for _ in 0..ads {
                let ad_id = format!("{}", Uuid::new_v4());
                map.insert(ad_id, campaign_id.clone());
            }
        }

        YSBGenerator {
            map:      map,
            time:     1.0 + (index * 1000 / threads) as f64,
            timestep: timestep,
            max_time: (seconds * 1000) as f64,
        }
    }

    /// Produces the events in the next epoch.
    pub fn next(&mut self) -> Result<(usize, Vec<Event>)> {
        let ad_types = vec!["banner", "modal", "sponsored-search", "mail", "mobile"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let event_types = vec!["view", "click", "purchase"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();

        let mut rng = rand::rngs::StdRng::seed_from_u64(0xDEAD); // Predictable RNG clutch
        let mut data = Vec::with_capacity((1000.0 / self.timestep) as usize);
        let epoch = self.time as usize / 1000;

        while self.time < ((epoch + 1) * 1000) as f64 && self.time < self.max_time as f64 {
            data.push(Event {
                user_id:    format!("{}", Uuid::new_v4()),
                page_id:    format!("{}", Uuid::new_v4()),
                ad_id:      self
                    .map
                    .keys()
                    .nth(rng.gen_range(0..self.map.len()))
                    .unwrap()
                    .clone(),
                ad_type:    ad_types.choose(&mut rng).unwrap().to_string(),
                event_type: event_types.choose(&mut rng).unwrap().to_string(),
                event_time: self.time as usize,
                ip_address: String::from("0.0.0.0"),
            });
            self.time += self.timestep;
        }

        if data.is_empty() {
            Err(Error::new(ErrorKind::Other, "out of data"))
        } else {
            Ok((epoch, data))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::thread::{self, JoinHandle};

    #[test]
    fn generate_ysb_data() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", format!("1"));
        config.insert("seconds", format!("2"));
        config.insert("events-per-second", format!("1000"));

        let data_dir = format!("{}/ysb", config.get_or("data-dir", "data"));
        let partitions = config.get_as_or("threads", 1);
        let campaigns = config.get_as_or("campaigns", 100);
        let ads = config.get_as_or("ads", 10);
        let seconds = config.get_as_or("seconds", 2);
        let events_per_second = config.get_as_or("events-per-second", 1000);
        fs::create_dir_all(&data_dir)?;

        println!(
            "Generating {} events/s for {}s over {} partitions for {} campaigns with {} ads each.",
            events_per_second, seconds, partitions, campaigns, ads
        );
        let generator = YSBGenerator::new(&config);
        let campaign_file = File::create(format!("{}/campaigns.json", &data_dir))?;
        serde_json::to_writer(campaign_file, &generator.map)?;

        // Generate events
        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        for p in 0..partitions {
            let mut generator = generator.clone();
            let mut file = File::create(format!("{}/events-{}.json", &data_dir, p))?;
            threads.push(thread::spawn(move || loop {
                let (_, d) = generator.next()?;
                for e in d {
                    serde_json::to_writer(&file, &e)?;
                    file.write(b"\n")?;
                }
            }));
        }
        for t in threads.drain(..) {
            t.join().unwrap().or_else(|e| {
                if e.to_string() == "out of data" {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
        }
        Ok(())
    }
}
