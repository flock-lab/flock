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

//! The data generator for YSB.

use crate::datasource::config::Config;
use crate::datasource::epoch::Epoch;
use crate::datasource::ysb::event::AdEvent;
use rand::prelude::SliceRandom;
use rand::{self, Rng, SeedableRng};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use uuid::Uuid;

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
            map,
            timestep,
            time: 1.0 + (index * 1000 / threads) as f64,
            max_time: (seconds * 1000) as f64,
        }
    }

    /// Produces the next epoch's events.
    pub fn next_epoch(&mut self) -> Result<(Epoch, (Vec<u8>, usize))> {
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

        let mut num = 0;
        while self.time < ((epoch + 1) * 1000) as f64 && self.time < self.max_time as f64 {
            let event = AdEvent {
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
                event_time: Epoch(self.time as usize),
                ip_address: String::from("0.0.0.0"),
            };

            data.extend(serde_json::to_vec(&event).unwrap());
            data.extend(vec![10]);
            num += 1;

            self.time += self.timestep;
        }

        Ok((Epoch(epoch), (data, num)))
    }

    /// Produces the events in the next epoch.
    #[allow(dead_code)]
    fn next(&mut self) -> Result<(Epoch, Vec<AdEvent>)> {
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
            data.push(AdEvent {
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
                event_time: Epoch(self.time as usize),
                ip_address: String::from("0.0.0.0"),
            });
            self.time += self.timestep;
        }

        if data.is_empty() {
            Err(Error::new(ErrorKind::Other, "out of data"))
        } else {
            Ok((Epoch(epoch), data))
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
        config.insert("threads", "1".to_string());
        config.insert("seconds", "2".to_string());
        config.insert("events-per-second", "1000".to_string());

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
                    file.write_all(b"\n")?;
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
