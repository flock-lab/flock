// Copyright (c) 2021 UMD Database Group. All Rights Reserved.
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

//! The NexMark data generator is based on the data generator by Nicolas Hafner,
//! original available on https://github.com/Shinmera/bsc-thesis/blob/master/benchmarks/src/nexmark.rs

use crate::datasource::nexmark::config::{Config, NEXMarkConfig};
use crate::datasource::nexmark::event::{Auction, Bid, Date, Event, Person};
use std::io::{Error, ErrorKind, Result, Write};

/// The NexMark event generator.
#[derive(Clone)]
pub struct NEXMarkGenerator {
    /// The NexMark configuration.
    pub config:  NEXMarkConfig,
    /// How many records are produced.
    pub events:  usize,
    /// How long an experiment is supposed to run.
    pub seconds: usize,
}

impl NEXMarkGenerator {
    /// Creates a new `NEXMarkGenerator`.
    pub fn new(config: &Config) -> Self {
        NEXMarkGenerator {
            config:  NEXMarkConfig::new(config),
            events:  0,
            seconds: config.get_as_or("seconds", 60),
        }
    }

    /// Produces the next epoch and classifies the events.
    pub fn next_epoch(
        &mut self,
        p: usize,
    ) -> Result<(Date, ((Vec<u8>, usize), (Vec<u8>, usize), (Vec<u8>, usize)))> {
        let epoch = (self
            .config
            .event_timestamp(self.events + self.config.first_event_id)
            - self.config.base_time)
            / 1000;

        let mut p_buf = Vec::with_capacity(
            200 * self.config.events_per_epoch / self.config.num_event_generators,
        );
        let mut a_buf = Vec::with_capacity(
            500 * self.config.events_per_epoch / self.config.num_event_generators,
        );
        let mut b_buf = Vec::with_capacity(
            100 * self.config.events_per_epoch / self.config.num_event_generators,
        );
        let mut p_num = 0;
        let mut a_num = 0;
        let mut b_num = 0;
        loop {
            let time = self
                .config
                .event_timestamp(self.events + self.config.first_event_id);
            let next_epoch = (time - self.config.base_time) / 1000;
            let event = Event::new(self.events, p, &mut self.config);

            if next_epoch < self.seconds && next_epoch == epoch {
                self.events += 1;
                match event {
                    Event::Person(person) => {
                        p_buf.extend(serde_json::to_vec(&person).unwrap());
                        p_buf.extend(vec![10]);
                        p_num += 1;
                    }
                    Event::Auction(auction) => {
                        a_buf.extend(serde_json::to_vec(&auction).unwrap());
                        a_buf.extend(vec![10]);
                        a_num += 1;
                    }
                    Event::Bid(bid) => {
                        b_buf.extend(serde_json::to_vec(&bid).unwrap());
                        b_buf.extend(vec![10]);
                        b_num += 1;
                    }
                }
            } else {
                break;
            }
        }

        Ok((
            Date::new(epoch),
            ((p_buf, p_num), (a_buf, a_num), (b_buf, b_num)),
        ))
    }

    /// Produces the events in the next epoch (for testing).
    pub fn next(&mut self, p: usize) -> Result<(Date, Vec<Event>)> {
        let mut data = Vec::with_capacity((1000.0 / self.config.inter_event_delays[0]) as usize);
        let epoch = (self
            .config
            .event_timestamp(self.events + self.config.first_event_id)
            - self.config.base_time)
            / 1000;

        loop {
            let time = self
                .config
                .event_timestamp(self.events + self.config.first_event_id);
            let next_epoch = (time - self.config.base_time) / 1000;
            let event = Event::new(self.events, p, &mut self.config);

            if next_epoch < self.seconds && next_epoch == epoch {
                self.events += 1;
                data.push(event);
            } else {
                break;
            }
        }

        if data.is_empty() {
            Err(Error::new(ErrorKind::Other, "out of data"))
        } else {
            Ok((Date::new(epoch), data))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::nexmark::event::EventCarrier;
    use std::fs;
    use std::fs::File;
    use std::io::{Result, Write};
    use std::thread::{self, JoinHandle};

    #[test]
    fn generate_events_in_json_file() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", "100".to_string());
        config.insert("seconds", "1".to_string());
        config.insert("events-per-second", "1000".to_string());

        let data_dir = format!("{}/nexmark", config.get_or("data-dir", "data"));
        fs::create_dir_all(&data_dir)?;

        let seconds = config.get_as_or("seconds", 5);
        let partitions = config.get_as_or("threads", 8);
        println!(
            "Generating events for {}s over {} partitions.",
            seconds, partitions
        );

        let generator = NEXMarkGenerator::new(&config);

        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        for p in 0..partitions {
            let mut file = File::create(format!("{}/events-{}.txt", &data_dir, p))?;
            let mut generator = generator.clone();
            threads.push(thread::spawn(move || loop {
                let (t, d) = generator.next(p)?;
                for e in d {
                    serde_json::to_writer(&file, &EventCarrier { time: t, event: e })?;
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
