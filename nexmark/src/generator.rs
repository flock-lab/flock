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

//! The NexMark data generator is based on the data generator by Nicolas Hafner,
//! original available on https://github.com/Shinmera/bsc-thesis/blob/master/benchmarks/src/nexmark.rs

use crate::config::{Config, NEXMarkConfig};
use crate::event::{Date, Event};
use std::io::{Error, ErrorKind, Result};

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

    /// Produces the events in the next epoch.
    pub fn next(&mut self) -> Result<(Date, Vec<Event>)> {
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
            let event = Event::new(self.events, &mut self.config);

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
    use crate::event::EventCarrier;
    use std::fs;
    use std::fs::File;
    use std::io::{Result, Write};
    use std::thread::{self, JoinHandle};

    #[test]
    fn generate_events_in_json_file() -> Result<()> {
        let mut config = Config::new();
        config.insert("person-proportion", "30".to_string());
        config.insert("auction-proportion", "30".to_string());
        config.insert("bid-proportion", "40".to_string());
        config.insert("threads", "8".to_string());
        config.insert("seconds", "5".to_string());

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
                let (t, d) = generator.next()?;
                for e in d {
                    serde_json::to_writer(&file, &EventCarrier { time: t, event: e })?;
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
