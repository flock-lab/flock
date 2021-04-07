// Copyright 2020 UMD Database Group. All Rights Reserved.
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

//! Nexmark benchmark suite

use arrow::datatypes::{Schema, SchemaRef};
use arrow::json::{self, reader::infer_json_schema};
use arrow::record_batch::RecordBatch;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::datasource::nexmark::config::{Config, NEXMarkConfig};
use crate::datasource::nexmark::event::{Auction, Bid, Date, Person};
use crate::datasource::nexmark::generator::NEXMarkGenerator;
use crate::error::Result;
use crate::query::StreamWindow;

type Epoch = Date;
type SourceId = usize;
type Partition = usize;
type NumEvents = usize;

/// A struct to classify three types of events.
#[derive(Debug, Default)]
pub struct NexMarkEvents {
    /// The Person events in different epochs and partitions.
    pub persons:  HashMap<Epoch, HashMap<SourceId, (Vec<u8>, NumEvents)>>,
    /// The Auction events in different epochs and partitions.
    pub auctions: HashMap<Epoch, HashMap<SourceId, (Vec<u8>, NumEvents)>>,
    /// The Bid events in different epochs and partitions.
    pub bids:     HashMap<Epoch, HashMap<SourceId, (Vec<u8>, NumEvents)>>,
}

impl NexMarkEvents {
    /// Creates a new NexMarkEvents.
    pub fn new() -> Self {
        NexMarkEvents {
            persons:  HashMap::new(),
            auctions: HashMap::new(),
            bids:     HashMap::new(),
        }
    }
}

/// A struct to manage all Nexmark info in cloud environment.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct NexMarkSource {
    /// The NexMark configuration.
    pub config: Config,
    /// The windows group stream elements by time or rows.
    pub window: StreamWindow,
}

impl Default for NexMarkSource {
    fn default() -> Self {
        let mut config = Config::new();
        config.insert("threads", 100.to_string());
        config.insert("seconds", 10.to_string());
        config.insert("events-per-second", 100_1000.to_string());
        let window = StreamWindow::None;
        NexMarkSource { config, window }
    }
}

impl NexMarkSource {
    /// Creates a new Nexmark benchmark data source.
    pub fn new(
        seconds: usize,
        threads: usize,
        events_per_second: usize,
        window: StreamWindow,
    ) -> Self {
        let mut config = Config::new();
        config.insert("threads", threads.to_string());
        config.insert("seconds", seconds.to_string());
        config.insert("events-per-second", events_per_second.to_string());
        NexMarkSource { config, window }
    }

    /// Assigns each event with the specific type for the upcoming processing.
    fn assgin_events(
        events: &mut NexMarkEvents,
        t: Epoch,
        p: Partition,
        persons: (Vec<u8>, usize),
        auctions: (Vec<u8>, usize),
        bids: (Vec<u8>, usize),
    ) {
        match events.persons.get_mut(&t) {
            Some(pm) => {
                (*pm).insert(p, persons);
            }
            None => {
                let mut pm = HashMap::new();
                pm.insert(p, persons);
                events.persons.insert(t, pm);
            }
        }

        match events.auctions.get_mut(&t) {
            Some(am) => {
                (*am).insert(p, auctions);
            }
            None => {
                let mut am = HashMap::new();
                am.insert(p, auctions);
                events.auctions.insert(t, am);
            }
        }

        match events.bids.get_mut(&t) {
            Some(bm) => {
                (*bm).insert(p, bids);
            }
            None => {
                let mut bm = HashMap::new();
                bm.insert(p, bids);
                events.bids.insert(t, bm);
            }
        }
    }

    /// Generates data events for Nexmark benchmark.
    pub fn generate_data(&self) -> Result<NexMarkEvents> {
        let partitions: usize = self.config.get_as_or("threads", 100);
        let seconds: usize = self.config.get_as_or("seconds", 10);

        info!(
            "Generating events for {}s over {} partitions.",
            seconds, partitions
        );

        let generator = NEXMarkGenerator::new(&self.config);
        let events_handle = Arc::new(Mutex::new(NexMarkEvents::new()));

        let mut threads = vec![];
        for p in 0..partitions {
            let mut generator = generator.clone();
            let events_handle = Arc::clone(&events_handle);
            threads.push(thread::spawn(move || loop {
                let (t, d) = generator.next_epoch(p).unwrap();
                if !((d.0).0.is_empty() && (d.1).0.is_empty() && (d.2).0.is_empty()) {
                    let mut events = events_handle.lock().unwrap();
                    NexMarkSource::assgin_events(&mut events, t, p, d.0, d.1, d.2);
                } else {
                    break;
                }
            }));
        }
        for t in threads.drain(..) {
            t.join().unwrap();
        }

        let mut events = events_handle.lock().unwrap();
        Ok(std::mem::take(&mut events))
    }

    /// Converts NexMark events to record batches in Arrow.
    pub fn to_batch(events: &[u8], schema: SchemaRef) -> Vec<RecordBatch> {
        let batch_size = 1024;
        let mut reader = json::Reader::new(BufReader::new(events), schema, batch_size, None);

        let mut batches = vec![];
        while let Some(batch) = reader.next().unwrap() {
            batches.push(batch);
        }
        batches
    }

    /// Counts the number of events. (for testing)
    pub fn count_events(&self, events: &NexMarkEvents) -> usize {
        let threads: usize = self.config.get_as_or("threads", 100);
        let seconds: usize = self.config.get_as_or("seconds", 10);
        (0..threads)
            .map(|p| {
                (0..seconds)
                    .map(|s| {
                        events
                            .persons
                            .get(&Date::new(s))
                            .unwrap()
                            .get(&p)
                            .unwrap()
                            .1
                            + events
                                .auctions
                                .get(&Date::new(s))
                                .unwrap()
                                .get(&p)
                                .unwrap()
                                .1
                            + events.bids.get(&Date::new(s)).unwrap().get(&p).unwrap().1
                    })
                    .sum::<usize>()
            })
            .sum()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_gen_data() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10.to_string());
        config.insert("seconds", 1.to_string());
        config.insert("events-per-second", 10_000.to_string());
        let nex = NexMarkSource {
            config,
            ..Default::default()
        };
        let events = nex.generate_data()?;
        assert_eq!(events.persons.len(), 1);
        assert_eq!(events.auctions.len(), 1);
        assert_eq!(events.bids.len(), 1);
        assert_eq!(nex.count_events(&events), 10_000);

        let seconds = 10;
        let threads = 100;
        let event_per_second = 10_000;
        let nex = NexMarkSource::new(seconds, threads, event_per_second, StreamWindow::None);
        let events = nex.generate_data()?;
        assert_eq!(events.persons.len(), 10);
        assert_eq!(events.auctions.len(), 10);
        assert_eq!(events.bids.len(), 10);
        assert_eq!(nex.count_events(&events), 100_000);

        Ok(())
    }
}
