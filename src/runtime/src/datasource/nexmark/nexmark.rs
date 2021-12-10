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

//! Nexmark benchmark suite

use crate::config::FLOCK_CONF;
use arrow::datatypes::SchemaRef;
use arrow::json;
use arrow::record_batch::RecordBatch;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::datasource::nexmark::config::Config;
use crate::datasource::nexmark::event::Date;
use crate::datasource::nexmark::generator::NEXMarkGenerator;
use crate::error::Result;
use crate::query::StreamWindow;

type Epoch = Date;
type SourceId = usize;
type Partition = usize;
type NumEvents = usize;

/// A struct to temporarily store three types of events along with the
/// timelines.
#[derive(Debug, Default)]
pub struct NexMarkStream {
    /// The Person events in different epochs and partitions.
    pub persons:  HashMap<Epoch, HashMap<SourceId, (Vec<u8>, NumEvents)>>,
    /// The Auction events in different epochs and partitions.
    pub auctions: HashMap<Epoch, HashMap<SourceId, (Vec<u8>, NumEvents)>>,
    /// The Bid events in different epochs and partitions.
    pub bids:     HashMap<Epoch, HashMap<SourceId, (Vec<u8>, NumEvents)>>,
}

/// A struct to hold events for a given epoch and source identifier, which can
/// be serialized as cloud function's payload for transmission on cloud.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NexMarkEvent {
    /// The encoded Person events.
    pub persons:  Vec<u8>,
    /// The encoded Auction events.
    pub auctions: Vec<u8>,
    /// The encoded Bid events.
    pub bids:     Vec<u8>,
    /// The logical timestamp for the current epoch.
    pub epoch:    usize,
    /// The data source identifier.
    pub source:   usize,
}

impl NexMarkStream {
    /// Creates a new NexMarkStream.
    pub fn new() -> Self {
        NexMarkStream {
            persons:  HashMap::new(),
            auctions: HashMap::new(),
            bids:     HashMap::new(),
        }
    }

    /// Fetches all events belong to the given epoch and source identifier.
    pub fn select(&self, time: usize, source: usize) -> Option<NexMarkEvent> {
        let mut event = NexMarkEvent {
            epoch: time,
            source,
            ..Default::default()
        };
        let epoch = Epoch::new(time);

        if let Some(map) = self.persons.get(&epoch) {
            if let Some((persons, _)) = map.get(&source) {
                event.persons = persons.clone();
            }
        }

        if let Some(map) = self.auctions.get(&epoch) {
            if let Some((auctions, _)) = map.get(&source) {
                event.auctions = auctions.clone();
            }
        }

        if let Some(map) = self.bids.get(&epoch) {
            if let Some((bids, _)) = map.get(&source) {
                event.bids = bids.clone();
            }
        }

        if event.persons.is_empty() && event.auctions.is_empty() && event.bids.is_empty() {
            return None;
        }

        Some(event)
    }
}

/// A struct to generate events for Nexmark benchmarks.
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
        let window = StreamWindow::ElementWise;
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
        events: &mut NexMarkStream,
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
    pub fn generate_data(&self) -> Result<NexMarkStream> {
        let partitions: usize = self.config.get_as_or("threads", 100);
        let seconds: usize = self.config.get_as_or("seconds", 10);

        info!(
            "Generating events for {}s over {} partitions.",
            seconds, partitions
        );

        let generator = NEXMarkGenerator::new(&self.config);
        let events_handle = Arc::new(Mutex::new(NexMarkStream::new()));

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

    /// Converts NexMarkSource events to record batches in Arrow.
    pub fn to_batch(events: &[u8], schema: SchemaRef) -> Vec<RecordBatch> {
        let batch_size = FLOCK_CONF["lambda"]["granule"].parse::<usize>().unwrap();
        let mut reader = json::Reader::new(BufReader::new(events), schema, batch_size, None);

        let mut batches = vec![];
        while let Some(batch) = reader.next().unwrap() {
            batches.push(batch);
        }
        batches
    }

    /// Counts the number of events. (for testing)
    pub fn count_events(&self, events: &NexMarkStream) -> usize {
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
    use crate::datasource::nexmark::event::{Auction, Bid, Person};

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
        let nex = NexMarkSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::ElementWise,
        );
        let events = nex.generate_data()?;
        assert_eq!(events.persons.len(), 10);
        assert_eq!(events.auctions.len(), 10);
        assert_eq!(events.bids.len(), 10);
        assert_eq!(nex.count_events(&events), 100_000);

        Ok(())
    }

    #[test]
    fn test_nexmark_serialization() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10.to_string());
        config.insert("seconds", 1.to_string());
        config.insert("events-per-second", 100.to_string());
        let nex = NexMarkSource {
            config,
            ..Default::default()
        };
        let events = nex.generate_data()?;
        let events = events.select(0, 1).unwrap();

        // serialization and compression
        let values = serde_json::to_value(events).unwrap();

        // decompression and deserialization
        let de_events: NexMarkEvent = serde_json::from_value(values).unwrap();

        let person_schema = Arc::new(Person::schema());
        let batches = NexMarkSource::to_batch(&de_events.persons, person_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let auction_schema = Arc::new(Auction::schema());
        let batches = NexMarkSource::to_batch(&de_events.auctions, auction_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        let bid_schema = Arc::new(Bid::schema());
        let batches = NexMarkSource::to_batch(&de_events.bids, bid_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        Ok(())
    }
}
