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

//! Yahoo Streaming Benchmark Suite.

use crate::config::FLOCK_CONF;
use crate::datasource::ysb::event::Campaign;
use arrow::datatypes::SchemaRef;
use arrow::json;
use arrow::record_batch::RecordBatch;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::datasource::config::Config;
use crate::datasource::date::DateTime;
use crate::datasource::ysb::generator::YSBGenerator;
use crate::error::Result;
use crate::query::{Schedule, StreamWindow};

type Epoch = DateTime;
type SourceId = usize;
type Partition = usize;
type NumEvents = usize;

/// A struct to temporarily hold events along with the timelines.
#[derive(Debug, Default)]
pub struct YSBStream {
    /// The events in different epochs and partitions.
    pub events:    HashMap<Epoch, HashMap<SourceId, (Vec<u8>, NumEvents)>>,
    /// The map from ad_id to campaign_id.
    pub campaigns: (Vec<u8>, usize),
}

/// A struct to hold events for a given epoch and source identifier, which can
/// be serialized as cloud function's payload for transmission on cloud.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct YSBEvent {
    /// The encoded events.
    pub ad_events: Vec<u8>,
    /// The logical timestamp for the current epoch.
    pub epoch:     usize,
    /// The data source identifier.
    pub source:    usize,
}

impl YSBStream {
    /// Creates a new YSBStream.
    pub fn new() -> Self {
        YSBStream {
            events:    HashMap::new(),
            campaigns: (Vec::new(), 0),
        }
    }

    /// Fetches all events belong to the given epoch and source identifier.
    pub fn select(&self, time: usize, source: usize) -> Option<(YSBEvent, usize)> {
        let mut event = YSBEvent {
            epoch: time,
            source,
            ..Default::default()
        };
        let epoch = Epoch::new(time);

        let mut ad_events_num = 0;
        if let Some(map) = self.events.get(&epoch) {
            if let Some((ad_events, num)) = map.get(&source) {
                event.ad_events = ad_events.clone();
                ad_events_num = *num;
            }
        }

        if event.ad_events.is_empty() {
            return None;
        }

        Some((event, ad_events_num))
    }
}

/// A struct to generate events for YSB benchmarks.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct YSBSource {
    /// The YSB configuration.
    pub config: Config,
    /// The windows group stream elements by time or rows.
    pub window: StreamWindow,
}

impl Default for YSBSource {
    fn default() -> Self {
        let mut config = Config::new();
        config.insert("threads", 16.to_string());
        config.insert("seconds", 10.to_string());
        config.insert("events-per-second", 1000.to_string());
        let window = StreamWindow::TumblingWindow(Schedule::Seconds(10));
        YSBSource { config, window }
    }
}

impl YSBSource {
    /// Creates a new YSB benchmark data source.
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
        YSBSource { config, window }
    }

    /// Assigns each event with the specific type for the upcoming processing.
    fn assgin_events(stream: &mut YSBStream, t: Epoch, p: Partition, event: (Vec<u8>, usize)) {
        match stream.events.get_mut(&t) {
            Some(m) => {
                (*m).insert(p, event);
            }
            None => {
                let mut m = HashMap::new();
                m.insert(p, event);
                stream.events.insert(t, m);
            }
        }
    }

    /// Generates data events for YSB benchmark.
    pub fn generate_data(&self) -> Result<YSBStream> {
        let partitions: usize = self.config.get_as_or("threads", 16);
        let seconds: usize = self.config.get_as_or("seconds", 10);

        info!(
            "Generating events for {}s over {} partitions.",
            seconds, partitions
        );

        let generator = YSBGenerator::new(&self.config);
        let events_handle = Arc::new(Mutex::new(YSBStream::new()));

        let mut threads = vec![];
        for p in 0..partitions {
            let mut generator = generator.clone();
            let events_handle = Arc::clone(&events_handle);
            threads.push(thread::spawn(move || loop {
                let (t, d) = generator.next_epoch().unwrap();
                if !d.0.is_empty() {
                    let mut events = events_handle.lock().unwrap();
                    YSBSource::assgin_events(&mut events, t, p, d);
                } else {
                    break;
                }
            }));
        }
        for t in threads.drain(..) {
            t.join().unwrap();
        }

        let mut stream = events_handle.lock().unwrap();
        let size = generator.map.len();
        let mut campaigns = vec![];
        generator
            .map
            .into_iter()
            .for_each(|(c_ad_id, campaign_id)| {
                campaigns.extend(
                    serde_json::to_vec(&Campaign {
                        c_ad_id:     c_ad_id,
                        campaign_id: campaign_id,
                    })
                    .unwrap(),
                );
                campaigns.extend(vec![10]);
            });

        stream.campaigns = (campaigns, size);

        Ok(std::mem::take(&mut stream))
    }

    /// Converts YSBSource events to record batches in Arrow.
    pub fn to_batch(events: &[u8], schema: SchemaRef) -> Vec<RecordBatch> {
        let batch_size = FLOCK_CONF["lambda"]["granule"].parse::<usize>().unwrap();
        let mut reader = json::Reader::new(BufReader::new(events), schema, batch_size, None);

        let mut batches = vec![];
        while let Some(batch) = reader.next().unwrap() {
            batches.push(batch);
        }
        batches
    }

    /// Converts YSBSource events to record batches in Arrow.
    pub fn to_batch_v2(events: &[u8], schema: SchemaRef, batch_size: usize) -> Vec<RecordBatch> {
        let mut reader = json::Reader::new(BufReader::new(events), schema, batch_size, None);
        let mut batches = vec![];
        while let Some(batch) = reader.next().unwrap() {
            batches.push(batch);
        }
        batches
    }

    /// Counts the number of events. (for testing)
    pub fn count_events(&self, stream: &YSBStream) -> usize {
        let threads: usize = self.config.get_as_or("threads", 16);
        let seconds: usize = self.config.get_as_or("seconds", 10);
        (0..threads)
            .map(|p| {
                (0..seconds)
                    .map(|s| {
                        stream
                            .events
                            .get(&DateTime::new(s))
                            .unwrap()
                            .get(&p)
                            .unwrap()
                            .1
                    })
                    .sum::<usize>()
            })
            .sum()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::datasource::ysb::event::AdEvent;

    #[test]
    fn test_gen_ysb_data() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10.to_string());
        config.insert("seconds", 1.to_string());
        config.insert("events-per-second", 10_000.to_string());
        let ysb = YSBSource {
            config,
            ..Default::default()
        };
        let stream = ysb.generate_data()?;
        assert_eq!(stream.events.len(), 1);
        assert!((10_000 - ysb.count_events(&stream)) < 100);

        let seconds = 10;
        let threads = 100;
        let event_per_second = 10_000;
        let ysb = YSBSource::new(
            seconds,
            threads,
            event_per_second,
            StreamWindow::ElementWise,
        );
        let stream = ysb.generate_data()?;
        assert_eq!(stream.events.len(), 10);
        assert!((100_000 - ysb.count_events(&stream)) < 100);

        Ok(())
    }

    #[test]
    fn test_ysb_serialization() -> Result<()> {
        let mut config = Config::new();
        config.insert("threads", 10.to_string());
        config.insert("seconds", 1.to_string());
        config.insert("events-per-second", 100.to_string());
        let ysb = YSBSource {
            config,
            ..Default::default()
        };
        let stream = ysb.generate_data()?;
        let (events, _) = stream.select(0, 1).unwrap();

        // serialization and compression
        let values = serde_json::to_value(events).unwrap();

        // decompression and deserialization
        let de_events: YSBEvent = serde_json::from_value(values).unwrap();

        let ad_event_schema = Arc::new(AdEvent::schema());
        let batches = YSBSource::to_batch(&de_events.ad_events, ad_event_schema);
        let formatted = arrow::util::pretty::pretty_format_batches(&batches).unwrap();
        println!("{}", formatted);

        Ok(())
    }
}
