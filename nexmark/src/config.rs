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

//! The NexMark Benchmark Configuration.

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::str::FromStr;

/// This is a simple command line options parser.
#[derive(Clone, Default, Debug)]
pub struct Config {
    args: HashMap<String, String>,
}

impl Config {
    /// Creates a new `Config`.
    pub fn new() -> Self {
        Config {
            args: HashMap::new(),
        }
    }

    /// Parses the command line arguments into a new Config object.
    ///
    /// Its parsing strategy is as follows:
    ///   If an argument starts with --, the remaining string is used as the key
    ///   and the next argument as the associated value.
    ///   Otherwise the argument is used as the next positional value, counting
    ///   from zero.
    pub fn from<I: Iterator<Item = String>>(mut cmd_args: I) -> Result<Self> {
        let mut args = HashMap::new();
        let mut i = 0;
        while let Some(arg) = cmd_args.next() {
            if let Some(key) = arg.strip_prefix("--") {
                match cmd_args.next() {
                    Some(value) => args.insert(key.to_string(), value),
                    None => return Err(Error::new(ErrorKind::Other, "No corresponding value.")),
                };
            } else {
                args.insert(format!("{}", i), arg);
                i += 1;
            }
        }
        Ok(Config { args })
    }

    /// Inserts the given value for the given key.
    ///
    /// If the key already exists, its value is overwritten.
    pub fn insert(&mut self, key: &str, value: String) {
        self.args.insert(String::from(key), value);
    }

    /// Returns the value for the given key, if available.
    pub fn get(&self, key: &str) -> Option<String> {
        self.args.get(key).cloned()
    }

    /// Returns the value for the given key automatically parsed if possible.
    pub fn get_as<T: FromStr>(&self, key: &str) -> Option<T> {
        self.args.get(key).and_then(|x| x.parse::<T>().ok())
    }

    /// Returns the value for the given key or a default value if the key does
    /// not exist.
    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.args
            .get(key)
            .map_or(String::from(default), |x| x.clone())
    }

    /// Returns the value for the given key automatically parsed, or a default
    /// value if the key does not exist.
    pub fn get_as_or<T: FromStr>(&self, key: &str, default: T) -> T {
        self.get_as(key).unwrap_or(default)
    }
}

use std::f64::consts::PI;

const BASE_TIME: usize = 1436918400_000;

fn split_string_arg(string: String) -> Vec<String> {
    string.split(',').map(String::from).collect::<Vec<String>>()
}

#[derive(PartialEq)]
enum RateShape {
    Square,
    Sine,
}

/// Nexmark Configuration
#[derive(Clone)]
pub struct NEXMarkConfig {
    /// Maximum number of people to consider as active for placing auctions or
    /// bids.
    pub active_people:           usize,
    /// Average number of auction which should be inflight at any time, per
    /// generator.
    pub in_flight_auctions:      usize,
    /// Number of events in out-of-order groups.
    /// 1 implies no out-of-order events. 1000 implies every 1000 events per
    /// generator are emitted in pseudo-random order.
    pub out_of_order_group_size: usize,
    /// Ratio of auctions for 'hot' sellers compared to all other people.
    pub hot_seller_ratio:        usize,
    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    pub hot_auction_ratio:       usize,
    /// Ratio of bids for 'hot' bidders compared to all other people.
    pub hot_bidder_ratio:        usize,
    /// Event id of first event to be generated.
    /// Event ids are unique over all generators, and are used as a seed to
    /// generate each event's data.
    pub first_event_id:          usize,
    /// First event number.
    /// Generators running in parallel time may share the same event number, and
    /// the event number is used to determine the event timestamp.
    pub first_event_number:      usize,
    /// Time for first event (ms since epoch).
    pub base_time:               usize,
    /// Delay before changing the current inter-event delay.
    pub step_length:             usize,
    /// Number of events per epoch.
    /// Derived from above. (Ie number of events to run through cycle for all
    /// interEventDelayUs entries).
    pub events_per_epoch:        usize,
    /// True period of epoch in milliseconds. Derived from above. (Ie time to
    /// run through cycle for all interEventDelayUs entries).
    pub epoch_period:            f32,
    /// Delay between events, in microseconds.
    /// If the array has more than one entry then the rate is changed every
    /// step_length, and wraps around.
    pub inter_event_delays:      Vec<f32>,
    // Originally constants
    /// Auction categories.
    pub num_categories:          usize,
    /// Use to calculate the next auction id.
    pub auction_id_lead:         usize,
    /// Ratio of auctions for 'hot' sellers compared to all other people.
    pub hot_seller_ratio_2:      usize,
    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    pub hot_auction_ratio_2:     usize,
    /// Ratio of bids for 'hot' bidders compared to all other people.
    pub hot_bidder_ratio_2:      usize,
    /// Person Proportion.
    pub person_proportion:       usize,
    /// Auction Proportion.
    pub auction_proportion:      usize,
    /// Bid Proportion.
    pub bid_proportion:          usize,
    /// Proportion Denominator.
    pub proportion_denominator:  usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_auction_id:        usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_person_id:         usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_category_id:       usize,
    /// Use to calculate the next id.
    pub person_id_lead:          usize,
    /// Use to calculate inter_event_delays for rate-shape sine.
    pub sine_approx_steps:       usize,
    /// The collection of U.S. statees
    pub us_states:               Vec<String>,
    /// The collection of U.S. cities.
    pub us_cities:               Vec<String>,
    /// The collection of first names.
    pub first_names:             Vec<String>,
    /// The collection of last names.
    pub last_names:              Vec<String>,
}

impl NEXMarkConfig {
    /// Creates the NexMark configuration.
    pub fn new(config: &Config) -> Self {
        let active_people = config.get_as_or("active-people", 1000);
        let in_flight_auctions = config.get_as_or("in-flight-auctions", 100);
        let out_of_order_group_size = config.get_as_or("out-of-order-group-size", 1);
        let hot_seller_ratio = config.get_as_or("hot-seller-ratio", 4);
        let hot_auction_ratio = config.get_as_or("hot-auction-ratio", 2);
        let hot_bidder_ratio = config.get_as_or("hot-bidder-ratio", 4);
        let first_event_id = config.get_as_or("first-event-id", 0);
        let first_event_number = config.get_as_or("first-event-number", 0);
        let num_categories = config.get_as_or("num-categories", 5);
        let auction_id_lead = config.get_as_or("auction-id-lead", 10);
        let hot_seller_ratio_2 = config.get_as_or("hot-seller-ratio-2", 100);
        let hot_auction_ratio_2 = config.get_as_or("hot-auction-ratio-2", 100);
        let hot_bidder_ratio_2 = config.get_as_or("hot-bidder-ratio-2", 100);
        let person_proportion = config.get_as_or("person-proportion", 1);
        let auction_proportion = config.get_as_or("auction-proportion", 3);
        let bid_proportion = config.get_as_or("bid-proportion", 46);
        let proportion_denominator = person_proportion + auction_proportion + bid_proportion;
        let first_auction_id = config.get_as_or("first-auction-id", 1000);
        let first_person_id = config.get_as_or("first-person-id", 1000);
        let first_category_id = config.get_as_or("first-category-id", 10);
        let person_id_lead = config.get_as_or("person-id-lead", 10);
        let sine_approx_steps = config.get_as_or("sine-approx-steps", 10);
        let base_time = config.get_as_or("base-time", BASE_TIME);
        let us_states = split_string_arg(config.get_or("us-states", "az,ca,id,or,wa,wy"));
        let us_cities = split_string_arg(config.get_or(
            "us-cities",
            "phoenix,los angeles,san francisco,boise,portland,bend,redmond,seattle,kent,cheyenne",
        ));
        let first_names = split_string_arg(config.get_or(
            "first-names",
            "peter,paul,luke,john,saul,vicky,kate,julie,sarah,deiter,walter",
        ));
        let last_names = split_string_arg(config.get_or(
            "last-names",
            "shultz,abrams,spencer,white,bartels,walton,smith,jones,noris",
        ));
        let rate_shape = if config.get_or("rate-shape", "sine") == "sine" {
            RateShape::Sine
        } else {
            RateShape::Square
        };
        let rate_period = config.get_as_or("rate-period", 600);
        let first_rate = config.get_as_or(
            "first-event-rate",
            config.get_as_or("events-per-second", 10_000),
        );
        let next_rate = config.get_as_or("next-event-rate", first_rate);
        let us_per_unit = config.get_as_or("us-per-unit", 1_000_000); // Rate is in μs
        let generators = config.get_as_or("threads", 1) as f32;
        // Calculate inter event delays array.
        let mut inter_event_delays = Vec::new();
        let rate_to_period = |r| (us_per_unit) as f32 / r as f32;
        if first_rate == next_rate {
            inter_event_delays.push(rate_to_period(first_rate) * generators);
        } else {
            match rate_shape {
                RateShape::Square => {
                    inter_event_delays.push(rate_to_period(first_rate) * generators);
                    inter_event_delays.push(rate_to_period(next_rate) * generators);
                }
                RateShape::Sine => {
                    let mid = (first_rate + next_rate) as f64 / 2.0;
                    let amp = (first_rate - next_rate) as f64 / 2.0;
                    for i in 0..sine_approx_steps {
                        let r = (2.0 * PI * i as f64) / sine_approx_steps as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays.push(rate_to_period(rate.round() as usize) * generators);
                    }
                }
            }
        }
        // Calculate events per epoch and epoch period.
        let n = if rate_shape == RateShape::Square {
            2
        } else {
            sine_approx_steps
        };
        let step_length = (rate_period + n - 1) / n;
        let mut events_per_epoch = 0;
        let mut epoch_period = 0.0;
        if inter_event_delays.len() > 1 {
            for inter_event_delay in &inter_event_delays {
                let num_events_for_this_cycle =
                    (step_length * 1_000_000) as f32 / inter_event_delay;
                events_per_epoch += num_events_for_this_cycle.round() as usize;
                epoch_period += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
            }
        }
        NEXMarkConfig {
            active_people,
            in_flight_auctions,
            out_of_order_group_size,
            hot_seller_ratio,
            hot_auction_ratio,
            hot_bidder_ratio,
            first_event_id,
            first_event_number,
            base_time,
            step_length,
            events_per_epoch,
            epoch_period,
            inter_event_delays,
            // Originally constants
            num_categories,
            auction_id_lead,
            hot_seller_ratio_2,
            hot_auction_ratio_2,
            hot_bidder_ratio_2,
            person_proportion,
            auction_proportion,
            bid_proportion,
            proportion_denominator,
            first_auction_id,
            first_person_id,
            first_category_id,
            person_id_lead,
            sine_approx_steps,
            us_states,
            us_cities,
            first_names,
            last_names,
        }
    }

    /// Returns a new event timestamp.
    pub fn event_timestamp(&self, event_number: usize) -> usize {
        if self.inter_event_delays.len() == 1 {
            return self.base_time
                + ((event_number as f32 * self.inter_event_delays[0]) / 1000.0).round() as usize;
        }

        let epoch = event_number / self.events_per_epoch;
        let mut event_i = event_number % self.events_per_epoch;
        let mut offset_in_epoch = 0.0;
        for inter_event_delay in &self.inter_event_delays {
            let num_events_for_this_cycle =
                (self.step_length * 1_000_000) as f32 / inter_event_delay;
            if self.out_of_order_group_size < num_events_for_this_cycle.round() as usize {
                let offset_in_cycle = event_i as f32 * inter_event_delay;
                return self.base_time
                    + (epoch as f32 * self.epoch_period
                        + offset_in_epoch
                        + offset_in_cycle / 1000.0)
                        .round() as usize;
            }
            event_i -= num_events_for_this_cycle.round() as usize;
            offset_in_epoch += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
        }
        return 0;
    }

    /// Returns the next adjusted event.
    pub fn next_adjusted_event(&self, events_so_far: usize) -> usize {
        let n = self.out_of_order_group_size;
        let event_number = self.first_event_number + events_so_far;
        (event_number / n) * n + (event_number * 953) % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() -> Result<()> {
        let config1 = Config::new();
        assert_eq!(config1.get("hello"), None);

        let mut config2 = Config::from(
            vec!["--hello", "world", "--db", "424", "layoff"]
                .iter()
                .map(ToString::to_string),
        )?;
        config2.insert("net", "417".to_string());

        assert_eq!(config2.get_or("0", "-1"), "layoff");
        assert_eq!(config2.get_or("hello", "-1"), "world");
        assert_eq!(config2.get_as_or("db", 424), 424);
        assert_eq!(config2.get_as_or("net", 417), 417);

        Ok(())
    }

    #[test]
    fn test_nexmark_config() {
        let mut config = Config::new();

        config.insert("active_people", "1024".to_string());
        let mut nexmark_cfg = NEXMarkConfig::new(&config);
        nexmark_cfg.event_timestamp(2048);
        nexmark_cfg.next_adjusted_event(100000);

        config.insert("rate-shape", "sine".to_string());
        config.insert("next-event-rate", "512".to_string());
        nexmark_cfg = NEXMarkConfig::new(&config);
        nexmark_cfg.event_timestamp(2048);
        nexmark_cfg.next_adjusted_event(100000);

        config.insert("rate-shape", "square".to_string());
        nexmark_cfg = NEXMarkConfig::new(&config);
        nexmark_cfg.event_timestamp(2048);
        nexmark_cfg.next_adjusted_event(100000);

        config.insert("threads", "8".to_string());
        nexmark_cfg = NEXMarkConfig::new(&config);
        nexmark_cfg.event_timestamp(2048);
        nexmark_cfg.next_adjusted_event(100000);
    }
}
