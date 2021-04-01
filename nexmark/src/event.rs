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

//! The NexMark events: `Person`, `Auction`, and `Bid`.

use crate::config::NEXMarkConfig;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};

const MIN_STRING_LENGTH: usize = 3;

trait NEXMarkRng {
    fn gen_string(&mut self, max: usize) -> String;
    fn gen_price(&mut self) -> usize;
}

impl NEXMarkRng for SmallRng {
    fn gen_string(&mut self, max: usize) -> String {
        let len = self.gen_range(MIN_STRING_LENGTH..max);
        String::from(
            (0..len)
                .map(|_| {
                    if self.gen_range(0..13) == 0 {
                        String::from(" ")
                    } else {
                        ::std::char::from_u32('a' as u32 + self.gen_range(0..26))
                            .unwrap()
                            .to_string()
                    }
                })
                .collect::<Vec<String>>()
                .join("")
                .trim(),
        )
    }

    fn gen_price(&mut self) -> usize {
        (10.0_f32.powf((*self).gen::<f32>() * 6.0) * 100.0).round() as usize
    }
}

type Id = usize;

/// The event's date time.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Clone,
    Serialize,
    Deserialize,
    Debug,
    Abomonation,
    Hash,
    Copy,
    Default,
)]
pub struct Date(usize);

impl Date {
    /// Creates a new date time.
    pub fn new(date_time: usize) -> Date {
        Date(date_time)
    }
}

impl ::std::ops::Deref for Date {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ::std::ops::Add for Date {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Date(self.0 + other.0)
    }
}

impl ::std::ops::Sub for Date {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Date(self.0 - other.0)
    }
}

/// The NexMark event with the date time.
#[derive(Serialize, Deserialize, Abomonation, Debug)]
pub struct EventCarrier {
    /// The date time.
    pub time:  Date,
    /// The NexMark event.
    pub event: Event,
}

/// The NexMark Event, including `Person`, `Auction`, and `Bid`.
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug, Abomonation)]
#[serde(tag = "type")]
pub enum Event {
    /// The Person event.
    Person(Person),
    /// The Auction event.
    Auction(Auction),
    /// The Bid event.
    Bid(Bid),
}

impl Event {
    /// Creates a new event randomly.
    pub fn new(events_so_far: usize, nex: &mut NEXMarkConfig) -> Self {
        let rem = nex.next_adjusted_event(events_so_far) % nex.proportion_denominator;
        let timestamp = Date(nex.event_timestamp(nex.next_adjusted_event(events_so_far)));
        let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);
        let mut rng = SmallRng::seed_from_u64(id as u64);
        if rem < nex.person_proportion {
            Event::Person(Person::new(id, timestamp, &mut rng, nex))
        } else if rem < nex.person_proportion + nex.auction_proportion {
            Event::Auction(Auction::new(events_so_far, id, timestamp, &mut rng, nex))
        } else {
            Event::Bid(Bid::new(id, timestamp, &mut rng, nex))
        }
    }
}

/// Person represents a person submitting an item for auction and/or making a
/// bid on an auction.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash,
)]
pub struct Person {
    /// A person-unique integer ID.
    pub id:            Id,
    /// A string for the person’s full name.
    pub name:          String,
    /// The person’s email address as a string.
    pub email_address: String,
    /// The credit card number as a 19-letter string.
    pub credit_card:   String,
    /// One of several US city names as a string.
    pub city:          String,
    /// One of several US states as a two-letter string.
    pub state:         String,
    /// A millisecond timestamp for the event origin.
    pub date_time:     Date,
}

impl Person {
    /// Parses the `Person` event.
    pub fn from(event: Event) -> Option<Person> {
        match event {
            Event::Person(p) => Some(p),
            _ => None,
        }
    }

    /// Creates a new `Person` event.
    fn new(id: usize, time: Date, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Self {
        Person {
            id:            Self::last_id(id, nex) + nex.first_person_id,
            name:          format!(
                "{} {}",
                nex.first_names.choose(rng).unwrap(),
                nex.last_names.choose(rng).unwrap(),
            ),
            email_address: format!("{}@{}.com", rng.gen_string(7), rng.gen_string(5)),
            credit_card:   (0..4)
                .map(|_| format!("{:04}", rng.gen_range(0..10000)))
                .collect::<Vec<String>>()
                .join(" "),
            city:          nex.us_cities.choose(rng).unwrap().clone(),
            state:         nex.us_states.choose(rng).unwrap().clone(),
            date_time:     time,
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Id {
        let people = Self::last_id(id, nex) + 1;
        let active = min(people, nex.active_people);
        people - active + rng.gen_range(0..active + nex.person_id_lead)
    }

    fn last_id(id: usize, nex: &NEXMarkConfig) -> Id {
        let epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if nex.person_proportion <= offset {
            offset = nex.person_proportion - 1;
        }
        epoch * nex.person_proportion + offset
    }
}

/// Auction represents an item under auction.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash,
)]
pub struct Auction {
    /// An auction-unique integer ID.
    pub id:          Id,
    /// The name of the item being auctioned.
    pub item_name:   String,
    /// A short description of the item.
    pub description: String,
    /// The initial bid price in cents.
    pub initial_bid: usize,
    /// The minimum price for the auction to succeed.
    pub reserve:     usize,
    /// A millisecond timestamp for the event origin.
    pub date_time:   Date,
    /// A UNIX epoch timestamp for the expiration date of the auction.
    pub expires:     Date,
    /// The ID of the person that created this auction.
    pub seller:      Id,
    /// The ID of the category this auction belongs to.
    pub category:    Id,
}

impl Auction {
    /// Parses a `Auction` event.
    pub fn from(event: Event) -> Option<Auction> {
        match event {
            Event::Auction(p) => Some(p),
            _ => None,
        }
    }

    fn new(
        events_so_far: usize,
        id: usize,
        time: Date,
        rng: &mut SmallRng,
        nex: &NEXMarkConfig,
    ) -> Self {
        let initial_bid = rng.gen_price();
        let seller = if rng.gen_range(0..nex.hot_seller_ratio) > 0 {
            (Person::last_id(id, nex) / nex.hot_seller_ratio_2) * nex.hot_seller_ratio_2
        } else {
            Person::next_id(id, rng, nex)
        };
        Auction {
            id: Self::last_id(id, nex) + nex.first_auction_id,
            item_name: rng.gen_string(20),
            description: rng.gen_string(100),
            initial_bid,
            reserve: initial_bid + rng.gen_price(),
            date_time: time,
            expires: time + Self::next_length(events_so_far, rng, time, nex),
            seller: seller + nex.first_person_id,
            category: nex.first_category_id + rng.gen_range(0..nex.num_categories),
        }
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Id {
        let max_auction = Self::last_id(id, nex);
        let min_auction = if max_auction < nex.in_flight_auctions {
            0
        } else {
            max_auction - nex.in_flight_auctions
        };
        min_auction + rng.gen_range(0..max_auction - min_auction + 1 + nex.auction_id_lead)
    }

    fn last_id(id: usize, nex: &NEXMarkConfig) -> Id {
        let mut epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if offset < nex.person_proportion {
            epoch -= 1;
            offset = nex.auction_proportion - 1;
        } else if nex.person_proportion + nex.auction_proportion <= offset {
            offset = nex.auction_proportion - 1;
        } else {
            offset -= nex.person_proportion;
        }
        epoch * nex.auction_proportion + offset
    }

    fn next_length(
        events_so_far: usize,
        rng: &mut SmallRng,
        time: Date,
        nex: &NEXMarkConfig,
    ) -> Date {
        let current_event = nex.next_adjusted_event(events_so_far);
        let events_for_auctions =
            (nex.in_flight_auctions * nex.proportion_denominator) / nex.auction_proportion;
        let future_auction = nex.event_timestamp(current_event + events_for_auctions);

        let horizon = future_auction - time.0;
        Date(1 + rng.gen_range(0..max(horizon * 2, 1)))
    }
}

/// Bid represents a bid for an item under auction.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Abomonation, Hash,
)]
pub struct Bid {
    /// The ID of the auction this bid is for.
    pub auction:   Id,
    /// The ID of the person that placed this bid.
    pub bidder:    Id,
    /// The price in cents that the person bid for.
    pub price:     usize,
    /// A millisecond timestamp for the event origin.
    pub date_time: Date,
}

impl Bid {
    /// Parses a `Bid` event.
    pub fn from(event: Event) -> Option<Bid> {
        match event {
            Event::Bid(p) => Some(p),
            _ => None,
        }
    }

    fn new(id: usize, time: Date, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Self {
        let auction = if 0 < rng.gen_range(0..nex.hot_auction_ratio) {
            (Auction::last_id(id, nex) / nex.hot_auction_ratio_2) * nex.hot_auction_ratio_2
        } else {
            Auction::next_id(id, rng, nex)
        };
        let bidder = if 0 < rng.gen_range(0..nex.hot_bidder_ratio) {
            (Person::last_id(id, nex) / nex.hot_bidder_ratio_2) * nex.hot_bidder_ratio_2 + 1
        } else {
            Person::next_id(id, rng, nex)
        };
        Bid {
            auction:   auction + nex.first_auction_id,
            bidder:    bidder + nex.first_person_id,
            price:     rng.gen_price(),
            date_time: time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn test_nexmark_rng() {
        let mut rng = SmallRng::seed_from_u64(1u64);
        let prices_1 = (0..100).map(|_| rng.gen_price()).collect::<Vec<_>>();
        let prices_2 = (0..100).map(|_| rng.gen_price()).collect::<Vec<_>>();
        assert_ne!(prices_1, prices_2);

        let strings_1 = (0..100).map(|_| rng.gen_string(10)).collect::<Vec<_>>();
        let strings_2 = (0..100).map(|_| rng.gen_string(10)).collect::<Vec<_>>();
        assert_ne!(strings_1, strings_2);
    }

    #[test]
    fn test_date_time() {
        let date_1 = Date::new(1);
        let date_2 = Date::new(2);

        assert_eq!(*date_1, 1);
        assert_eq!(*date_2, 2);
        assert_eq!(*(date_1 + date_2), 3);
        assert_eq!(*(date_2 - date_1), 1);
    }

    #[test]
    fn test_events() {
        let mut config = Config::new();
        config.insert("person-proportion", "30".to_string());
        config.insert("auction-proportion", "30".to_string());
        config.insert("bid-proportion", "40".to_string());

        let mut nex = NEXMarkConfig::new(&config);
        (0..100).for_each(|i| {
            Event::new(i, &mut nex);
        });
    }
}
