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

//! YSB Ad Event.

use crate::datasource::epoch::Epoch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Advertising event types.
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct AdEvent {
    /// A UUID identifying the user that caused the event.
    pub user_id:    String,
    /// A UUID identifying the page on which the event occurred.
    pub page_id:    String,
    /// A UUID for the specific advertisement that was interacted with.
    pub ad_id:      String,
    /// A string, one of "banner", "modal", "sponsored-search", "mail", and
    /// "mobile".
    pub ad_type:    String,
    /// A string, one of "view", "click", and "purchase".
    pub event_type: String,
    /// An integer timestamp in milliseconds of the time the event occurred.
    pub event_time: Epoch,
    /// A string of the user's IP address.
    pub ip_address: String,
}

impl AdEvent {
    /// Returns the schema of the event.
    pub fn schema() -> Schema {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "ysb_ad_events".to_string());
        Schema::new_with_metadata(
            vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("page_id", DataType::Utf8, false),
                Field::new("ad_id", DataType::Utf8, false),
                Field::new("ad_type", DataType::Utf8, false),
                Field::new("event_type", DataType::Utf8, false),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("ip_address", DataType::Utf8, false),
            ],
            metadata,
        )
    }
}

/// Campaigns map from ad_id to campaign_id.
#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct Campaign {
    /// A UUID for the specific advertisement that was interacted with.
    pub c_ad_id:     String,
    /// A UUID for the campaign that the ad belongs to.
    pub campaign_id: String,
}

impl Campaign {
    /// Returns the schema of the event.
    pub fn schema() -> Schema {
        let mut metadata = HashMap::new();
        metadata.insert("name".to_string(), "ysb_campaigns".to_string());
        Schema::new_with_metadata(
            vec![
                Field::new("c_ad_id", DataType::Utf8, false),
                Field::new("campaign_id", DataType::Utf8, false),
            ],
            metadata,
        )
    }
}
