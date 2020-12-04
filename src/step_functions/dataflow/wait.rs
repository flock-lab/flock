// Copyright (c) 2020 UMD Database Group. All rights reserved.
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

#[path = "./common.rs"]
mod common;
use common::Common;

/// A Wait state ("Type": "Wait") delays the state machine from continuing for a
/// specified time. You can choose either a relative time, specified in seconds
/// from when the state begins, or an absolute end time, specified as a
/// timestamp.
#[allow(dead_code)]
pub struct Wait {
    /// Common state fields.
    pub common:         Common,
    /// A time, in seconds, to wait before beginning the state specified in the
    /// Next field.
    pub seconds:        Option<String>,
    /// An absolute time to wait until beginning the state specified in the Next
    /// field. Timestamps must conform to the RFC3339 profile of ISO 8601,
    /// with the further restrictions that an uppercase T must separate the date
    /// and time portions, and an uppercase Z must denote that a numeric time
    /// zone offset is not present, for example, 2016-08-18T17:33:00Z.
    pub timestamp:      Option<String>,
    /// A time, in seconds, to wait before beginning the state specified in the
    /// Next field, specified using a path from the state's input data.
    pub seconds_path:   Option<String>,
    /// An absolute time to wait until beginning the state specified in the Next
    /// field, specified using a path from the state's input data.
    pub timestamp_path: Option<String>,
}
