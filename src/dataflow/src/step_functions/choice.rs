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

use crate::common::Common;
use json::JsonValue;

#[allow(dead_code)]
pub struct Choice {
    /// Common state fields.
    pub common:  Common,
    /// The name of the state to transition to if none of the transitions in
    /// Choices is taken.
    pub default: Option<String>,
    /// An array of Choice Rules that determines which state the state machine
    /// transitions to next.
    pub choices: JsonValue,
}
