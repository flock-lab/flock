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

/// A Fail state ("Type": "Fail") stops the execution of the state machine and
/// marks it as a failure.
#[allow(dead_code)]
pub struct Fail {
    pub name:    String,
    pub family:  String,
    pub comment: Option<String>,
    /// Provides a custom failure string that can be used for operational or
    /// diagnostic purposes.
    pub cause:   Option<String>,
    /// Provides an error name that can be used for error handling
    /// (Retry/Catch), operational, or diagnostic purposes.
    pub error:   Option<String>,
}