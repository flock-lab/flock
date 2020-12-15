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
use crate::paths::{Parameters, ResultPath};
use json::JsonValue;

#[allow(dead_code)]
pub struct Pass {
    /// Common state fields.
    pub common:      Common,
    /// Treated as the output of a virtual task to be passed to the next state,
    /// and filtered as specified by the ResultPath field (if present).
    pub result:      Option<JsonValue>,
    /// Specifies where (in the input) to place the "output" of the virtual task
    /// specified in Result. The input is further filtered as specified by the
    /// OutputPath field (if present) before being used as the state's output.
    pub result_path: Option<ResultPath>,
    /// Create a collection of key-value pairs that will be passed as input.
    /// Values can be static, or selected from the input with a path.
    pub parameters:  Option<Parameters>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_json_value() {
        let mut data = json::JsonValue::new_object();
        data["answer"] = 42.into();
        data["foo"] = "bar".into();
        assert_eq!(data.dump(), r#"{"answer":42,"foo":"bar"}"#);
    }
}
