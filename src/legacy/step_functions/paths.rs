// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
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

//! Input and Output Processing in Step Functions
//!
//! A Step Functions execution receives a JSON text as input and passes that
//! input to the first state in the workflow. Individual states receive JSON as
//! input and usually pass JSON as output to the next state. Understanding how
//! this information flows from state to state, and learning how to filter and
//! manipulate this data, is key to effectively designing and implementing
//! workflows in AWS Step Functions.
//!
//! In the Amazon States Language, these fields filter and control the flow of
//! JSON from state to state:
//!
//! - InputPath
//! - OutputPath
//! - ResultPath
//! - Parameters
//! - ResultSelector
//!
//! For example, InputPath selects which parts of the JSON input to pass to the
//! task of the Task state (for example, an AWS Lambda function). ResultPath
//! then selects what combination of the state input and the task result to pass
//! to the output. OutputPath can filter the JSON output to further limit the
//! information that's passed to the output.

// FIXME: we don't have to filter and manipulate some fields in the input and
// output. Streaming data is of the same type, and no additional processing is
// required.
use json::JsonValue;

#[allow(dead_code)]
pub struct InputPath {
    input_path: JsonValue,
}

#[allow(dead_code)]
pub struct OutputPath {
    output_path: JsonValue,
}

#[allow(dead_code)]
pub struct ResultPath {
    result_path: JsonValue,
}

#[allow(dead_code)]
pub struct Parameters {
    parameters: JsonValue,
}

#[allow(dead_code)]
pub struct ResultSelector {
    result_selector: JsonValue,
}
