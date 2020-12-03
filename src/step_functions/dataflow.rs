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

//! You create a workflow that runs a group of Lambda functions (steps) in a
//! specific order. One Lambda function's output passes to the next Lambda
//! function's input. The last step in your workflow gives a result. With Step
//! Functions, you can see how each step in your workflow interacts with one
//! other, so you can make sure that each step performs its intended function.

#[path = "states/mod.rs"]
mod states;
use states::State;

use std::collections::HashMap;

/// A State Machine is represented by a JSON Object.
///
/// The operation of a state machine is specified by states, which are
/// represented by JSON objects, fields in the top-level "States" object.
#[allow(dead_code)]
pub struct Dataflow {
    /// A State Machine MAY have a string field named "Comment", provided for
    /// human-readable description of the machine.
    pub comment:         Option<String>,
    /// A State Machine MAY have a string field named "Version", which gives the
    /// version of the States language used in the machine. This document
    /// describes version 1.0, and if omitted, the default value of "Version" is
    /// the string "1.0".
    pub version:         Option<String>,
    /// A State Machine MUST have a string field named "StartAt", whose value
    /// MUST exactly match one of names of the "States" fields. The interpreter
    /// starts running the the machine at the named state.
    pub start_at:        String,
    /// A State Machine MUST have an object field named "States", whose fields
    /// represent the states.
    pub states:          HashMap<String, State>,
    /// A State Machine MAY have an integer field named "TimeoutSeconds". If
    /// provided, it provides the maximum number of seconds the machine is
    /// allowed to run. If the machine runs longer than the specified time, then
    /// the interpreter fails the machine with a States.Timeout Error Name.
    pub timeout_seconds: u32,
}
