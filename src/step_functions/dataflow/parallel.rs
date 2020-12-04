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

#[path = "./graph.rs"]
mod graph;
use graph::Dataflow;

#[path = "./paths.rs"]
mod paths;
use paths::{ResultPath, ResultSelector};

/// The Parallel state ("Type": "Parallel") can be used to create parallel
/// branches of execution in your state machine.
///
/// A Parallel state causes AWS Step Functions to execute each branch, starting
/// with the state named in that branch's StartAt field, as concurrently as
/// possible, and wait until all branches terminate (reach a terminal state)
/// before processing the Parallel state's Next field.
#[allow(dead_code)]
pub struct Parallel {
    /// Common state fields.
    pub common:          Common,
    /// Specifies where (in the input) to place the results of executing the
    /// task that's specified in Resource. The input is then filtered as
    /// specified by the OutputPath field (if present) before being used as the
    /// state's output.
    pub result_path:     Option<ResultPath>,
    /// Use the `ResultSelector` field to manipulate a state's result before
    /// `ResultPath` is applied. The `ResultSelector` field lets you create a
    /// collection of key value pairs, where the values are static or selected
    /// from the state's result. The output of ResultSelector replaces the
    /// state's result and is passed to ResultPath.
    pub result_selector: Option<ResultSelector>,
    /// An array of objects that specify state machines to execute in parallel.
    /// Each such state machine object must have fields named States and
    /// StartAt, whose meanings are exactly like those in the top level of a
    /// state machine.
    pub branches:        Vec<Dataflow>,
}
