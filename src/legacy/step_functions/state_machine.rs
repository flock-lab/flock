// Copyright 2020 UMD Database Group. All Rights Reserved.
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

use std::collections::HashMap;

/// States are elements in your state machine. A state is referred to by its
/// name, which can be any string, but which must be unique within the scope of
/// the entire state machine.
///
/// States can perform a variety of functions in your state machine:
///
/// - Do some work in your state machine (a Task state)
/// - Make a choice between branches of execution (a Choice state)
/// - Stop an execution with a failure or success (a Fail or Succeed state)
/// - Simply pass input to its output or inject some fixed data (a Pass state)
/// - Provide a delay for a certain amount of time or until a specified
///   time/date (a Wait state)
/// - Begin parallel branches of execution (a Parallel state)
/// - Dynamically iterate steps (a Map state)
#[allow(dead_code)]
pub enum State {
    /// The Map state ("Type": "Map") can be used to run a set of steps for each
    /// element of an input array. While the Parallel state executes multiple
    /// branches of steps using the same input, a Map state will execute the
    /// same steps for multiple entries of an array in the state input.
    Map,
    /// A Choice state ("Type": "Choice") adds branching logic to a state
    /// machine.
    Choice,
    /// A Fail state ("Type": "Fail") stops the execution of the state machine
    /// and marks it as a failure.
    Fail,
    /// The Parallel state ("Type": "Parallel") can be used to create parallel
    /// branches of execution in your state machine.
    Parallel,
    /// A Succeed state ("Type": "Succeed") stops an execution successfully. The
    /// Succeed state is a useful target for Choice state branches that don't do
    /// anything but stop the execution.
    Succeed,
    /// A Wait state ("Type": "Wait") delays the state machine from continuing
    /// for a specified time. You can choose either a relative time, specified
    /// in seconds from when the state begins, or an absolute end time,
    /// specified as a timestamp.
    Wait,
    /// A Task state ("Type": "Task") represents a single unit of work performed
    /// by a state machine.
    Task,
    /// A Pass state ("Type": "Pass") passes its input to its output, without
    /// performing work.
    Pass,
}

/// A State Machine is represented by a JSON Object.
///
/// The operation of a state machine is specified by states, which are
/// represented by JSON objects, fields in the top-level "States" object.
#[allow(dead_code)]
pub struct StateMachine {
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
