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

//! The Task State (identified by "Type":"Task") causes the interpreter to
//! execute the work identified by the state’s "Resource" field.
//!
//! Here is an example:
//!
//! "TaskState": {
//!    "Comment": "Task State example",
//!    "Type": "Task",
//!    "Resource": "arn:aws:states:us-east-1:123456789012:task:HelloWorld",
//!    "Next": "NextState",
//!    "TimeoutSeconds": 300,
//!    "HeartbeatSeconds": 60
//! }

/// A Task state ("Type": "Task") represents a single unit of work performed by
/// a state machine.
/// All work in your state machine is done by tasks. A task performs work by
/// using an activity or an AWS Lambda function, or by passing parameters to the
/// API actions of other services. AWS Step Functions can invoke Lambda
/// functions directly from a task state. A Lambda function is a cloud-native
/// task that runs on AWS Lambda.
/// https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-task-state.html
#[allow(dead_code)]
pub struct Task {
    /// All states MUST have a "Type" field. This document refers to the values
    /// of this field as a state’s type, and to a state such as the one in the
    /// example above as a Task State.
    /// FIXME: we use `family` to replace `type` keyword reserved by rust.
    pub family:            String,
    /// A Task State MUST include a "Resource" field, whose value MUST be a URI
    /// that uniquely identifies the specific task to execute. The States
    /// language does not constrain the URI scheme nor any other part of the
    /// URI.
    pub resource:          String,
    pub next:              String,
    /// Any state MAY have a "Comment" field, to hold a human-readable comment
    /// or description.
    pub comment:           String,
    /// Tasks can optionally specify timeouts. Timeouts (the "TimeoutSeconds"
    /// and "HeartbeatSeconds" fields) are specified in seconds and MUST be
    /// positive integers.
    /// If the state runs longer than the specified timeout, or if more time
    /// than the specified heartbeat elapses between heartbeats from the task,
    /// then the interpreter fails the state with a States.Timeout Error Name.
    /// If not provided, the default value of "TimeoutSeconds" is 60.
    pub timeout_seconds:   u32,
    /// If provided, the "HeartbeatSeconds" interval MUST be smaller than the
    /// "TimeoutSeconds" value.
    pub heartbeat_seconds: u32,
}
