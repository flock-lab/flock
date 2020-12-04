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

//! States can perform a variety of functions in your state machine:
//!
//! - Do some work in your state machine (a Task state)
//! - Make a choice between branches of execution (a Choice state)
//! - Stop an execution with a failure or success (a Fail or Succeed state)
//! - Simply pass input to its output or inject some fixed data (a Pass state)
//! - Provide a delay for a certain amount of time or until a specified
//!   time/date (a Wait state)
//! - Begin parallel branches of execution (a Parallel state)
//! - Dynamically iterate steps (a Map state)

pub mod map;
pub use map::Map;

pub mod choice;
pub use choice::Choice;

pub mod fail;
pub use fail::Fail;

pub mod parallel;
pub use parallel::Parallel;

pub mod pass;
pub use pass::Pass;

pub mod succeed;
pub use succeed::Succeed;

pub mod wait;
pub use wait::Wait;

pub mod task;
pub use task::Task;

pub mod paths;
pub use paths::{InputPath, OutputPath, Parameters, ResultPath, ResultSelector};

pub mod graph;
pub use graph::Dataflow;
