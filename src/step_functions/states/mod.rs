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

/// States are represented as fields of the top-level "States" object.
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
