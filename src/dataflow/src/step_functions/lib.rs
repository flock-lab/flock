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

//! Step Functions is a serverless orchestration service that lets you combine
//! AWS Lambda functions and other AWS services to build business-critical
//! applications. Through Step Functions' graphical console, you see your
//! application’s workflow as a series of event-driven steps.
//! https://docs.aws.amazon.com/step-functions
//!
//! Step Functions is based on state machines and tasks. A state machine is a
//! workflow. A task is a state in a workflow that represents a single unit of
//! work that another AWS service performs. Each step in a workflow is a state.
//!
//! With Step Functions' built-in controls, you examine the state of each step
//! in your workflow to make sure that your application runs in order and as
//! expected. Depending on your use case, you can have Step Functions call AWS
//! services, such as Lambda, to perform tasks. You also can create
//! long-running, automated workflows for applications that require human
//! interaction.
//!
//! Individual states can make decisions based on their input, perform actions,
//! and pass output to other states. In AWS Step Functions you define your
//! workflows in the Amazon States Language (https://states-language.net/). The
//! Step Functions console provides a graphical representation of that state
//! machine to help visualize your application logic.

pub mod common;
pub use common::Common;

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

pub mod state_machine;
pub use state_machine::StateMachine;
