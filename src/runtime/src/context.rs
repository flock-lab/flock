// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
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

//! When the lambda function is called for the first time, it deserializes the
//! corresponding execution context from the cloud environment variable.

use serde::{Deserialize, Serialize};

type PhysicalPlan = String;
type LambdaFunctionName = String;

/// Execution environment context for lambda functions.
#[derive(Debug, Deserialize, Serialize)]
pub struct LambdaContext {
    /// JSON formatted string for a specific physical plan.
    pub plan: PhysicalPlan,
    /// Lambda function name(s) for next invocation(s).
    /// - if it's `None`, then the current function is a sink operation.
    /// - if vec.len() = 1, then the next function's concurrency > 1.
    /// - if vec.len() > 1, then the next function's concurrency = 1.
    pub next: Option<Vec<LambdaFunctionName>>,
}
