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

//! This crate responsibles for deploying the query to cloud function services
//! on public clouds.

use crate::funcgen::function::QueryFlow;

use lazy_static::lazy_static;
use runtime::error::{Result, SquirtleError};

lazy_static! {
    static ref S3_BUCKET: &'static str = "squirtle";
    static ref S3_KEY: &'static str = "one-function-fit-all";
    static ref S3_OBJECT_VERSION: &'static str = env!("CARGO_PKG_VERSION");
}

/// Query Execution Context decides to execute your queries either remotely or
/// locally.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExecutionEnvironment {
    /// The query is executed in local environment.
    Local,
    /// The query is executed on AWS Lambda Functions.
    Lambda,
    /// The query is executed on Microsoft Azure Functions.
    Azure,
    /// The query is executed on Google Cloud Functions.
    GCP,
    /// The query is executed on Aliyun Cloud Functions.
    AliCloud,
    /// Unknown execution context.
    Unknown,
}

impl ExecutionEnvironment {
    /// Returns a new `ExecutionEnvironment` that executes query locally.
    pub fn new() -> Self {
        Self::Local
    }

    /// Deploys a query to cloud function services on a public cloud.
    pub fn deploy(&self, query: &QueryFlow) -> Result<()> {
        match &self {
            ExecutionEnvironment::Local => Err(SquirtleError::FunctionGeneration(
                "Local execution doesn't require a deployment.".to_owned(),
            )),
            ExecutionEnvironment::Lambda => {
                unimplemented!()
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cargo_toml::Manifest;

    #[tokio::test]
    async fn version_check() -> Result<()> {
        let manifest = Manifest::from_str(include_str!("../../Cargo.toml")).unwrap();
        assert_eq!(env!("CARGO_PKG_VERSION"), manifest.package.unwrap().version);
        Ok(())
    }
}
