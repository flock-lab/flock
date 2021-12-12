// Copyright (c) 2020-present, UMD Database Group.
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! This crate responsibles for deploying the query to cloud function services
//! on public clouds.

use crate::funcgen::function::QueryFlow;

use daggy::NodeIndex;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_lambda::{CreateFunctionRequest, Lambda, LambdaClient};
use Schedule::Seconds;
use StreamWindow::TumblingWindow;

pub mod lambda;

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
    /// Return a new `ExecutionEnvironment` that executes query locally.
    pub fn new() -> Self {
        Self::Local
    }

    /// Deploy a query to cloud function services on a public cloud.
    pub async fn deploy(&self, query: &QueryFlow) -> Result<()> {
        match &self {
            ExecutionEnvironment::Local => Err(FlockError::FunctionGeneration(
                "Local execution doesn't require a deployment.".to_owned(),
            )),
            ExecutionEnvironment::Lambda => Self::lambda_deployment(&query).await,
            _ => unimplemented!(),
        }
    }

    /// Deploy a query to lambda function services.
    /// To create a function, you need a [deployment package](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-package.html) and an execution role.
    ///
    /// - The deployment package contains lambda function code.
    ///
    /// - The execution role grants the function permission to use AWS services,
    /// such as Amazon CloudWatch Logs for log streaming and AWS X-Ray for
    /// request tracing.
    async fn lambda_deployment(flow: &QueryFlow) -> Result<()> {
        let client = &LambdaClient::new(Region::default());
        for (_, ctx) in flow.ctx.iter() {
            let _: Vec<_> = lambda::function_name(&ctx)
                .iter()
                .map(|name| async move {
                    client
                        .create_function(CreateFunctionRequest {
                            code: lambda::function_code(),
                            environment: lambda::environment(&ctx, true),
                            function_name: name.to_owned(),
                            handler: lambda::handler(),
                            memory_size: lambda::memory_size(&ctx),
                            role: lambda::role().await,
                            runtime: lambda::runtime(),
                            ..CreateFunctionRequest::default()
                        })
                        .await
                })
                .collect();
        }

        // Event source mapping
        if flow.query.as_any().downcast_ref::<StreamQuery>().is_some() {
            // data source node
            let ctx = &flow.ctx[&NodeIndex::new(flow.dag.node_count() - 1)];
            match &ctx.datasource {
                DataSource::KinesisEvent(event) => {
                    let window_in_seconds = match &event.window {
                        TumblingWindow(Seconds(secs)) => secs,
                        _ => unimplemented!(),
                    };
                    let request = kinesis::create_event_source_mapping_request(
                        &event.stream_name,
                        &ctx.name,
                        *window_in_seconds as i64,
                    )
                    .await?;
                    match client.create_event_source_mapping(request).await {
                        Err(e) => {
                            return Err(FlockError::FunctionGeneration(format!(
                                "Kinesis event source mapping failed: {}.",
                                e
                            )));
                        }
                        Ok(_) => return Ok(()),
                    }
                }
                DataSource::KafkaEvent(event) => {
                    let window_in_seconds = match &event.window {
                        TumblingWindow(Seconds(secs)) => secs,
                        _ => unimplemented!(),
                    };
                    let request = kafka::create_event_source_mapping_request(
                        &ctx.name,
                        *window_in_seconds as i64,
                        &event.cluster_arn,
                        &event.topics,
                    )
                    .await?;
                    match client.create_event_source_mapping(request).await {
                        Err(e) => {
                            return Err(FlockError::FunctionGeneration(format!(
                                "Kafka event source mapping failed: {}.",
                                e
                            )));
                        }
                        Ok(_) => return Ok(()),
                    }
                }
                _ => unimplemented!(),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cargo_toml::Manifest;
    use rusoto_core::Region;
    use rusoto_iam::{GetRoleRequest, Iam, IamClient};

    #[tokio::test]
    async fn version_check() -> Result<()> {
        let manifest = Manifest::from_str(include_str!("../../Cargo.toml")).unwrap();
        assert_eq!(env!("CARGO_PKG_VERSION"), manifest.package.unwrap().version);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn get_role() -> Result<()> {
        let iam = IamClient::new(Region::default());
        let resp = iam
            .get_role(GetRoleRequest {
                role_name: "flock".to_owned(),
            })
            .await
            .unwrap();
        println!("{}", resp.role.arn);
        Ok(())
    }
}
