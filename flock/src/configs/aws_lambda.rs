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

//! Helper functions to create a Lambda function.

use crate::configs::FLOCK_CONF;
use crate::encoding::Encoding;
use crate::error::{FlockError, Result};
use crate::runtime::context::ExecutionContext;
use rusoto_core::Region;
use rusoto_iam::{GetRoleRequest, Iam, IamClient};
use rusoto_lambda::{Environment, FunctionCode};
use std::collections::hash_map::HashMap;

/// Aws Lambda function configuration.
#[derive(Debug, Clone)]
pub struct AwsLambdaConfig {
    /// Lambda supports multiple languages through the use of runtimes. The
    /// runtime provides a language-specific environment that runs in an
    /// execution environment. The runtime relays invocation events, context
    /// information, and responses between Lambda and the function. You can use
    /// runtimes that Lambda provides, or build your own. If you package your
    /// code as a .zip file archive, you must configure your function to use a
    /// runtime that matches your programming language. For a container image,
    /// you include the runtime when you build the image.
    ///
    /// # Examples
    ///
    /// ## Amazon Linux
    /// * Image – amzn-ami-hvm-2018.03.0.20181129-x86_64-gp2
    /// * Linux kernel – 4.14.171-105.231.amzn1.x86_64
    ///
    /// ## Amazon Linux 2
    /// * Image – Custom
    /// * Linux kernel – 4.14.165-102.205.amzn2.x86_64
    ///
    /// ## Custom runtime
    /// | Name           | identifier   | Operating system | Architecture  |
    /// |----------------|--------------|------------------|---------------|
    /// | Custom Runtime | provided.al2 | Amazon Linux 2   | x86_64, arm64 |
    /// | Custom Runtime | provided     | Amazon Linux     | x86_64        |
    ///
    /// For more information about Lambda runtimes, see the AWS Lambda Runtimes:
    /// https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
    pub runtime:       Option<String>,
    /// The AWS Lambda function handler.
    ///
    /// The Lambda function handler is the method in your function code that
    /// processes events. When your function is invoked, Lambda runs the handler
    /// method. When the handler exits or returns a response, it becomes
    /// available to handle another event.
    pub handler:       Option<String>,
    /// The AWS Lambda function memory size.
    ///
    /// The value of the memory property must be a multiple of 64 MB. This
    /// restriction isn't configurable. The default value is 128 MB. AWS Lambda
    /// customers can now provision Lambda functions with a maximum of 10,240 MB
    /// (10 GB) of memory, a more than 3x increase compared to the previous
    /// limit of 3,008 MB. This helps workloads like batch, extract, transform,
    /// load (ETL) jobs, and media processing applications perform memory
    /// intensive operations at scale.
    /// The price for Duration depends on the amount of memory you allocate to
    /// your function. You can allocate any amount of memory to your function
    /// between 128MB and 10,240MB, in 1MB increments. The table below contains
    /// a few examples of the price per 1ms associated with different memory
    /// sizes.
    ///
    /// # Example
    ///
    /// | Memory (MB) (with larger memory) | Price per 1ms |
    /// |----------------------------------|---------------|
    /// | 128                              | $0.0000000021 |
    /// | 512                              | $0.0000000083 |
    /// | 1024                             | $0.0000000167 |
    /// | 1536                             | $0.0000000250 |
    /// | 2048                             | $0.0000000333 |
    /// | 3072                             | $0.0000000500 |
    /// | 4096                             | $0.0000000667 |
    /// | 5120                             | $0.0000000833 |
    /// | 6144                             | $0.0000001000 |
    /// | 7168                             | $0.0000001167 |
    /// | 8192                             | $0.0000001333 |
    /// | 9216                             | $0.0000001500 |
    /// | 10240                            | $0.0000001667 |
    ///
    /// TODO: The follow-up research work here is to use machine learning to
    /// dynamically optimize the memory size setting of each lambda function.
    pub memory_size:   Option<i64>,
    /// The AWS Lambda function timeout.
    ///
    /// The value of the timeout property is a maximum function execution time,
    /// measured in seconds. The maximum execution time for a function is 900s
    /// or 15 minutes.
    /// When the specified timeout is reached, AWS Lambda terminates execution
    /// of your Lambda function. As a best practice, you should set the timeout
    /// value based on your expected execution time to prevent your function
    /// from running longer than intended.
    pub timeout:       Option<i64>,
    /// The AWS Lambda function role.
    ///
    /// A Lambda function's execution role is an AWS Identity and Access
    /// Management (IAM) role that grants the function permission to access AWS
    /// services and resources. You provide this role when you create a
    /// function, and Lambda assumes the role when your function is invoked.
    ///
    /// The role is used to determine the permissions that your Lambda function
    /// has to access AWS resources. The role must be a role that you have
    /// created in your AWS account. For more information, see the AWS Lambda
    /// Roles: https://docs.aws.amazon.com/lambda/latest/dg/lambda-intro-execution-role.html
    pub role:          String,
    /// The AWS Lambda function VPC configuration.
    ///
    /// You can configure a Lambda function to connect to private subnets in a
    /// virtual private cloud (VPC) in your AWS account. Use Amazon Virtual
    /// Private Cloud (Amazon VPC) to create a private network for resources
    /// such as databases, cache instances, or internal services. Connect your
    /// function to the VPC to access private resources while the function is
    /// running.
    pub vpc_config:    Option<rusoto_lambda::VpcConfig>,
    /// The AWS Lambda function environment variables.
    ///
    /// Lambda invokes your function in an execution environment. The execution
    /// environment provides a secure and isolated runtime environment that
    /// manages the resources required to run your function. Lambda re-uses the
    /// execution environment from a previous invocation if one is available, or
    /// it can create a new execution environment.
    pub environment:   Option<Environment>,
    /// The AWS Lambda function code.
    ///
    /// The Lambda service stores your function code in an internal S3 bucket
    /// that's private to your account. Each AWS account is allocated 75 GB of
    /// storage in each Region.
    pub code:          FunctionCode,
    /// The AWS Lambda function names.
    ///
    /// The function names are unique to each AWS account. If you choose not to
    /// specify a function name, AWS Lambda assigns a unique name to your
    /// function.
    /// The name of the Lambda function.
    ///
    /// Name formats
    ///
    /// - Function name: my-function.
    /// - Function ARN:
    ///   arn:aws:lambda:us-west-2:123456789012:function:my-function.
    /// - Partial ARN: 123456789012:function:my-function.
    ///
    /// The length constraint applies only to the full ARN. If you
    /// specify only the function name, it is limited to 64 characters in
    /// length.
    pub function_name: String,
}

impl AwsLambdaConfig {
    /// Creates a new AWS Lambda function.
    pub async fn try_new() -> Result<AwsLambdaConfig> {
        let runtime = Some(FLOCK_CONF["aws"]["runtime"].to_string());
        let handler = Some("handler".to_owned());
        let memory_size = Some(
            FLOCK_CONF["lambda"]["regular_memory_size"]
                .parse::<i64>()
                .unwrap(),
        );
        let timeout = Some(FLOCK_CONF["lambda"]["timeout"].parse::<i64>().unwrap());
        let role = AwsLambdaConfig::default_role().await?;
        let vpc_config = None;
        let environment = None;

        // Flock uploaded the pre-compiled deployment package to Amazon S3 in advance.
        let code = FunctionCode {
            // S3 bucket for the pre-compiled deployment package.
            s3_bucket:         Some(FLOCK_CONF["s3"]["bucket"].to_string()),
            // S3 key for the pre-compiled deployment package.
            s3_key:            Some(FLOCK_CONF["s3"]["key"].to_string()),
            s3_object_version: None,
            zip_file:          None,
            image_uri:         None,
        };

        let function_name = "".to_string();

        Ok(AwsLambdaConfig {
            runtime,
            handler,
            memory_size,
            timeout,
            role,
            vpc_config,
            environment,
            code,
            function_name,
        })
    }

    /// Creates a new AWS Lambda function with the specified function name.
    pub fn set_runtime(&mut self, runtime: &str) -> &mut Self {
        self.runtime = Some(runtime.to_string());
        self
    }

    /// Creates a new AWS Lambda function with the specified handler name.
    pub fn set_handler(&mut self, handler: &str) -> &mut Self {
        self.handler = Some(handler.to_owned());
        self
    }

    /// Creates a new AWS Lambda function with the specified memory size.
    pub fn set_memory_size(&mut self, memory_size: i64) -> &mut Self {
        self.memory_size = Some(memory_size);
        self
    }

    /// Creates a new AWS Lambda function with the specified timeout.
    pub fn set_timeout(&mut self, timeout: i64) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// Creates a new AWS Lambda function with the specified role.
    pub fn set_role(&mut self, role: &str) -> &mut Self {
        self.role = role.to_string();
        self
    }

    /// Creates a new AWS Lambda function with the specified VPC configuration.
    pub fn set_vpc_config(&mut self, vpc_config: rusoto_lambda::VpcConfig) -> &mut Self {
        // FIXME: Disable mount EFS
        // file_system_configs: Some(vec![FileSystemConfig {
        //     arn:              efs,
        //     local_mount_path: mount_path.to_str().unwrap().to_string(),
        // }]),
        // vpc_config: Some(VpcConfig {
        //     subnet_ids:         Some(vec![FLOCK_SUBNET_ID.clone()]),
        //     security_group_ids: Some(vec![FLOCK_SECURITY_GROUP_ID.clone()]),
        // }),
        self.vpc_config = Some(vpc_config);
        self
    }

    /// Creates a new AWS Lambda function with the specified environment and
    /// function name.
    pub fn set_function_spec(&mut self, ctx: &ExecutionContext) -> &mut Self {
        // Set the environment variables.
        let mut map = HashMap::new();
        map.insert(
            (&FLOCK_CONF["lambda"]["environment"]).to_owned(),
            ctx.marshal(Encoding::default()).unwrap(),
        );
        map.insert("RUST_LOG".to_owned(), "info".to_owned());
        map.insert("RUST_BACKTRACE".to_owned(), "full".to_owned());

        self.environment = Some(Environment {
            variables: Some(map),
        });

        // Set the function name.
        self.function_name = ctx.name.clone();
        self
    }

    /// Creates a new AWS Lambda function with a default role.
    async fn default_role() -> Result<String> {
        let iam = IamClient::new(Region::default());
        let resp = iam
            .get_role(GetRoleRequest {
                role_name: FLOCK_CONF["aws"]["role"].to_string(),
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;
        Ok(resp.role.arn)
    }
}
