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

//! Helper functions to create a Lambda function.

use crate::funcgen::dag::*;
use runtime::context::{CloudFunction, ExecutionContext};
use runtime::encoding::Encoding;
use rusoto_core::Region;
use rusoto_iam::{GetRoleRequest, Iam, IamClient};
use rusoto_lambda::{Environment, FunctionCode};
use std::collections::hash_map::HashMap;

use lazy_static::lazy_static;

/// Your AWS Lambda function's code consists of scripts or compiled programs and
/// their dependencies. You use a deployment package to deploy your function
/// code to Lambda. Lambda supports two types of deployment packages: container
/// images and .zip files. To approach real-time query processing, you **don't
/// require** to upload the deployment package from your local machine. Squirtle
/// uploaded the pre-compiled deployment package to Amazon Simple Storage
/// Service (Amazon S3) in advance.
struct LambdaDeploymentPackage<'a> {
    /// S3 bucket for the pre-compiled deployment package.
    pub s3_bucket:         &'a str,
    /// S3 key for the pre-compiled deployment package.
    pub s3_key:            &'a str,
    /// S3 object version for the pre-compiled deployment package to be
    /// compatible with the client version.
    pub s3_object_version: &'a str,
}

/// The price for Duration depends on the amount of memory you allocate to your
/// function. You can allocate any amount of memory to your function between
/// 128MB and 10,240MB, in 1MB increments. The table below contains a few
/// examples of the price per 1ms associated with different memory sizes.
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
struct LambdaMemoryFootprint {
    // regular operator's memory size (MB).
    pub default:    i64,
    /// OLAP aggregate operator's memory size (MB).
    pub agg_batch:  i64,
    /// Stream aggregate operator's memory size (MB).
    pub agg_stream: i64,
}

lazy_static! {
    static ref LAMBDA_DEPLOYMENT_PACKAGE: LambdaDeploymentPackage<'static> =
        LambdaDeploymentPackage {
            s3_bucket:         "squirtle",
            s3_key:            "one-function-fits-all",
            s3_object_version: env!("CARGO_PKG_VERSION"),
        };

    /// Amazon Linux 2
    static ref LAMABDA_RUNTIME:  &'static str = "provided.al2";

    /// The Amazon Resource Name (ARN) of the function's execution role.
    static ref ROLE_NAME: &'static str = "squirtle";

    static ref LAMBDA_MEMORY_FOOTPRINT: LambdaMemoryFootprint =
        LambdaMemoryFootprint {
            default: 128,
            agg_batch: 2048,
            agg_stream: 512,
        };
}

/// The code for the function where we specify an object in Amazon S3.
pub fn function_code() -> FunctionCode {
    FunctionCode {
        s3_bucket:         Some(LAMBDA_DEPLOYMENT_PACKAGE.s3_bucket.to_owned()),
        s3_key:            Some(LAMBDA_DEPLOYMENT_PACKAGE.s3_key.to_owned()),
        s3_object_version: Some(LAMBDA_DEPLOYMENT_PACKAGE.s3_object_version.to_owned()),
        zip_file:          None,
    }
}

/// Environment variables that are accessible from function code during
/// execution.
pub fn environment(ctx: &ExecutionContext) -> Option<Environment> {
    let mut map = HashMap::new();
    map.insert(
        "execution_context".to_owned(),
        ctx.marshal(Encoding::Snappy),
    );
    Some(Environment {
        variables: Some(map),
    })
}

/// The name of the Lambda function.
///
/// Name formats
///
/// - Function name: my-function.
/// - Function ARN: arn:aws:lambda:us-west-2:123456789012:function:my-function.
/// - Partial ARN: 123456789012:function:my-function.
///
/// The length constraint applies only to the full ARN. If you
/// specify only the function name, it is limited to 64 characters in length.
///
/// - If the next call is `CloudFunction::None`, then the current lambda
///   function's concurrency = 1 and its type is `CloudFunction::Chorus((name,
///   group_size))`.
///
/// - If the next call is `CloudFunction::Chorus(..)`, then the current lambda
///   function's concurrency > 1 (default = 8) and its type is
///   `CloudFunction::Solo(name)`.
///
/// - If the next call is `CloudFunction::Solo(..)`, then the current lambda
///   function's concurrency = 1 and its type is `CloudFunction::Chorus((name,
///   group_size))`.
pub fn function_name(ctx: &ExecutionContext) -> Vec<String> {
    match &ctx.next {
        CloudFunction::None => (0..CONCURRENCY_8)
            .map(|idx| format!("{}-{}", ctx.name, idx))
            .collect(),
        CloudFunction::Chorus(..) => vec![ctx.name.to_owned()],
        CloudFunction::Solo(..) => (0..CONCURRENCY_8)
            .map(|idx| format!("{}-{}", ctx.name, idx))
            .collect(),
    }
}

/// The identifier of the function's runtime.
/// <https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html>
pub fn runtime() -> String {
    LAMABDA_RUNTIME.to_owned()
}

/// The name of the method within your code that Lambda calls to execute your
/// function. The format includes the file name. It can also include namespaces
/// and other qualifiers, depending on the runtime.
pub fn handler() -> String {
    "doesn't matter".to_owned()
}

/// The amount of memory that your function has access to. Increasing the
/// function's memory also increases its CPU allocation. The default value is
/// 128 MB. The value must be a multiple of 64 MB.
pub fn memory_size(_ctx: &ExecutionContext) -> Option<i64> {
    // TODO: optimize the memory size for different subplans.
    Some(LAMBDA_MEMORY_FOOTPRINT.default)
}

/// The Amazon Resource Name (ARN) of the function's execution role.
pub async fn role() -> String {
    let iam = IamClient::new(Region::default());
    let resp = iam
        .get_role(GetRoleRequest {
            role_name: ROLE_NAME.to_owned(),
        })
        .await
        .unwrap();
    resp.role.arn
}