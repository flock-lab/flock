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

use crate::funcgen::dag::*;
use runtime::prelude::*;
use rusoto_core::Region;
use rusoto_iam::{GetRoleRequest, Iam, IamClient};
use rusoto_lambda::{Environment, FunctionCode};
use std::collections::hash_map::HashMap;

use lazy_static::lazy_static;

/// Your AWS Lambda function's code consists of scripts or compiled programs and
/// their dependencies. You use a deployment package to deploy your function
/// code to Lambda. Lambda supports two types of deployment packages: container
/// images and .zip files. To approach real-time query processing, you **don't
/// require** to upload the deployment package from your local machine. Flock
/// uploaded the pre-compiled deployment package to Amazon Simple Storage
/// Service (Amazon S3) in advance.
struct LambdaDeploymentPackage<'a> {
    /// S3 bucket for the pre-compiled deployment package.
    pub s3_bucket:         &'a str,
    /// S3 key for the pre-compiled deployment package.
    pub s3_key:            &'a str,
    /// S3 object version for the pre-compiled deployment package to be
    /// compatible with the client version.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub agg_batch:  i64,
    /// Stream aggregate operator's memory size (MB).
    #[allow(dead_code)]
    pub agg_stream: i64,
}

lazy_static! {
    static ref LAMBDA_DEPLOYMENT_PACKAGE: LambdaDeploymentPackage<'static> =
        LambdaDeploymentPackage {
            s3_bucket:         "umd-flock",
            s3_key:            "regular",
            s3_object_version: env!("CARGO_PKG_VERSION"),
        };

    static ref NEXMARK_DEPLOYMENT_PACKAGE: LambdaDeploymentPackage<'static> =
        LambdaDeploymentPackage {
            s3_bucket:         "umd-flock",
            s3_key:            "nexmark",
            s3_object_version: env!("CARGO_PKG_VERSION"),
        };

    /// Amazon Linux 2
    static ref LAMABDA_RUNTIME:  &'static str = "provided.al2";

    /// The Amazon Resource Name (ARN) of the function's execution role.
    static ref ROLE_NAME: &'static str = "flock";

    static ref LAMBDA_MEMORY_FOOTPRINT: LambdaMemoryFootprint =
        LambdaMemoryFootprint {
            default: 128,
            agg_batch: 2048,
            agg_stream: 512,
        };
}

/// The lambda function code in Amazon S3.
pub fn function_code() -> FunctionCode {
    FunctionCode {
        s3_bucket:         Some(LAMBDA_DEPLOYMENT_PACKAGE.s3_bucket.to_owned()),
        s3_key:            Some(LAMBDA_DEPLOYMENT_PACKAGE.s3_key.to_owned()),
        s3_object_version: None,
        zip_file:          None,
        image_uri:         None,
    }
}

/// The nexmark benchmark code in Amazon S3.
pub fn nexmark_function_code() -> FunctionCode {
    FunctionCode {
        s3_bucket:         Some(NEXMARK_DEPLOYMENT_PACKAGE.s3_bucket.to_owned()),
        s3_key:            Some(NEXMARK_DEPLOYMENT_PACKAGE.s3_key.to_owned()),
        s3_object_version: None,
        zip_file:          None,
        image_uri:         None,
    }
}

/// Environment variables that are accessible from function code during
/// execution.
pub fn environment(ctx: &ExecutionContext, debug: bool) -> Option<Environment> {
    let mut map = HashMap::new();
    map.insert(
        (&FLOCK_CONF["lambda"]["name"]).to_owned(),
        ctx.marshal(Encoding::Zstd),
    );
    // Enable crate `env_logger`
    // https://docs.rs/env_logger/latest/env_logger/
    if debug {
        map.insert(
            "RUST_LOG".to_owned(),
            "info".to_owned(),
        );
    }
    map.insert("RUST_BACKTRACE".to_owned(), "1".to_owned());
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
///   function's concurrency = 1 and its type is `CloudFunction::Group((name,
///   group_size))`.
///
/// - If the next call is `CloudFunction::Group(..)`, then the current lambda
///   function's concurrency > 1 (default = 8) and its type is
///   `CloudFunction::Lambda(name)`.
///
/// - If the next call is `CloudFunction::Lambda(..)`, then the current lambda
///   function's concurrency = 1 and its type is `CloudFunction::Group((name,
///   group_size))`.
pub fn function_name(ctx: &ExecutionContext) -> Vec<String> {
    if ctx.datasource != DataSource::Payload {
        return vec![ctx.name.to_owned()];
    }

    match &ctx.next {
        CloudFunction::None => (0..CONCURRENCY_8)
            .map(|idx| format!("{}-{}", ctx.name, idx))
            .collect(),
        CloudFunction::Group(..) => vec![ctx.name.to_owned()],
        CloudFunction::Lambda(..) => (0..CONCURRENCY_8)
            .map(|idx| format!("{}-{}", ctx.name, idx))
            .collect(),
    }
}

/// The identifier of the function's runtime.
/// <https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html>
pub fn runtime() -> Option<String> {
    Some(LAMABDA_RUNTIME.to_owned())
}

/// The name of the method within your code that Lambda calls to execute your
/// function. The format includes the file name. It can also include namespaces
/// and other qualifiers, depending on the runtime.
pub fn handler() -> Option<String> {
    Some("handler".to_owned())
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
