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

//! This module contains various utility functions.

use crate::configs::AwsLambdaConfig;
use crate::configs::FLOCK_CONF;
use crate::driver::monitor::cloudwatch::{self, fetch_logs, AWSResponse};
use crate::error::{FlockError, Result};
use crate::runtime::context::ExecutionContext;
use bytes::Bytes;
use humantime::parse_duration;
use lazy_static::lazy_static;
use log::info;
use rusoto_core::{ByteStream, Region, RusotoError};
use rusoto_efs::{
    CreateAccessPointError, CreateAccessPointRequest, CreateFileSystemError,
    CreateFileSystemRequest, CreateMountTargetError, CreateMountTargetRequest, CreationInfo,
    DescribeAccessPointsRequest, DescribeFileSystemsRequest, Efs, EfsClient, PosixUser,
    RootDirectory,
};
use rusoto_lambda::{
    CreateFunctionRequest, GetFunctionRequest, InvocationRequest, InvocationResponse, Lambda,
    LambdaClient, PutFunctionConcurrencyRequest, UpdateFunctionCodeRequest,
};
use rusoto_logs::CloudWatchLogsClient;
use rusoto_s3::{ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use rusoto_sqs::SqsClient;
use std::time::Duration;

lazy_static! {
    /// AWS Lambda function async invocation.
    pub static ref FLOCK_LAMBDA_ASYNC_CALL: String = "Event".to_string();
    /// AWS Lambda function sync invocation.
    pub static ref FLOCK_LAMBDA_SYNC_CALL: String = "RequestResponse".to_string();
    /// AWS Lambda function maximum error retry.
    pub static ref FLOCK_LAMBDA_MAX_RETRIES: usize = FLOCK_CONF["lambda"]["max_invoke_retries"].parse::<usize>().unwrap();
    /// AWS Lambda function timeout.
    pub static ref FLOCK_LAMBDA_TIMEOUT: i64 = FLOCK_CONF["lambda"]["timeout"].parse::<i64>().unwrap();

    /// Flock sync invocation granularity.
    pub static ref FLOCK_SYNC_GRANULE_SIZE: usize = FLOCK_CONF["lambda"]["sync_granule"].parse::<usize>().unwrap();
    /// Flock async invocation granularity.
    pub static ref FLOCK_ASYNC_GRANULE_SIZE: usize = FLOCK_CONF["lambda"]["async_granule"].parse::<usize>().unwrap();

    /// Flock S3 key prefix.
    pub static ref FLOCK_S3_KEY: String = FLOCK_CONF["s3"]["key"].to_string();
    /// Flock S3 bucket name.
    pub static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["s3"]["bucket"].to_string();
    /// Flock availablity zone.
    pub static ref FLOCK_AVAILABILITY_ZONE: String = FLOCK_CONF["aws"]["availability_zone"].to_string();
    /// Flock subnet id.
    pub static ref FLOCK_SUBNET_ID: String = FLOCK_CONF["aws"]["subnet_id"].to_string();
    /// Flock security group id.
    pub static ref FLOCK_SECURITY_GROUP_ID: String = FLOCK_CONF["aws"]["security_group_id"].to_string();

    /// Flock EFS creation token.
    pub static ref FLOCK_EFS_CREATION_TOKEN: String = FLOCK_CONF["efs"]["creation_token"].to_string();
    /// Flock EFS Posix user ID.
    pub static ref FLOCK_EFS_POSIX_UID: i64 = FLOCK_CONF["efs"]["user_id"].parse::<i64>().unwrap();
    /// Flock EFS Posix group ID.
    pub static ref FLOCK_EFS_POSIX_GID: i64 = FLOCK_CONF["efs"]["group_id"].parse::<i64>().unwrap();
    /// Flock EFS access point permissions.
    pub static ref FLOCK_EFS_PERMISSIONS: String = FLOCK_CONF["efs"]["permissions"].to_string();
    /// Flock EFS root directory.
    pub static ref FLOCK_EFS_ROOT_DIR: String = FLOCK_CONF["efs"]["root_directory"].to_string();
    /// Flocl EFS local mount point.
    pub static ref FLOCK_EFS_MOUNT_PATH: String = FLOCK_CONF["efs"]["mount_path"].to_string();

    /// Flock associated services.
    /// Flock S3 Client.
    pub static ref FLOCK_S3_CLIENT: S3Client = S3Client::new(Region::default());
    /// Flock LAMBDA Client.
    pub static ref FLOCK_LAMBDA_CLIENT: LambdaClient = LambdaClient::new(Region::default());
    /// Flock EFS Client.
    pub static ref FLOCK_EFS_CLIENT: EfsClient = EfsClient::new(Region::default());
    /// Flock SQS Client.
    pub static ref FLOCK_SQS_CLIENT: SqsClient = SqsClient::new(Region::default());
    /// Flock CloudWatch Logs Client.
    pub static ref FLOCK_WATCHLOGS_CLIENT: CloudWatchLogsClient = CloudWatchLogsClient::new(Region::default());
}

/// Set the lambda function's concurrency.
/// <https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html>
pub async fn set_lambda_concurrency(function_name: String, concurrency: i64) -> Result<()> {
    let request = PutFunctionConcurrencyRequest {
        function_name,
        reserved_concurrent_executions: concurrency,
    };
    let concurrency = FLOCK_LAMBDA_CLIENT
        .put_function_concurrency(request)
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?;
    assert_ne!(concurrency.reserved_concurrent_executions, Some(0));
    Ok(())
}

/// Fetch the lambda function's latest log.
pub async fn fetch_aws_watchlogs(group: &str, mtime: std::time::Duration) -> Result<()> {
    let mut logged = false;
    let timeout = parse_duration("1min").unwrap();
    let sleep_for = parse_duration("5s").ok();
    let mut token: Option<String> = None;
    let mut req = cloudwatch::create_filter_request(group, mtime, None, token);
    loop {
        if logged {
            break;
        }

        match fetch_logs(&FLOCK_WATCHLOGS_CLIENT, req, timeout)
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?
        {
            AWSResponse::Token(x) => {
                info!("Got a Token response");
                logged = true;
                token = Some(x);
                req = cloudwatch::create_filter_request(group, mtime, None, token);
            }
            AWSResponse::LastLog(t) => match sleep_for {
                Some(x) => {
                    info!("Got a lastlog response");
                    token = None;
                    req = cloudwatch::create_filter_from_timestamp(group, t, None, token);
                    info!("Waiting {:?} before requesting logs again...", x);
                    tokio::time::sleep(x).await;
                }
                None => break,
            },
        };
    }

    Ok(())
}

/// Invoke the lambda function with the given payload.
///
/// # Arguments
/// * `function_name` - The name of the lambda function.
/// * `payload` - The payload to be passed to the lambda function.
/// * `invocation_type` - The invocation type of the lambda function.
///   - `Event` - Asynchronous invocation.
///   - `RequestResponse` - Synchronous invocation.
///
/// # Returns
/// The result of the invocation.
pub async fn invoke_lambda_function(
    function_name: String,
    payload: Option<Bytes>,
    invocation_type: String,
) -> Result<InvocationResponse> {
    let request = InvocationRequest {
        function_name,
        payload,
        invocation_type: Some(invocation_type.clone()),
        ..Default::default()
    };

    if invocation_type == *FLOCK_LAMBDA_ASYNC_CALL {
        let response = FLOCK_LAMBDA_CLIENT
            .invoke(request)
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;
        Ok(response)
    } else {
        // Error retries and exponential backoff in AWS Lambda
        let mut retries = 0;
        loop {
            let response = FLOCK_LAMBDA_CLIENT
                .invoke(request.clone())
                .await
                .map_err(|e| FlockError::AWS(e.to_string()))?;
            if response.function_error.is_none() {
                return Ok(response);
            }
            if retries > 0 {
                if response.payload.is_some() {
                    info!(
                        "Function invocation error: {:?}",
                        serde_json::from_slice::<serde_json::Value>(&response.payload.unwrap())
                    );
                }
                info!("Retrying invocation...");
            }

            tokio::time::sleep(Duration::from_millis(2_u64.pow(retries) * 100)).await;

            retries += 1;

            if retries as usize > *FLOCK_LAMBDA_MAX_RETRIES {
                return Err(FlockError::AWS(format!(
                    "Sync invocation failed after {} retries",
                    *FLOCK_LAMBDA_MAX_RETRIES
                )));
            }
        }
    }
}

/// Creates a single lambda function using bootstrap.zip in Amazon S3.
pub async fn create_lambda_function(
    ctx: &ExecutionContext,
    memory_size: i64,
    architecture: &str,
) -> Result<String> {
    let func_name = ctx.name.clone();
    if FLOCK_LAMBDA_CLIENT
        .get_function(GetFunctionRequest {
            function_name: ctx.name.clone(),
            ..Default::default()
        })
        .await
        .is_ok()
    {
        let conf = FLOCK_LAMBDA_CLIENT
            .update_function_code(UpdateFunctionCodeRequest {
                function_name: func_name.clone(),
                s3_bucket: Some(FLOCK_S3_BUCKET.clone()),
                s3_key: Some(FLOCK_S3_KEY.clone()),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;
        conf.function_name
            .ok_or_else(|| FlockError::AWS("No function name!".to_string()))
    } else {
        let mut conf = AwsLambdaConfig::try_new().await?;
        conf.set_memory_size(memory_size);
        conf.set_function_spec(ctx);
        conf.set_architectures(vec![architecture.to_string()]);
        let resp = FLOCK_LAMBDA_CLIENT
            .create_function(CreateFunctionRequest {
                architectures: conf.architectures,
                function_name: conf.function_name,
                code: conf.code,
                handler: conf.handler,
                runtime: conf.runtime,
                role: conf.role,
                vpc_config: conf.vpc_config,
                environment: conf.environment,
                timeout: conf.timeout,
                memory_size: conf.memory_size,
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;

        resp.function_name
            .ok_or_else(|| FlockError::AWS("No function name!".to_string()))
    }
}

/// Creates an elastic file system.
///
/// Amazon Elastic File System (Amazon EFS) provides a simple, scalable, elastic
/// file system for general purpose workloads for use with AWS Cloud services
/// and on-premises resources.
///
/// Creates a new, empty file system. The operation requires a creation token in
/// the request that Amazon EFS uses to ensure idempotent creation (calling the
/// operation with same creation token has no effect). If a file system does not
/// currently exist that is owned by the caller's AWS account with the specified
/// creation token, this operation does the following:
///
/// * Creates a new, empty file system. The file system will have an Amazon EFS
///   assigned ID, and an initial lifecycle state `creating`.
/// * Returns with the description of the created file system.
///
/// Otherwise, this operation returns a `FileSystemAlreadyExists` error with the
/// ID of the existing file system.
///
/// For basic use cases, you can use a randomly generated UUID for the creation
/// token.
///
/// # Returns
/// The ID of the file system.
pub async fn create_aws_efs() -> Result<String> {
    let req = CreateFileSystemRequest {
        // A string of up to 64 ASCII characters. Amazon EFS uses this to ensure idempotent
        // creation.
        creation_token: FLOCK_EFS_CREATION_TOKEN.to_string(),
        // Used to create a file system that uses One Zone storage classes. It specifies the AWS
        // Availability Zone in which to create the file system.
        availability_zone_name: Some(FLOCK_AVAILABILITY_ZONE.to_string()),
        ..Default::default()
    };

    match FLOCK_EFS_CLIENT.create_file_system(req).await {
        Ok(resp) => Ok(resp.file_system_id),
        Err(RusotoError::Service(CreateFileSystemError::FileSystemAlreadyExists(_))) => {
            Ok(String::new())
        }
        Err(e) => Err(FlockError::AWS(e.to_string())),
    }
}

/// Returns the description of a specific Amazon EFS file system if either the
/// file system `CreationToken` or the `FileSystemId` is provided. Otherwise, it
/// returns descriptions of all file systems owned by the caller's AWS account
/// in the AWS Region of the endpoint that you're calling. This operation
/// requires permissions for the elasticfilesystem:DescribeFileSystems action.
///
/// # Returns
/// The ID of the file system.
pub async fn discribe_aws_efs() -> Result<String> {
    let req = DescribeFileSystemsRequest {
        creation_token: Some(FLOCK_EFS_CREATION_TOKEN.to_string()),
        ..Default::default()
    };
    match FLOCK_EFS_CLIENT.describe_file_systems(req).await {
        Ok(resp) => Ok(resp.file_systems.unwrap()[0].file_system_id.clone()),
        Err(e) => Err(FlockError::AWS(e.to_string())),
    }
}

/// Creates an EFS access point. An access point is an application-specific view
/// into an EFS file system that applies an operating system user and group, and
/// a file system path, to any file system request made through the access
/// point. The operating system user and group override any identity information
/// provided by the NFS client. The file system path is exposed as the access
/// point's root directory. Applications using the access point can only access
/// data in its own directory and below.
///
/// # Arguments
/// * `file_system_id` - The ID of the EFS file system that the access point
///
/// # Returns
/// The ID of the access point.
pub async fn create_aws_efs_access_point(file_system_id: &str) -> Result<String> {
    let req = CreateAccessPointRequest {
        file_system_id: file_system_id.to_string(),
        client_token: FLOCK_EFS_CREATION_TOKEN.to_string(),
        posix_user: Some(PosixUser {
            gid:            *FLOCK_EFS_POSIX_GID,
            uid:            *FLOCK_EFS_POSIX_UID,
            secondary_gids: None,
        }),
        root_directory: Some(RootDirectory {
            path:          Some(FLOCK_EFS_ROOT_DIR.to_string()),
            creation_info: Some(CreationInfo {
                owner_gid:   *FLOCK_EFS_POSIX_GID,
                owner_uid:   *FLOCK_EFS_POSIX_UID,
                permissions: FLOCK_EFS_PERMISSIONS.to_string(),
            }),
        }),
        ..Default::default()
    };

    match FLOCK_EFS_CLIENT.create_access_point(req).await {
        Ok(resp) => resp
            .access_point_id
            .ok_or_else(|| FlockError::AWS("No access point ID!".to_string())),
        Err(RusotoError::Service(CreateAccessPointError::AccessPointAlreadyExists(_))) => {
            Ok(String::new())
        }
        Err(e) => Err(FlockError::AWS(e.to_string())),
    }
}

/// Returns the description of a specific Amazon EFS access point if the
/// AccessPointId is provided. If you provide an EFS FileSystemId, it returns
/// descriptions of all access points for that file system. You can provide
/// either an AccessPointId or a FileSystemId in the request, but not both.
///
/// # Arguments
/// * `access_point_id` - The ID of the access point.
/// * `file_system_id` - The ID of the EFS file system.
///
/// # Returns
/// The unique Amazon Resource Name (ARN) associated with the access point.
pub async fn describe_aws_efs_access_point(
    access_point_id: Option<String>,
    file_system_id: Option<String>,
) -> Result<String> {
    let req = DescribeAccessPointsRequest {
        access_point_id,
        file_system_id,
        ..Default::default()
    };

    match FLOCK_EFS_CLIENT
        .describe_access_points(req)
        .await
        .map_err(|e| FlockError::AWS(e.to_string()))
    {
        Ok(resp) => resp
            .access_points
            .ok_or_else(|| FlockError::AWS("No access points!".to_string()))?[0]
            .access_point_arn
            .clone()
            .ok_or_else(|| FlockError::AWS("No access point arn!".to_string())),
        Err(e) => Err(FlockError::AWS(e.to_string())),
    }
}

/// Creates a mount target for a file system. You can then mount the file system
/// on EC2 or Lambda instances by using the mount target. All instances in a VPC
/// within a given Availability Zone share a single mount target for a given
/// file system. If you have multiple subnets in an Availability Zone, you
/// create a mount target in one of the subnets. Instances do not need to be
/// in the same subnet as the mount target in order to access their file system.
/// You can create only one mount target for an EFS file system using One Zone
/// storage classes. You must create that mount target in the same Availability
/// Zone in which the file system is located.
///
/// # Arguments
/// * `file_system_id` - The ID of the EFS file system that the mount target
///
/// # Returns
/// The ID of the mount target.
pub async fn create_mount_target(file_system_id: &str) -> Result<String> {
    let req = CreateMountTargetRequest {
        file_system_id: file_system_id.to_string(),
        subnet_id: FLOCK_SUBNET_ID.clone(),
        security_groups: Some(vec![FLOCK_SECURITY_GROUP_ID.clone()]),
        ..Default::default()
    };

    match FLOCK_EFS_CLIENT.create_mount_target(req).await {
        Ok(resp) => Ok(resp.mount_target_id),
        Err(RusotoError::Service(CreateMountTargetError::MountTargetConflict(_))) => {
            Ok(String::new())
        }
        Err(e) => Err(FlockError::AWS(e.to_string())),
    }
}

/// Puts an object to AWS S3 if the object does not exist. If the object exists,
/// it isn't modified.
///
/// # Arguments
/// * `bucket` - The name of the bucket to put the object in.
/// * `key` - The key of the object to put.
/// * `body` - The body of the object to put.
pub async fn put_object_to_s3_if_missing(bucket: String, key: String, body: Vec<u8>) -> Result<()> {
    if let Some(0) = FLOCK_S3_CLIENT
        .list_objects_v2(ListObjectsV2Request {
            bucket: bucket.clone(),
            prefix: Some(key.clone()),
            max_keys: Some(1),
            ..Default::default()
        })
        .await
        .map_err(|e| FlockError::Internal(e.to_string()))?
        .key_count
    {
        FLOCK_S3_CLIENT
            .put_object(PutObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                body: Some(ByteStream::from(body)),
                ..Default::default()
            })
            .await
            .map_err(|e| FlockError::AWS(e.to_string()))?;
    }
    Ok(())
}
