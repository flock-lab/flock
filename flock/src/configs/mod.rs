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

//! This module provides various default configurations for Flock.

pub mod aws_lambda;
pub use aws_lambda::AwsLambdaConfig;

mod flock;
pub use self::flock::FLOCK_CONF;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use lazy_static::lazy_static;
use rusoto_core::Region;
use rusoto_efs::EfsClient;
use rusoto_lambda::LambdaClient;
use rusoto_logs::CloudWatchLogsClient;
use rusoto_s3::S3Client;
use rusoto_sqs::SqsClient;
use std::sync::Arc;

lazy_static! {
    /// AWS Lambda function async invocation.
    pub static ref FLOCK_LAMBDA_ASYNC_CALL: String = "Event".to_string();
    /// AWS Lambda function sync invocation.
    pub static ref FLOCK_LAMBDA_SYNC_CALL: String = "RequestResponse".to_string();
    /// AWS Lambda function maximum error retry.
    pub static ref FLOCK_LAMBDA_MAX_RETRIES: usize = FLOCK_CONF["lambda"]["max_invoke_retries"].parse::<usize>().unwrap();
    /// AWS Lambda function maximum error retry.
    pub static ref FLOCK_LAMBDA_MAX_BACKOFF: u64 = FLOCK_CONF["lambda"]["max_backoff"].parse::<u64>().unwrap();
    /// AWS Lambda function timeout.
    pub static ref FLOCK_LAMBDA_TIMEOUT: i64 = FLOCK_CONF["lambda"]["timeout"].parse::<i64>().unwrap();
    /// AWS Lambda function concurrency.
    pub static ref FLOCK_FUNCTION_CONCURRENCY: usize = FLOCK_CONF["lambda"]["concurrency"].parse::<usize>().unwrap();

    /// Flock sync invocation granularity.
    pub static ref FLOCK_SYNC_GRANULE_SIZE: usize = FLOCK_CONF["lambda"]["sync_granule"].parse::<usize>().unwrap();
    /// Flock async invocation granularity.
    pub static ref FLOCK_ASYNC_GRANULE_SIZE: usize = FLOCK_CONF["lambda"]["async_granule"].parse::<usize>().unwrap();

    /// Flock x86_64 binary S3 key prefix.
    pub static ref FLOCK_S3_X86_64_KEY: String = FLOCK_CONF["s3"]["x86_64_key"].to_string();
    /// Flock Arm_64 binary S3 key prefix.
    pub static ref FLOCK_S3_ARM_64_KEY: String = FLOCK_CONF["s3"]["arm_64_key"].to_string();
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

    /// Flock Empty query plan
    pub static ref FLOCK_EMPTY_PLAN: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(false, Arc::new(Schema::empty())));
    /// Flock data source function name
    pub static ref FLOCK_DATA_SOURCE_FUNC_NAME: String = FLOCK_CONF["flock"]["data_source"].to_string();

    /// Flock target partitions.
    pub static ref FLOCK_TARGET_PARTITIONS: usize = FLOCK_CONF["datafusion"]["target_partitions"].parse::<usize>().unwrap();
}
