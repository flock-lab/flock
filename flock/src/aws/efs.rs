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

//! This crate contains all wrapped functions of the AWS EFS services.

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

//! This crate contains all wrapped functions of the AWS Lambda services.

use crate::configs::*;
use crate::error::{FlockError, Result};
use rusoto_core::RusotoError;
use rusoto_efs::{
    CreateAccessPointError, CreateAccessPointRequest, CreateFileSystemError,
    CreateFileSystemRequest, CreateMountTargetError, CreateMountTargetRequest, CreationInfo,
    DescribeAccessPointsRequest, DescribeFileSystemsRequest, Efs, PosixUser, RootDirectory,
};

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
pub async fn create() -> Result<String> {
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
pub async fn discribe() -> Result<String> {
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
pub async fn create_access_point(file_system_id: &str) -> Result<String> {
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
pub async fn describe_access_point(
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
