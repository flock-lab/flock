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

//! Functions produced by the Flock API often hold state in various forms:
//!
//! - Windows gather elements or aggregates until they are triggered;
//! - Transformation functions may use the key/value state interface to store
//!   values;
//! - Transformation functions may implement the CheckpointedFunction interface
//!   to make their local variables fault tolerant;
//!
//! When checkpointing is activated, such state is persisted upon checkpoints to
//! guard against data loss and recover consistently. How the state is
//! represented internally, and how and where it is persisted upon checkpoints
//! depends on the chosen State Backend.
//!
//! Out of the box, Flock bundles these state backends:
//!
//! - `HashMapStateBackend`: holds data internally as objects in the function's
//!   global memory. Key/value state and window operators hold hash tables that
//!   store the values, triggers, etc. This backend does not provide any
//!   guarantees on fault tolerance. This backend is always available, which
//!   doesn't need to be specified.
//!
//! - `S3StateBackend`: holds in-flight data in AWS S3 buckets. Unlike the Arena
//!   backend, data is stored as serialized byte arrays, or CSV files or Parquet
//!   files. This backend provides fault tolerance.
//!
//! - `EfsStateBackend`: holds in-flight data in Amazon Elastic File System
//!   (EFS). Unlike the Arena backend, data is stored as serialized byte arrays,
//!   or CSV files or Parquet files. This backend provides fault tolerance.
//!
//! If nothing else is configured, the system will use the HashMapStateBackend.
//!
//! Note that S3StateBackend and EfsStateBackend allow keeping very large state,
//! compared to the HashMapStateBackend that keeps state in memory. This also
//! means, however, that the maximum throughput that can be achieved will be
//! lower with this state backend. All reads/writes from/to this backend have to
//! go through de-/serialization to retrieve/store the state objects, which is
//! also more expensive than always working with the in-memory representation as
//! the memory-based backends are doing.
//!
//! When deciding between state backends, it is a choice between performance,
//! scalability and fault tolerance. HashMapStateBackend is very fast as each
//! state access and update operates on objects in the memory; however, state
//! size is limited by available memory in the functions. On the other hand,
//! EFS and S3 can scale several orders of magnitude better than it. However,
//! each state access and update requires (de-)serialization and potentially
//! reading from remote SSD which leads to average performance that is an order
//! of magnitude slower than the memory state backends.

mod s3;
pub use s3::S3StateBackend;

mod efs;
pub use efs::EfsStateBackend;

use crate::error::Result;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::Debug;

/// The state backend trait defines the interface for state backends.
#[async_trait]
#[typetag::serde(tag = "state_backend")]
pub trait StateBackend: Debug + Send + Sync {
    /// The type of the state backend.
    fn name(&self) -> String;
    /// Returns the state backend as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Returns the value as an mutable Any to allow for downcasts to specific
    /// implementations.
    fn as_mut_any(&mut self) -> &mut dyn Any;
    /// Writes record batches to the state backend.
    async fn write(&self, bucket: &str, key: &str, batches: Vec<RecordBatch>) -> Result<()>;
}

/// The default state backend.
///
/// Note: Currently, the functionalities of the HashMapStateBackend are
/// implemented by the `Arena` module. `HashMapStateBackend` is an unified
/// abstraction we will use to encapsulate the Arena module.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct HashMapStateBackend {}

#[async_trait]
#[typetag::serde(name = "hashmap_state_backend")]
impl StateBackend for HashMapStateBackend {
    fn name(&self) -> String {
        "HashMapStateBackend".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    async fn write(&self, bucket: &str, key: &str, batches: Vec<RecordBatch>) -> Result<()> {
        Ok(())
    }
}

impl HashMapStateBackend {
    /// Creates a new HashMapStateBackend.
    pub fn new() -> Self {
        Self {}
    }
}
