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

pub mod aws;
pub mod azure;
pub mod gcp;
pub mod local;
pub use aws::AwsLambdaLauncher;
pub use local::LocalLauncher;

use crate::error::Result;
use crate::query::Query;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;

/// The execution model for the query.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// In centralized mode, the query is executed on a single cloud
    /// function.
    Centralized,
    /// In distributed mode, the query is represented as a DAG and
    /// executed on multiple cloud functions.
    Distributed,
}

/// Launcher is a trait that defines the interface for deploying and executing
/// queries on cloud function services.
#[async_trait]
pub trait Launcher {
    /// Create a new launcher.
    ///
    /// # Arguments
    /// `query` - The query to be deployed.
    async fn new(query: &Query) -> Result<Self>
    where
        Self: Sized;

    /// Deploy a query to a specific cloud function service.
    /// It is called before the query is executed.
    fn deploy(&mut self) -> Result<()>;

    /// Execute a query on a specific cloud function service.
    /// It is called after the query is deployed.
    ///
    /// # Arguments
    /// `mode` - The execution mode of the query.
    ///
    /// # Returns
    /// A vector of record batches.
    async fn execute(&self, mode: ExecutionMode) -> Result<Vec<RecordBatch>>;
}
