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

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use flock::error::Result;
use flock::query::Query;

/// Launcher is a trait that defines the interface for deploying and executing
/// queries on cloud function services.
#[async_trait]
pub trait Launcher {
    /// Create a new launcher.
    ///
    /// # Arguments
    /// `query` - The query to be deployed.
    async fn new<T>(query: &Query<T>) -> Result<Self>
    where
        Self: Sized,
        T: AsRef<str> + Send + Sync + 'static;

    /// Deploy a query to a specific cloud function service.
    /// It is called before the query is executed.
    fn deploy(&mut self) -> Result<()>;

    /// Execute a query on a specific cloud function service.
    /// It is called after the query is deployed.
    ///
    /// # Returns
    /// A vector of record batches.
    async fn execute(&self) -> Result<Vec<RecordBatch>>;
}
