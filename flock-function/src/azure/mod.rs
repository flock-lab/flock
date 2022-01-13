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

//! The `azure` crate contains the Azure-specific parts of the `flock-function`
//! library.

use crate::launcher::{ExecutionMode, Launcher};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use flock::error::Result;
use flock::query::Query;

/// AzureLauncher defines the interface for deploying and executing
/// queries on Azure Functions.
pub struct AzureLauncher {}

#[async_trait]
impl Launcher for AzureLauncher {
    async fn new<T>(_: &Query<T>) -> Result<Self>
    where
        Self: Sized,
        T: AsRef<str> + Send + Sync + 'static,
    {
        Ok(AzureLauncher {})
    }

    fn deploy(&mut self) -> Result<()> {
        unimplemented!();
    }

    async fn execute(&self, _: ExecutionMode) -> Result<Vec<RecordBatch>> {
        unimplemented!();
    }
}
