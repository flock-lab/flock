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

//! The `gcp` crate contains the GCP-specific parts of the `flock-function`
//! library.

use crate::launcher::Launcher;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use flock::error::Result;
use flock::query::Query;

/// GCPLauncher defines the interface for deploying and executing
/// queries on GCP Functions.
pub struct GCPLauncher {}

#[async_trait]
impl Launcher for GCPLauncher {
    async fn new<T>(_: &Query<T>) -> Result<Self>
    where
        Self: Sized,
        T: AsRef<str> + Send + Sync + 'static,
    {
        Ok(GCPLauncher {})
    }

    fn deploy(&mut self) -> Result<()> {
        unimplemented!();
    }

    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        unimplemented!();
    }
}