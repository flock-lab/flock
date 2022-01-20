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

//! This crate responsibles for executing queries on GCP Functions.

use crate::error::Result;
use crate::launcher::{ExecutionMode, Launcher};
use crate::query::Query;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;

/// GCPLauncher defines the interface for deploying and executing
/// queries on GCP Functions.
pub struct GCPLauncher {}

#[async_trait]
impl Launcher for GCPLauncher {
    async fn new(_: &Query) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(GCPLauncher {})
    }

    fn deploy(&mut self) -> Result<()> {
        unimplemented!();
    }

    async fn execute(&self, _: ExecutionMode) -> Result<Vec<RecordBatch>> {
        unimplemented!();
    }
}
