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

use flock::error::{FlockError, Result};
use flock::query::Query;

/// Launcher is a trait that defines the interface for deploying and executing
/// queries on cloud function services.
pub trait Launcher {
    /// Create a new launcher.
    ///
    /// # Arguments
    /// `flow` - The query flow to be deployed.
    fn new(query: &Query) -> Self;

    /// Deploy a query to a specific cloud function service.
    /// It is called before the query is executed.
    fn deploy(&self) -> Result<()>;

    /// Execute a query on a specific cloud function service.
    /// It is called after the query is deployed.
    fn execute(&self) -> Result<()>;
}

/// LocalLauncher executes the query locally.
pub struct LocalLauncher {}

impl Launcher for LocalLauncher {
    fn new(_query: &Query) -> Self {
        LocalLauncher {}
    }

    fn deploy(&self) -> Result<()> {
        Err(FlockError::Internal(
            "Local execution doesn't require a deployment.".to_owned(),
        ))
    }

    fn execute(&self) -> Result<()> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn version_check() -> Result<()> {
        let manifest = cargo_toml::Manifest::from_str(include_str!("../Cargo.toml")).unwrap();
        assert_eq!(env!("CARGO_PKG_VERSION"), manifest.package.unwrap().version);
        Ok(())
    }
}
