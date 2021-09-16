// Copyright (c) 2020 UMD Database Group. All Rights Reserved.
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
// Only bring in dependencies for the repl when the cli feature is enabled.

//! A Query API to associate front-end CLI with back-end function generation and
//! continuous deployment.

#![warn(missing_docs)]
// Clippy lints, some should be disabled incrementally
#![allow(
    clippy::float_cmp,
    clippy::module_inception,
    clippy::new_without_default,
    clippy::ptr_arg,
    clippy::type_complexity,
    clippy::wrong_self_convention
)]

use crate::datasource::DataSource;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// A `Query` trait to decouple CLI and back-end cloud function generation.
pub trait Query: Debug + Send + Sync {
    /// Returns the query as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Returns a SQL query.
    fn sql(&self) -> &String;
    /// Returns the data schema for a given query.
    fn schema(&self) -> &SchemaRef;
    /// Returns the entire physical plan for a given query.
    fn plan(&self) -> &Arc<dyn ExecutionPlan>;
    /// Returns the data source for a given query.
    fn datasource(&self) -> &DataSource;
}

pub mod batch;
pub mod stream;

pub use batch::BatchQuery;
pub use stream::{Schedule, StreamQuery, StreamWindow};
