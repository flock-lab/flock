// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
