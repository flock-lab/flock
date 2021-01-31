// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
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

//! Batch processing is the processing of a large volume of data all at once.
//! You can store the preceding reference data as an object in Amazon Simple
//! Storage Service (Amazon S3).  Squirtle reads the Amazon S3 object and
//! creates an in-application reference table that you can query in your
//! application code. In your application code, you write a join query to join
//! the in-application stream with the in-application reference table, to obtain
//! more accurate results.

use super::Query;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use runtime::datasource::DataSource;
use std::any::Any;
use std::sync::Arc;

/// SQL queries in your application code execute over in-application batches.
#[derive(Debug)]
pub struct BatchQuery {
    /// ANSI 2008 SQL standard with extensions.
    /// SQL is a domain-specific language used in programming and designed for
    /// managing data held in a relational database management system, or for
    /// stream processing in a relational data stream management system.
    pub ansi_sql:   String,
    /// A schema that is the skeleton structure that represents the logical view
    /// of streaming data.
    pub schema:     Option<SchemaRef>,
    /// A streaming data source.
    pub datasource: DataSource,
}

impl Query for BatchQuery {
    /// Returns a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns a SQL query.
    fn sql(&self) -> &String {
        &self.ansi_sql
    }

    /// Returns the data schema for a given query.
    fn schema(&self) -> &Option<SchemaRef> {
        &self.schema
    }

    /// Returns the entire physical plan for a given query.
    fn plan(&self) -> Arc<dyn ExecutionPlan> {
        unimplemented!();
    }

    /// Returns the data source for a given query.
    fn datasource(&self) -> &DataSource {
        &self.datasource
    }
}
