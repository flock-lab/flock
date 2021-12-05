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

//! Batch processing is the processing of a large volume of data all at once.
//! You can store the preceding reference data as an object in Amazon Simple
//! Storage Service (Amazon S3).  Flock reads the Amazon S3 object and
//! creates an in-application reference table that you can query in your
//! application code. In your application code, you write a join query to join
//! the in-application stream with the in-application reference table, to obtain
//! more accurate results.

use super::Query;
use crate::datasource::DataSource;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
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
    pub schema:     SchemaRef,
    /// A streaming data source.
    pub datasource: DataSource,
    /// The execution plan.
    pub plan:       Arc<dyn ExecutionPlan>,
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
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the entire physical plan for a given query.
    fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }

    /// Returns the data source for a given query.
    fn datasource(&self) -> &DataSource {
        &self.datasource
    }
}
