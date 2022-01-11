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

//! The query interface is responsible for bringing the underlying query
//! implementation (streaming and OLAP) into the system backend.

use crate::datasource::DataSource;
use crate::error::{FlockError, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

type TableName = String;

/// SQL queries in your application code execute over in-application batches.
#[derive(Debug, Default)]
pub struct Query {
    /// ANSI 2008 SQL standard with extensions.
    /// SQL is a domain-specific language used in programming and designed for
    /// managing data held in a relational database management system, or for
    /// stream processing in a relational data stream management system.
    pub sql:        String,
    /// Table defines the incoming data stream. each schema that is the skeleton
    /// structure that represents the logical view of streaming data.
    pub tables:     Vec<(TableName, SchemaRef)>,
    /// A streaming data source.
    pub datasource: DataSource,
}

impl Query {
    /// Creates a new query.
    pub fn new(sql: String, tables: Vec<(TableName, SchemaRef)>, datasource: DataSource) -> Self {
        Self {
            sql,
            tables,
            datasource,
        }
    }

    /// Returns a SQL query.
    pub fn sql(&self) -> String {
        self.sql.to_owned()
    }

    /// Returns the data schema for a given query.
    pub fn tables(&self) -> &Vec<(TableName, SchemaRef)> {
        &self.tables
    }

    /// Returns the data source for a given query.
    pub fn datasource(&self) -> DataSource {
        self.datasource.clone()
    }

    /// Returns the physical plan for a given query.
    pub fn plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = ExecutionContext::new();
        for (name, schema) in &self.tables {
            let table = MemTable::try_new(
                schema.clone(),
                vec![vec![RecordBatch::new_empty(schema.clone())]],
            )?;
            ctx.register_table(name.as_str(), Arc::new(table))?;
        }
        let plan = ctx.create_logical_plan(&self.sql)?;
        let plan = ctx.optimize(&plan)?;
        futures::executor::block_on(ctx.create_physical_plan(&plan))
            .map_err(|e| FlockError::Internal(e.to_string()))
    }
}
