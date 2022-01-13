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

use crate::datasink::DataSinkType;
use crate::datasource::DataSource;
use crate::error::{FlockError, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::Debug;
use std::sync::Arc;

/// The relational table represents the logical view of incoming data.
#[derive(Clone)]
pub struct Table<T: AsRef<str>>(pub T, pub SchemaRef);

impl<T: AsRef<str>> Table<T> {
    /// Creates a new table.
    pub fn new(name: T, schema: SchemaRef) -> Self {
        Table(name, schema)
    }
}

impl<T: AsRef<str>> Debug for Table<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table({:?}, {:?})", self.0.as_ref(), self.1)
    }
}

/// SQL queries in your application code execute over in-application batches.
#[derive(Debug, Default, Clone)]
pub struct Query<T: AsRef<str>> {
    /// ANSI 2008 SQL standard with extensions.
    /// SQL is a domain-specific language used in programming and designed for
    /// managing data held in a relational database management system, or for
    /// stream processing in a relational data stream management system.
    pub sql:        T,
    /// Table defines the incoming data stream. Each table that is the skeleton
    /// structure that represents the logical view of streaming data.
    pub tables:     Vec<Table<T>>,
    /// A streaming data source.
    pub datasource: DataSource,
    /// A sink for the output of the query.
    pub datasink:   DataSinkType,
    /// This is used to specify the function name for benchmarking. Otherwise,
    /// the function name is generated from `sql`. To make the debugging easier,
    /// we define human-readable function name for benchmarking.
    pub query_code: Option<T>,
}

impl<T: AsRef<str>> Query<T> {
    /// Creates a new query.
    pub fn new(
        sql: T,
        tables: Vec<Table<T>>,
        datasource: DataSource,
        datasink: DataSinkType,
        query_code: Option<T>,
    ) -> Self {
        Self {
            sql,
            tables,
            datasource,
            datasink,
            query_code,
        }
    }

    /// Returns a SQL query.
    pub fn sql(&self) -> String {
        self.sql.as_ref().to_owned()
    }

    /// Returns the data schema for a given query.
    pub fn tables(&self) -> &Vec<Table<T>> {
        &self.tables
    }

    /// Returns the data source for a given query.
    pub fn datasource(&self) -> DataSource {
        self.datasource.clone()
    }

    /// Returns the data sink for a given query.
    pub fn datasink(&self) -> DataSinkType {
        self.datasink.clone()
    }

    /// Returns the physical plan for a given query.
    pub fn plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = ExecutionContext::new();
        for table in &self.tables {
            let mem_table = MemTable::try_new(
                table.1.clone(),
                vec![vec![RecordBatch::new_empty(table.1.clone())]],
            )?;
            ctx.register_table(table.0.as_ref(), Arc::new(mem_table))?;
        }

        let plan = ctx.create_logical_plan(self.sql.as_ref())?;
        let plan = ctx.optimize(&plan)?;

        futures::executor::block_on(ctx.create_physical_plan(&plan))
            .map_err(|e| FlockError::Internal(e.to_string()))
    }

    /// Returns the query code for a given query.
    pub fn query_code(&self) -> Option<String> {
        self.query_code.as_ref().map(|s| s.as_ref().to_owned())
    }

    /// Returns the physical plan for a given query.
    ///
    /// # Arguments
    /// * `shuffle_partitions` - The number of output partitions to shuffle the
    ///   data into.
    pub fn plan_with_partitions(
        &self,
        shuffle_partitions: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = ExecutionConfig::new().with_target_partitions(shuffle_partitions);
        let mut ctx = ExecutionContext::with_config(config);
        for table in &self.tables {
            let mem_table = MemTable::try_new(
                table.1.clone(),
                vec![vec![RecordBatch::new_empty(table.1.clone())]],
            )?;
            ctx.register_table(table.0.as_ref(), Arc::new(mem_table))?;
        }

        let plan = ctx.create_logical_plan(self.sql.as_ref())?;
        let plan = ctx.optimize(&plan)?;

        futures::executor::block_on(ctx.create_physical_plan(&plan))
            .map_err(|e| FlockError::Internal(e.to_string()))
    }
}
