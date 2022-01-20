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
use crate::state::*;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::Debug;
use std::sync::Arc;

/// TableName name for the query.
pub type TableName = String;

/// The relational table represents the logical view of incoming data.
#[derive(Clone)]
pub struct Table(pub TableName, pub SchemaRef);

impl Table {
    /// Creates a new table.
    pub fn new<T>(name: T, schema: SchemaRef) -> Self
    where
        T: Into<String>,
    {
        Table(name.into(), schema)
    }
}

impl Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table({:?}, {:?})", self.0, self.1)
    }
}

/// The stream type for the query.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum StreamType {
    /// Regular stream.
    Regular,
    /// NEXMark Benchmark.
    NEXMarkBench,
    /// Yahoo! Streaming Benchmark.
    YSBBench,
}

/// The query type for the query.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum QueryType {
    /// OLAP workload.
    OLAP,
    /// Streaming workload.
    Streaming(StreamType),
}

impl Default for QueryType {
    fn default() -> Self {
        QueryType::Streaming(StreamType::NEXMarkBench)
    }
}

/// SQL queries in your application code execute over in-application batches.
#[derive(Debug, Clone)]
pub struct Query {
    /// ANSI 2008 SQL standard with extensions.
    /// SQL is a domain-specific language used in programming and designed for
    /// managing data held in a relational database management system, or for
    /// stream processing in a relational data stream management system.
    pub sql:           String,
    /// Table defines the incoming data stream. Each table that is the skeleton
    /// structure that represents the logical view of streaming data.
    pub tables:        Vec<Table>,
    /// A streaming data source.
    pub datasource:    DataSource,
    /// A sink for the output of the query.
    pub datasink:      DataSinkType,
    /// This is used to specify the function name for benchmarking. Otherwise,
    /// the function name is generated from `sql`. To make the debugging easier,
    /// we define human-readable function name for benchmarking.
    pub query_code:    Option<String>,
    /// The query type.
    pub query_type:    QueryType,
    /// The state backend to use.
    pub state_backend: Arc<dyn StateBackend>,
}

impl Default for Query {
    fn default() -> Self {
        Query {
            sql:           String::new(),
            tables:        vec![],
            datasource:    DataSource::default(),
            datasink:      DataSinkType::default(),
            query_code:    None,
            query_type:    QueryType::default(),
            state_backend: Arc::new(HashMapStateBackend::new()),
        }
    }
}

impl Query {
    /// Creates a new query.
    pub fn new<T>(
        sql: T,
        tables: Vec<Table>,
        datasource: DataSource,
        datasink: DataSinkType,
        query_code: Option<T>,
        query_type: QueryType,
        state_backend: Arc<dyn StateBackend>,
    ) -> Self
    where
        T: Into<String>,
    {
        Self {
            sql: sql.into(),
            tables,
            datasource,
            datasink,
            query_code: query_code.map(|x| x.into()),
            query_type,
            state_backend,
        }
    }

    /// Returns a SQL query.
    pub fn sql(&self) -> String {
        self.sql.to_owned()
    }

    /// Returns the data schema for a given query.
    pub fn tables(&self) -> &Vec<Table> {
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

    /// Returns the state backend for a given query.
    /// The state backend is used to store the state of the query.
    pub fn state_backend(&self) -> Arc<dyn StateBackend> {
        self.state_backend.clone()
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
        self.query_code.to_owned()
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
