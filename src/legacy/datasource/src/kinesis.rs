// Copyright 2020 UMD Database Group. All Rights Reserved.
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

//! AWS Kinesis Data Streams collects streaming data, at scale, for real-time
//! analytics https://aws.amazon.com/kinesis/data-streams/
//!
//! Kinesis stream source

use std::any::Any;
use std::string::String;

use arrow::datatypes::*;
use lambda::error::Result;
use physical_plan::KinesisStreamExec;

use crate::datasource::Statistics;
use crate::datasource::TableProvider;

/// Table-based representation of a Kinesis stream.
#[allow(dead_code)]
pub struct KinesisStreamTable {
    name:       String,
    schema:     SchemaRef,
    statistics: Option<Statistics>,
}

impl KinesisStreamTable {
    /// Attempt to initialize a new `KinesisStreamTable` from a stream name.
    pub fn try_new(stream_name: &str) -> Result<Self> {
        let kinesis_stream_exec = KinesisStreamExec::try_new(stream_name, None, None, 0)?;
        let schema = kinesis_stream_exec.schema();
        Ok(Self {
            name: stream_name.to_string(),
            schema,
            statistics: None,
        })
    }
}

impl TableProvider for KinesisStreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this parquet file.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.statistics.clone()
    }
}
