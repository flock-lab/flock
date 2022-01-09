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

//! This is copied from DataFusion because it is declared as `pub(crate)`. See
//! https://issues.apache.org/jira/browse/ARROW-11276.

use std::task::{Context, Poll};

use datafusion::arrow::{datatypes::SchemaRef, error::Result, record_batch::RecordBatch};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;

/// Iterator over batches
pub struct MemoryStream {
    /// Vector of record batches
    data:       Vec<RecordBatch>,
    /// Schema representing the data
    schema:     SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index:      usize,
}

impl MemoryStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            schema,
            projection,
            index: 0,
        })
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;

            let batch = &self.data[self.index - 1];

            // apply projection
            match &self.projection {
                Some(columns) => Some(RecordBatch::try_new(
                    self.schema.clone(),
                    columns.iter().map(|i| batch.column(*i).clone()).collect(),
                )),
                None => Some(Ok(batch.clone())),
            }
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for MemoryStream {
    /// Get the schema

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
