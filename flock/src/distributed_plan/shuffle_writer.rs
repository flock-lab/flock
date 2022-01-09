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

//! ShuffleWriterExec represents a section of a query plan that has consistent
//! partitioning and can be executed as one unit with each partition being
//! executed in parallel. The output of each partition is re-partitioned and
//! streamed to disk in Arrow IPC format. Future stages of the query
//! will use the ShuffleReaderExec to read these results.

use std::iter::Iterator;
use std::sync::Arc;
use std::time::Instant;
use std::{any::Any, pin::Pin};

use crate::distributed_plan::memory_stream::MemoryStream;

use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::LambdaExecPlan;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, Statistics,
};
use futures::StreamExt;
use log::info;
use serde::{Deserialize, Serialize};

/// ShuffleWriterExec represents a section of a query plan that has consistent
/// partitioning and can be executed as one unit with each partition being
/// executed in parallel. The output of each partition is re-partitioned and
/// streamed to disk in Arrow IPC format. Future stages of the query
/// will use the ShuffleReaderExec to read these results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleWriterExec {
    /// Unique query stage ID within the job
    stage_id:                    usize,
    /// Physical execution plan for this query stage
    plan:                        Arc<dyn ExecutionPlan>,
    /// Optional shuffle output partitioning
    shuffle_output_partitioning: Option<Partitioning>,
    /// Execution metrics
    metrics:                     ExecutionPlanMetricsSet,
}

impl ShuffleWriterExec {
    /// Create a new shuffle writer
    pub fn try_new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        shuffle_output_partitioning: Option<Partitioning>,
    ) -> Result<Self> {
        Ok(Self {
            stage_id,
            plan,
            shuffle_output_partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Get the Stage ID for this query stage
    #[allow(dead_code)]
    pub fn stage_id(&self) -> usize {
        self.stage_id
    }

    /// Get the true output partitioning
    #[allow(dead_code)]
    pub fn shuffle_output_partitioning(&self) -> Option<&Partitioning> {
        self.shuffle_output_partitioning.as_ref()
    }

    /// Execute the shuffle writer
    pub async fn execute_shuffle_write(&self, input_partition: usize) -> Result<Vec<RecordBatch>> {
        let now = Instant::now();
        let mut batches = vec![];
        let mut stream = self.plan.execute(input_partition).await?;

        match &self.shuffle_output_partitioning {
            None => {
                info!(
                    "Executed partition {} in {} seconds.",
                    input_partition,
                    now.elapsed().as_secs()
                );

                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
                Ok(batches)
            }
            Some(Partitioning::Hash(exprs, n)) => {
                let num_output_partitions = *n;
                let hashes_buf = &mut vec![];
                let random_state = datafusion::ahash::RandomState::with_seeds(0, 0, 0, 0);

                while let Some(result) = stream.next().await {
                    let input_batch = result?;
                    let arrays = exprs
                        .iter()
                        .map(|expr| {
                            Ok(expr
                                .evaluate(&input_batch)?
                                .into_array(input_batch.num_rows()))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    hashes_buf.clear();
                    hashes_buf.resize(arrays[0].len(), 0);
                    // Hash arrays and compute buckets based on number of partitions
                    let hashes = create_hashes(&arrays, &random_state, hashes_buf)?;
                    let mut indices = vec![vec![]; num_output_partitions];
                    for (index, hash) in hashes.iter().enumerate() {
                        indices[(*hash % num_output_partitions as u64) as usize].push(index as u64)
                    }
                    for (_, partition_indices) in indices.into_iter().enumerate() {
                        if partition_indices.is_empty() {
                            continue;
                        }
                        let indices = partition_indices.into();

                        // Produce batches based on indices
                        let columns = input_batch
                            .columns()
                            .iter()
                            .map(|c| {
                                take(c.as_ref(), &indices, None)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))
                            })
                            .collect::<Result<Vec<Arc<dyn Array>>>>()?;
                        batches.push(RecordBatch::try_new(input_batch.schema(), columns)?);
                    }
                }
                Ok(batches)
            }
            _ => Err(DataFusionError::Execution(
                "Invalid shuffle partitioning scheme".to_owned(),
            )),
        }
    }
}

#[async_trait]
impl LambdaExecPlan for ShuffleWriterExec {
    fn feed_batches(&mut self, _partitions: Vec<Vec<RecordBatch>>) {
        unimplemented!();
    }
}

#[async_trait]
#[typetag::serde(name = "shuffle_writer_exec")]
impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        // This operator needs to be executed once for each *input* partition and there
        // isn't really a mechanism yet in DataFusion to support this use case so we
        // report the input partitioning as the output partitioning here. The
        // executor reports output partition meta data back to the scheduler.
        self.plan.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);
        Ok(Arc::new(ShuffleWriterExec::try_new(
            self.stage_id,
            children[0].clone(),
            self.shuffle_output_partitioning.clone(),
        )?))
    }

    async fn execute(
        &self,
        input_partition: usize,
    ) -> Result<Pin<Box<dyn RecordBatchStream + Send + Sync>>> {
        let batches = self.execute_shuffle_write(input_partition).await?;
        let schema = self.plan.schema();
        Ok(Box::pin(MemoryStream::try_new(batches, schema, None)?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ShuffleWriterExec: {:?}",
                    self.shuffle_output_partitioning
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.plan.statistics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, UInt32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::memory::MemoryExec;

    async fn collect_stream(
        stream: &mut Pin<Box<dyn RecordBatchStream + Send + Sync>>,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let input_plan = Arc::new(CoalescePartitionsExec::new(create_input_plan()?));
        let query_stage = ShuffleWriterExec::try_new(
            1,
            input_plan,
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;
        let mut stream = query_stage.execute(0).await?;
        let batches = collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        assert_eq!(4, batches.len());
        let batch = &batches[0];
        assert_eq!(2, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let b_cols = batch.columns()[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert!(b_cols.value(0) == "hello");
        assert!(b_cols.value(1) == "world");

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned() -> Result<()> {
        let input_plan = create_input_plan()?;
        let query_stage = ShuffleWriterExec::try_new(
            1,
            input_plan,
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 2)),
        )?;

        let mut stream = query_stage.execute(0).await?;
        let batches = collect_stream(&mut stream)
            .await
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;

        assert_eq!(2, batches.len());

        let batch = &batches[0];
        assert_eq!(2, batch.num_columns());
        assert_eq!(2, batch.num_rows());

        let batch = &batches[1];
        assert_eq!(2, batch.num_columns());
        assert_eq!(2, batch.num_rows());

        println!("{}", pretty_format_batches(&batches)?);

        Ok(())
    }

    fn create_input_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )?;
        let partition = vec![batch.clone(), batch];
        let partitions = vec![partition.clone(), partition];
        Ok(Arc::new(MemoryExec::try_new(&partitions, schema, None)?))
    }
}
