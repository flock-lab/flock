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

//! This crate responsibles for executing queries on the local machine.

use crate::launcher::Launcher;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use flock::error::{FlockError, Result};
use flock::query::Query;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

/// LocalLauncher executes the query locally.
pub struct LocalLauncher {
    /// The physical plan of the query.
    execution_plan: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl Launcher for LocalLauncher {
    fn new<T>(query: &Query<T>) -> Self
    where
        T: AsRef<str> + Send + Sync + 'static,
    {
        LocalLauncher {
            execution_plan: query.plan().unwrap(),
        }
    }

    fn deploy(&self) -> Result<()> {
        Err(FlockError::Internal(
            "Local execution doesn't require a deployment.".to_owned(),
        ))
    }

    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        collect(self.execution_plan.clone())
            .await
            .map_err(|e| FlockError::Execution(e.to_string()))
    }
}

impl LocalLauncher {
    /// Compare two execution plans' schemas.
    /// Returns true if they are belong to the same plan node.
    /// 
    /// # Arguments
    /// * `schema1` - The first schema.
    /// * `schema2` - The second schema.
    /// 
    /// # Returns
    /// * `true` - If the schemas belong to the same plan node.
    fn compare_schema(schema1: SchemaRef, schema2: SchemaRef) -> bool {
        let (superset, subset) = if schema1.fields().len() >= schema2.fields().len() {
            (schema1, schema2)
        } else {
            (schema2, schema1)
        };

        let fields = superset
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<HashSet<_>>();

        subset.fields().iter().all(|f| fields.contains(&f.name()))
    }

    /// Feeds the query with data.
    /// 
    /// # Arguments
    /// * `sources` - A list of data sources.
    pub fn feed_data_sources(&mut self, sources: &[Vec<Vec<RecordBatch>>]) {
        // Breadth-first search
        let mut queue = VecDeque::new();
        queue.push_front(self.execution_plan.clone());

        while !queue.is_empty() {
            let mut p = queue.pop_front().unwrap();
            if p.children().is_empty() {
                for partition in sources {
                    if LocalLauncher::compare_schema(p.schema(), partition[0][0].schema()) {
                        unsafe {
                            Arc::get_mut_unchecked(&mut p)
                                .as_mut_any()
                                .downcast_mut::<MemoryExec>()
                                .unwrap()
                                .set_partitions(partition);
                        }
                        break;
                    }
                }
            }

            p.children()
                .iter()
                .enumerate()
                .for_each(|(i, _)| queue.push_back(p.children()[i].clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use flock::assert_batches_eq;
    use flock::datasource::DataSource;
    use flock::query::Table;

    #[tokio::test]
    async fn version_check() -> Result<()> {
        let manifest = cargo_toml::Manifest::from_str(include_str!("../Cargo.toml")).unwrap();
        assert_eq!(env!("CARGO_PKG_VERSION"), manifest.package.unwrap().version);
        Ok(())
    }

    #[tokio::test]
    async fn local_launcher() -> Result<()> {
        let table_name = "test_table";
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Float64, false),
            Field::new("c3", DataType::Utf8, false),
            Field::new("c4", DataType::UInt64, false),
            Field::new("c5", DataType::Utf8, false),
            Field::new("neg", DataType::Int64, false),
        ]));
        let sql = "SELECT MIN(c1), AVG(c4), COUNT(c3) FROM test_table";
        let query = Query::new(
            sql,
            vec![Table(table_name, schema.clone())],
            DataSource::Memory,
        );

        let mut launcher = LocalLauncher::new(&query);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![90, 90, 91, 101, 92, 102, 93, 103])),
                Arc::new(Float64Array::from(vec![
                    92.1, 93.2, 95.3, 96.4, 98.5, 99.6, 100.7, 101.8,
                ])),
                Arc::new(StringArray::from(vec![
                    "a", "a", "d", "b", "b", "d", "c", "c",
                ])),
                Arc::new(UInt64Array::from(vec![33, 1, 54, 33, 12, 75, 2, 87])),
                Arc::new(StringArray::from(vec![
                    "rapport",
                    "pedantic",
                    "mimesis",
                    "haptic",
                    "baksheesh",
                    "amok",
                    "devious",
                    "c",
                ])),
                Arc::new(Int64Array::from(vec![
                    -90, -90, -91, -101, -92, -102, -93, -103,
                ])),
            ],
        )?;

        launcher.feed_data_sources(&[vec![vec![batch]]]);
        let batches = launcher.execute().await?;

        let expected = vec![
            "+--------------------+--------------------+----------------------+",
            "| MIN(test_table.c1) | AVG(test_table.c4) | COUNT(test_table.c3) |",
            "+--------------------+--------------------+----------------------+",
            "| 90                 | 37.125             | 8                    |",
            "+--------------------+--------------------+----------------------+",
        ];

        assert_batches_eq!(&expected, &batches);

        Ok(())
    }
}
