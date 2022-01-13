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

//! The `aws` crate contains the AWS-specific parts of the `flock-function`
//! library.

extern crate daggy;
use crate::launcher::Launcher;
use async_trait::async_trait;
use daggy::NodeIndex;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use flock::configs::*;
use flock::datasink::DataSinkType;
use flock::distributed_plan::DistributedPlanner;
use flock::distributed_plan::QueryDag;
use flock::error::Result;
use flock::query::Query;
use flock::runtime::context::*;
use flock::runtime::plan::CloudExecutionPlan;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// AwsLambdaLauncher defines the interface for deploying and executing
/// queries on AWS Lambda.
#[derive(Debug)]
pub struct AwsLambdaLauncher {
    /// The first component of the function name.
    pub query_code: Option<String>,
    /// The DAG of a given query.
    pub dag:        QueryDag,
    /// The data sink type of a given query.
    pub sink_type:  DataSinkType,
    /// The entire execution plan. This can be used to execute the query
    /// in a single Lambda function.
    pub plan:       Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl Launcher for AwsLambdaLauncher {
    async fn new<T>(query: &Query<T>) -> Result<Self>
    where
        Self: Sized,
        T: AsRef<str> + Send + Sync + 'static,
    {
        let plan = query.plan()?;
        let sink_type = query.datasink();

        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan.clone()).await?;

        let mut query_code = query.query_code();
        if query_code.is_none() {
            let mut hasher = DefaultHasher::new();
            query.sql().hash(&mut hasher);
            query_code = Some(hasher.finish().to_string());
        }

        Ok(AwsLambdaLauncher {
            plan,
            dag,
            sink_type,
            query_code,
        })
    }

    fn deploy(&mut self) -> Result<()> {
        self.create_cloud_contexts()?;
        self.create_cloud_functions()?;
        Ok(())
    }

    /// Invoke the data source function for the given query.
    ///
    /// The **BIG** innovation is that we don't need a coordinator or scheduler
    /// to coordinate the execution of the query stage, monitoring the progress
    /// of the query, and reporting the results, which is a core part of the
    /// traditional distributed query engine. Instead, each lambda function
    /// is responsible for executing the query stage for itself, and forwards
    /// the results to the next lambda function. This greatly simplifies the
    /// code size and complexity of the distributed query engine. Meanwhile, the
    /// latency is significantly reduced.
    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        unimplemented!();
    }
}

impl AwsLambdaLauncher {
    /// Initialize the query code for the query.
    pub fn set_query_code<T>(&mut self, query: &Query<T>)
    where
        T: AsRef<str> + Send + Sync + 'static,
    {
        self.query_code = query.query_code();
        if self.query_code.is_none() {
            let mut hasher = DefaultHasher::new();
            query.sql().hash(&mut hasher);
            self.query_code = Some(hasher.finish().to_string());
        }
    }

    /// Create the cloud contexts for the query.
    ///
    /// This function creates a new context for each query stage in the DAG.
    fn create_cloud_contexts(&mut self) -> Result<()> {
        let dag = &mut self.dag;
        let count = dag.node_count();
        assert!(count < 100);

        let concurrency = (0..count)
            .map(|i| dag.get_node(NodeIndex::new(i)).unwrap().concurrency)
            .collect::<Vec<usize>>();

        (0..count).rev().for_each(|i| {
            let node = dag.get_node_mut(NodeIndex::new(i)).unwrap();
            let query_code = self.query_code.as_ref().expect("query code not set");

            let next = if i == 0 {
                CloudFunction::Sink(self.sink_type.clone())
            } else if concurrency[i - 1 /* parent */] == 1 {
                CloudFunction::Group((
                    format!("{}-{:02}", query_code, count - 1 - (i - 1)),
                    *FLOCK_FUNCTION_CONCURRENCY,
                ))
            } else {
                CloudFunction::Lambda(format!("{}-{:02}", query_code, count - 1 - (i - 1)))
            };

            let ctx = ExecutionContext {
                plan: CloudExecutionPlan::new(node.stage.clone(), None),
                name: format!("{}-{:02}", query_code, count - 1 - i),
                next,
            };

            node.context = Some(ctx);
        });

        Ok(())
    }

    /// Create the cloud functions for the query.
    fn create_cloud_functions(&self) -> Result<()> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use flock::assert_batches_eq;
    use flock::datasource::DataSource;
    use flock::query::Table;
    use std::sync::Arc;

    fn init_query() -> Result<Query<&'static str>> {
        let table1 = "t1";
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let table2 = "t2";
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("d", DataType::Int32, false),
        ]));

        let sql = concat!(
            "SELECT a, b, d ",
            "FROM t1 JOIN t2 ON a = c ",
            "ORDER BY a ASC ",
            "LIMIT 3"
        );

        Ok(Query::new(
            sql,
            vec![Table(table1, schema1), Table(table2, schema2)],
            DataSource::Memory,
            DataSinkType::Blackhole,
            None,
        ))
    }

    #[tokio::test]
    async fn aws_launcher_create_context() -> Result<()> {
        let query = init_query()?;
        let mut launcher = AwsLambdaLauncher::new(&query).await?;
        println!("SQL: {}", query.sql());
        println!("Query Code: {}\n", launcher.query_code.as_ref().unwrap());
        launcher.create_cloud_contexts()?;

        let stages = launcher.dag.get_all_stages();
        for (i, stage) in stages.iter().enumerate() {
            println!("=== Query Stage {:02} ===", i);
            println!(
                "{:#?}\nFunction Concurrency: {}\n",
                stage.context.as_ref().unwrap(),
                stage.concurrency
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn aws_launcher_execute_stages() -> Result<()> {
        let query = init_query()?;
        let mut launcher = AwsLambdaLauncher::new(&query).await?;
        println!("SQL: {}", query.sql());
        println!("Query Code: {}\n", launcher.query_code.as_ref().unwrap());
        launcher.create_cloud_contexts()?;

        // define data.
        let batch1 = RecordBatch::try_new(
            query.tables()[0].1.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;
        // define data.
        let batch2 = RecordBatch::try_new(
            query.tables()[1].1.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;

        let stages = launcher.dag.get_all_stages();
        let mut input = vec![vec![vec![batch1]], vec![vec![batch2]]];
        for (i, stage) in stages.into_iter().enumerate() {
            println!("=== Query Stage {:02} ===", i);
            let mut ctx = stage.context.clone().unwrap();
            ctx.feed_data_sources(&input).await?;
            input = ctx
                .execute()
                .await?
                .into_iter()
                .map(|batches| vec![batches])
                .collect();
        }

        let expected = vec![
            "+---+----+----+",
            "| a | b  | d  |",
            "+---+----+----+",
            "| a | 1  | 1  |",
            "| b | 10 | 10 |",
            "| c | 10 | 10 |",
            "+---+----+----+",
        ];

        assert_batches_eq!(&expected, &input[0][0]);

        Ok(())
    }
}
