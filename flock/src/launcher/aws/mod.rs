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

//! This crate responsibles for executing queries on AWS Lambda Functions.

extern crate daggy;
use crate::configs::*;
use crate::datasink::DataSinkType;
use crate::distributed_plan::DistributedPlanner;
use crate::distributed_plan::QueryDag;
use crate::error::Result;
use crate::launcher::{ExecutionMode, Launcher};
use crate::query::Query;
use crate::runtime::context::*;
use crate::runtime::plan::CloudExecutionPlan;
use async_trait::async_trait;
use daggy::NodeIndex;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;
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
    async fn execute(&self, _: ExecutionMode) -> Result<Vec<RecordBatch>> {
        unimplemented!();
    }
}

impl AwsLambdaLauncher {
    /// Create a new `AwsLambdaLauncher` instance.
    pub async fn try_new<T>(
        query_code: T,
        plan: Arc<dyn ExecutionPlan>,
        sink_type: DataSinkType,
    ) -> Result<Self>
    where
        T: Into<String>,
    {
        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan.clone()).await?;
        Ok(AwsLambdaLauncher {
            query_code: Some(query_code.into()),
            plan,
            dag,
            sink_type,
        })
    }

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
    pub fn create_cloud_contexts(&mut self) -> Result<()> {
        debug!("Creating cloud contexts for both central and distributed query processing.");

        // Creates the cloud contexts for the distributed mode
        {
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
        }

        // Creates the cloud contexts for centralized mode
        {
            let query_code = self.query_code.as_ref().expect("query code not set");
            let _data_source_ctx = ExecutionContext {
                plan: CloudExecutionPlan::new(vec![FLOCK_EMPTY_PLAN.clone()], None),
                name: FLOCK_DATA_SOURCE_FUNC_NAME.clone(),
                next: CloudFunction::Group((
                    format!("{}-{:02}", query_code, 0),
                    *FLOCK_FUNCTION_CONCURRENCY,
                )),
            };
            let _worker_ctx = ExecutionContext {
                // TODO: add option to store the execution plan in S3.
                plan: CloudExecutionPlan::new(vec![self.plan.clone()], None),
                name: format!("{}-{:02}", query_code, 0),
                next: CloudFunction::Sink(self.sink_type.clone()),
            };
        }

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

    use crate::assert_batches_eq;
    use crate::assert_batches_sorted_eq;
    use crate::datasource::nexmark::event::{Auction, Person};
    use crate::datasource::nexmark::NEXMarkSource;
    use crate::datasource::DataSource;
    use crate::launcher::LocalLauncher;
    use crate::query::Table;
    use crate::query::{QueryType, StreamType};
    use crate::stream::Window;
    use crate::transmute::event_bytes_to_batch;
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use indoc::indoc;
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
            QueryType::OLAP,
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

    #[tokio::test]
    async fn aws_launcher_nexmark_q3_dist_hash_join() -> Result<()> {
        let auction_schema = Arc::new(Auction::schema());
        let person_schema = Arc::new(Person::schema());

        let query = Query::new(
            indoc! {"
            SELECT  name,
                    city,
                    state,
                    a_id
            FROM    auction
                    INNER JOIN person
                            ON seller = p_id
            WHERE  category = 10
                    AND ( state = 'or'
                            OR state = 'id'
                            OR state = 'ca' );
            "},
            vec![
                Table("auction", auction_schema.clone()),
                Table("person", person_schema.clone()),
            ],
            DataSource::Memory,
            DataSinkType::Blackhole,
            None,
            QueryType::Streaming(StreamType::NEXMarkBench),
        );

        let mut launcher = AwsLambdaLauncher::new(&query).await?;
        println!("SQL: {}", query.sql());
        println!("Query Code: {}\n", launcher.query_code.as_ref().unwrap());
        launcher.create_cloud_contexts()?;

        // Generate events.
        let seconds = 1;
        let threads = 1;
        let event_per_second = 10_000;
        let nexmark_source =
            NEXMarkSource::new(seconds, threads, event_per_second, Window::ElementWise);
        let stream = nexmark_source.generate_data()?;

        let (events, (persons_num, auctions_num, bids_num)) =
            stream.select(0, 0).expect("Failed to select event.");

        println!(
            "Selecting events for epoch {}: {} persons, {} auctions, {} bids.",
            0, persons_num, auctions_num, bids_num
        );

        let auctions_batches = event_bytes_to_batch(&events.auctions, auction_schema, 1024);
        let person_batches = event_bytes_to_batch(&events.persons, person_schema, 1024);

        #[rustfmt::skip]
        // --------------------------------------------------------------------------------
        //                               NEXMark Query 3
        // --------------------------------------------------------------------------------
        // === Stage 0 ===
        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "seller", index: 1 }], 16)
        //     CoalesceBatchesExec: target_batch_size=4096
        //       FilterExec: CAST(category@2 AS Int64) = 10
        //         RepartitionExec: partitioning=RoundRobinBatch(16)
        //           MemoryExec: partitions=0, partition_sizes=[]

        // CoalesceBatchesExec: target_batch_size=4096
        //   RepartitionExec: partitioning=Hash([Column { name: "p_id", index: 0 }], 16)
        //     CoalesceBatchesExec: target_batch_size=4096
        //       FilterExec: state@3 = or OR state@3 = id OR state@3 = ca
        //         RepartitionExec: partitioning=RoundRobinBatch(16)
        //           MemoryExec: partitions=0, partition_sizes=[]
        //
        // === Stage 1 ===
        // ProjectionExec: expr=[name@4 as name, city@5 as city, state@6 as state, a_id@0 as a_id]
        //   CoalesceBatchesExec: target_batch_size=4096
        //     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: "seller", index: 1 }, Column { name: "p_id", index: 0 })]
        //       MemoryExec: partitions=0, partition_sizes=[]
        //       MemoryExec: partitions=0, partition_sizes=[]
        assert!(launcher.dag.node_count() == 2);

        let stages = launcher.dag.get_all_stages();
        let input = vec![vec![auctions_batches], vec![person_batches]];

        // === Query Stage 0 ===
        let mut ctx = stages[0].context.clone().unwrap();
        ctx.feed_data_sources(&input).await?;
        // We **MUST USE** execute_partitioned() instead of execute() here.
        let output = ctx.execute_partitioned().await?;
        assert!(output.len() == 2);
        assert_eq!(output[0].len(), output[1].len());

        // === Query Stage 1 ===
        let num_partitions = output[0].len();
        let mut ctx = stages[1].context.clone().unwrap();
        let mut result = vec![];
        for i in 0..num_partitions {
            ctx.feed_data_sources(&vec![
                vec![output[0][i].clone()],
                vec![output[1][i].clone()],
            ])
            .await?;
            let sliced_output = ctx.execute().await?;
            assert!(sliced_output.len() == 1);
            result.push(sliced_output.into_iter().nth(0).unwrap());
        }

        let result = result.into_iter().flatten().collect::<Vec<_>>();
        let formatted = pretty_format_batches(&result).unwrap().to_string();
        let expected: Vec<&str> = formatted.trim().lines().collect();

        // Local execution mode
        let mut launcher = LocalLauncher::new(&query).await?;
        launcher.feed_data_sources(&input);
        let batches = launcher.collect().await?;

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }
}
