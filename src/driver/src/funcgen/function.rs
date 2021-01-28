// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
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

//! LambdaFunction contains all the context information of the current query
//! plan. It is responsible for deploying lambda functions and execution
//! context.

extern crate daggy;
use daggy::{NodeIndex, Walker};

use crate::funcgen::dag::{DagNode, LambdaDag, CONCURRENCY_1};
use query::{Query, StreamQuery};
use runtime::context::LambdaContext;
use runtime::error::Result;
use runtime::DataSource;
use std::collections::{HashMap, VecDeque};

/// Lambda function invocation payload (request and response)
/// - 6 MB (synchronous)
/// - 256 KB (asynchronous)
const PAYLOAD_SIZE: usize = 256;

/// A struct `LambdaFunction` to generate cloud function names via `Query` and
/// `LambdaDag`.
#[derive(Debug)]
pub struct LambdaFunction {
    /// A query information received from the client-side.
    pub query:  Box<dyn Query>,
    /// A DAG structure representing the partitioned subplans from a given
    /// query.
    pub dag:    LambdaDag,
    /// A Lambda context representing a unique execution environment for the
    /// lambda function. The node in `dag` obtains the corresponding execution
    /// context from `ctx` through NodeIndex.
    pub ctx:    HashMap<NodeIndex, LambdaContext>,
    /// Query continuous data stream or offline batches.
    pub stream: bool,
}

impl LambdaFunction {
    /// Creates a new `LambdaFunction` from a given query.
    pub fn from(query: Box<dyn Query>) -> Self {
        let plan = query.plan();
        let stream = query.as_any().downcast_ref::<StreamQuery>().is_some();

        let mut dag = LambdaDag::from(&plan);
        Self::add_source(
            format!("{}", serde_json::to_value(&plan).unwrap()),
            &mut dag,
        );
        let ctx = Self::build_context(&query, &mut dag);
        Self {
            query,
            dag,
            ctx,
            stream,
        }
    }

    /// Deploys the lambda functions for a given query to the cloud.
    pub fn deploy() -> Result<()> {
        Ok(())
    }

    /// Adds a data source node into `LambdaDag`.
    fn add_source<S>(plan: S, dag: &mut LambdaDag)
    where
        S: Into<String>,
    {
        let parent = dag.node_count() - 1;
        dag.add_child(
            NodeIndex::new(parent),
            DagNode {
                plan:        plan.into(),
                concurrency: CONCURRENCY_1,
            },
        );
    }

    /// Creates the environmental execution context for all lambda functions.
    fn build_context(
        query: &Box<dyn Query>,
        dag: &mut LambdaDag,
    ) -> HashMap<NodeIndex, LambdaContext> {
        let mut ctx = HashMap::new();
        let mut queue = VecDeque::new();

        let root = NodeIndex::new(0);
        ctx.insert(
            root,
            LambdaContext {
                plan:       dag.get_node(root).unwrap().plan.clone(),
                next:       None,
                datasource: (*query.datasource()).clone(),
            },
        );

        queue.push_back(root);
        while let Some(parent) = queue.pop_front() {
            for (_, node) in dag.children(parent).iter(&dag) {
                ctx.insert(
                    node,
                    LambdaContext {
                        plan:       dag.get_node(node).unwrap().plan.clone(),
                        next:       None,
                        datasource: DataSource::Payload(PAYLOAD_SIZE),
                    },
                );
                queue.push_back(node);
            }
        }
        ctx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use query::stream::StreamWindow;

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext;

    use blake2::{Blake2b, Digest};

    async fn init_lambda_contex(sql: &str) -> Result<LambdaFunction> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;

        let mut ctx = ExecutionContext::new();

        let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;

        ctx.register_table("t", Box::new(table));

        let plan = ctx.create_logical_plan(&sql)?;
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;

        let json = serde_json::to_string(&plan).unwrap();
        let query: Box<dyn Query> = Box::new(StreamQuery {
            ansi_sql:   json,
            schema:     Some(schema),
            window:     StreamWindow::SessionWindow,
            cloudwatch: false,
            datasource: DataSource::UnknownEvent,
        });

        let mut dag = LambdaDag::from(&plan);
        LambdaFunction::add_source(
            format!("{}", serde_json::to_value(&plan).unwrap()),
            &mut dag,
        );
        let ctx = LambdaFunction::build_context(&query, &mut dag);

        Ok(LambdaFunction {
            query,
            dag,
            ctx,
            stream: true,
        })
    }

    #[tokio::test]
    async fn lambda_context_with_select() -> Result<()> {
        let sql = concat!("SELECT b FROM t ORDER BY b ASC LIMIT 3");
        let mut lambda_functions = init_lambda_contex(&sql).await?;
        assert_eq!(
            "UnknownEvent",
            format!(
                "{:?}",
                lambda_functions
                    .ctx
                    .get(&NodeIndex::new(0))
                    .unwrap()
                    .datasource
            )
        );
        assert_eq!(
            "Payload(256)",
            format!(
                "{:?}",
                lambda_functions
                    .ctx
                    .get(&NodeIndex::new(1))
                    .unwrap()
                    .datasource
            )
        );

        let dag = &mut lambda_functions.dag;
        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut node = iter.next().unwrap();
        assert!(node.plan.contains(r#"execution_plan":"projection_exec"#));
        assert!(node.plan.contains(r#"execution_plan":"memory_exec"#));
        assert_eq!(8, node.concurrency);

        node = iter.next().unwrap();
        assert!(node.plan.contains(r#"execution_plan":"projection_exec"#));
        assert!(node.plan.contains(r#"execution_plan":"memory_exec"#));
        assert_eq!(1, node.concurrency);

        Ok(())
    }

    #[tokio::test]
    async fn lambda_context_with_agg() -> Result<()> {
        let sql = concat!("SELECT MIN(a), AVG(b) ", "FROM t ", "GROUP BY b");
        let mut lambda_functions = init_lambda_contex(&sql).await?;
        assert_eq!(
            "UnknownEvent",
            format!(
                "{:?}",
                lambda_functions
                    .ctx
                    .get(&NodeIndex::new(0))
                    .unwrap()
                    .datasource
            )
        );
        assert_eq!(
            "Payload(256)",
            format!(
                "{:?}",
                lambda_functions
                    .ctx
                    .get(&NodeIndex::new(1))
                    .unwrap()
                    .datasource
            )
        );
        assert_eq!(
            "Payload(256)",
            format!(
                "{:?}",
                lambda_functions
                    .ctx
                    .get(&NodeIndex::new(2))
                    .unwrap()
                    .datasource
            )
        );

        let dag = &mut lambda_functions.dag;
        assert_eq!(3, dag.node_count());
        assert_eq!(2, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut node = iter.next().unwrap();
        assert!(node.plan.contains(r#"execution_plan":"projection_exec"#));
        assert!(node
            .plan
            .contains(r#"execution_plan":"hash_aggregate_exec"#));
        assert!(node.plan.contains(r#"execution_plan":"memory_exec"#));
        assert_eq!(1, node.concurrency);

        node = iter.next().unwrap();
        assert!(node
            .plan
            .contains(r#"execution_plan":"hash_aggregate_exec"#));
        assert!(node.plan.contains(r#"execution_plan":"memory_exec"#));
        assert_eq!(8, node.concurrency);

        node = iter.next().unwrap();
        assert!(node.plan.contains(r#"execution_plan":"projection_exec"#));
        assert!(node
            .plan
            .contains(r#"execution_plan":"hash_aggregate_exec"#));
        assert!(node
            .plan
            .contains(r#"execution_plan":"hash_aggregate_exec"#));
        assert!(node.plan.contains(r#"execution_plan":"memory_exec"#));
        assert_eq!(1, node.concurrency);

        Ok(())
    }

    #[tokio::test]
    async fn lambda_function_name() -> Result<()> {
        let hash = Blake2b::digest(b"SELECT b FROM t ORDER BY b ASC LIMIT 3");
        let mut s1 = base64::encode(&hash);
        s1.truncate(16);

        let s2 = "00";
        let s3 = chrono::offset::Utc::now();

        let name = format!("{:?}-{:?}-{:?}", s1, s2, s3);
        println!("{}", name);
        Ok(())
    }
}
