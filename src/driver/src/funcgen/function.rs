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

//! QueryFlow contains all the context information of the current query
//! plan. It is responsible for deploying lambda functions and execution
//! context.

extern crate daggy;
use daggy::{NodeIndex, Walker};

use crate::deploy::ExecutionEnvironment;
use crate::funcgen::dag::*;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use runtime::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use blake2::{Blake2b, Digest};
use chrono::{DateTime, Utc};

/// A struct `QueryFlow` to generate cloud function names via `Query` and
/// `QueryDag`.
#[derive(Debug)]
pub struct QueryFlow {
    /// A query information received from the client-side.
    pub query: Box<dyn Query>,
    /// A DAG structure representing the partitioned subplans from a given
    /// query.
    pub dag:   QueryDag,
    /// A Lambda context representing a unique execution environment for the
    /// lambda function. The node in `dag` obtains the corresponding execution
    /// context from `ctx` through NodeIndex.
    pub ctx:   HashMap<NodeIndex, ExecutionContext>,
}

impl QueryFlow {
    /// Create a new `QueryFlow`.
    pub fn new(
        sql: &str,
        schema: SchemaRef,
        datasource: DataSource,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let query = Box::new(StreamQuery {
            ansi_sql: sql.to_owned(),
            schema,
            datasource,
            plan,
        });
        QueryFlow::from(query)
    }

    /// Create a new `QueryFlow` from a given query.
    pub fn from(query: Box<dyn Query>) -> Self {
        let plan = query.plan();

        let mut dag = QueryDag::from(&plan);
        Self::add_source(plan.clone(), &mut dag);
        let ctx = Self::build_context(&*query, &mut dag);
        Self { query, dag, ctx }
    }

    /// Deploy the lambda functions for a given query to the cloud.
    pub async fn deploy(&self, environment: ExecutionEnvironment) -> Result<()> {
        environment.deploy(&self).await
    }

    /// Add a data source node into `QueryDag`.
    #[inline]
    fn add_source(plan: Arc<dyn ExecutionPlan>, dag: &mut QueryDag) {
        let parent = dag.node_count() - 1;
        dag.add_child(
            NodeIndex::new(parent),
            DagNode {
                plan:        plan.clone(),
                concurrency: CONCURRENCY_1,
            },
        );
    }

    /// Return a unique function name.
    #[inline]
    fn function_name(query_code: &String, node: &NodeIndex, timestamp: &DateTime<Utc>) -> String {
        let plan_index = format!("{:0>2}", node.index());
        format!("{}-{}-{:?}", query_code, plan_index, timestamp)
    }

    /// Create the environmental execution context for all lambda functions.
    fn build_context(
        query: &dyn Query,
        dag: &mut QueryDag,
    ) -> HashMap<NodeIndex, ExecutionContext> {
        let mut query_code = base64::encode(&Blake2b::digest(query.sql().as_bytes()));
        query_code.truncate(16);
        let timestamp = chrono::offset::Utc::now();

        let mut ctx = HashMap::new();
        let root = NodeIndex::new(0);
        ctx.insert(
            root,
            ExecutionContext {
                plan:       dag.get_node(root).unwrap().plan.clone(),
                name:       Self::function_name(&query_code, &root, &timestamp),
                next:       CloudFunction::None,
                datasource: DataSource::Payload,
            },
        );

        let ncount = dag.node_count();
        assert!((1..=99).contains(&ncount));

        // Breadth-first search
        let mut queue = VecDeque::new();

        queue.push_back(root);
        while let Some(parent) = queue.pop_front() {
            for (_, node) in dag.children(parent).iter(&dag) {
                ctx.insert(
                    node,
                    ExecutionContext {
                        plan:       dag.get_node(node).unwrap().plan.clone(),
                        name:       Self::function_name(&query_code, &node, &timestamp),
                        next:       {
                            let name = ctx.get(&parent).unwrap().name.clone();
                            if dag.get_node(parent).unwrap().concurrency == 1 {
                                CloudFunction::Chorus((name, CONCURRENCY_8))
                            } else {
                                CloudFunction::Solo(name)
                            }
                        },
                        datasource: {
                            if node.index() == ncount - 1 {
                                (*query.datasource()).clone()
                            } else {
                                DataSource::Payload
                            }
                        },
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

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext;

    use blake2::{Blake2b, Digest};

    async fn init_query_flow(sql: &str) -> Result<QueryFlow> {
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

        ctx.register_table("t", Arc::new(table));

        let plan = physical_plan(&mut ctx, &sql)?;
        let query: Box<dyn Query> = Box::new(StreamQuery {
            ansi_sql: sql.to_string(),
            schema,
            datasource: DataSource::UnknownEvent,
            plan,
        });

        let mut dag = QueryDag::from(&query.plan());
        QueryFlow::add_source(query.plan().clone(), &mut dag);
        let ctx = QueryFlow::build_context(&*query, &mut dag);

        Ok(QueryFlow { query, dag, ctx })
    }

    fn datasource(func: &QueryFlow, idx: usize) -> String {
        format!(
            "{:?}",
            func.ctx.get(&NodeIndex::new(idx)).unwrap().datasource
        )
    }

    fn function_name(func: &QueryFlow, idx: usize) -> String {
        func.ctx.get(&NodeIndex::new(idx)).unwrap().name.clone()
    }

    fn next_function(func: &QueryFlow, idx: usize) -> CloudFunction {
        func.ctx.get(&NodeIndex::new(idx)).unwrap().next.clone()
    }

    #[tokio::test]
    async fn execute_context_with_select() -> Result<()> {
        let sql = concat!("SELECT b FROM t ORDER BY b ASC LIMIT 3");

        let mut functions = init_query_flow(&sql).await?;
        assert_eq!("Payload", datasource(&functions, 0));
        assert_eq!("UnknownEvent", datasource(&functions, 1));

        assert!(function_name(&functions, 0).contains("00"));
        assert!(function_name(&functions, 1).contains("01"));

        assert!(matches!(next_function(&functions, 0), CloudFunction::None));
        assert!(matches!(
            next_function(&functions, 1),
            CloudFunction::Solo(..)
        ));

        let dag = &mut functions.dag;
        assert_eq!(2, dag.node_count());
        assert_eq!(1, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut node = iter.next().unwrap();
        assert!(node.get_plan_str().contains(r#"projection_exec"#));
        assert!(node.get_plan_str().contains(r#"memory_exec"#));
        assert_eq!(8, node.concurrency);

        node = iter.next().unwrap();
        assert!(node.get_plan_str().contains(r#"projection_exec"#));
        assert!(node.get_plan_str().contains(r#"memory_exec"#));
        assert_eq!(1, node.concurrency);

        Ok(())
    }

    #[tokio::test]
    async fn execute_context_with_agg() -> Result<()> {
        let sql = concat!("SELECT MIN(a), AVG(b) ", "FROM t ", "GROUP BY b");

        let mut functions = init_query_flow(&sql).await?;
        assert_eq!("Payload", datasource(&functions, 0));
        assert_eq!("Payload", datasource(&functions, 1));
        assert_eq!("UnknownEvent", datasource(&functions, 2));

        assert!(function_name(&functions, 0).contains("00"));
        assert!(function_name(&functions, 1).contains("01"));
        assert!(function_name(&functions, 2).contains("02"));

        assert!(matches!(next_function(&functions, 0), CloudFunction::None));
        assert!(matches!(
            next_function(&functions, 1),
            CloudFunction::Chorus(..)
        ));
        assert!(matches!(
            next_function(&functions, 2),
            CloudFunction::Solo(..)
        ));

        let dag = &mut functions.dag;
        assert_eq!(3, dag.node_count());
        assert_eq!(2, dag.edge_count());

        let mut iter = dag.node_weights_mut();
        let mut node = iter.next().unwrap();
        assert!(node.get_plan_str().contains(r#"projection_exec"#));
        assert!(node.get_plan_str().contains(r#"hash_aggregate_exec"#));
        assert!(node.get_plan_str().contains(r#"memory_exec"#));
        assert_eq!(1, node.concurrency);

        node = iter.next().unwrap();
        assert!(node.get_plan_str().contains(r#"hash_aggregate_exec"#));
        assert!(node.get_plan_str().contains(r#"memory_exec"#));
        assert_eq!(8, node.concurrency);

        node = iter.next().unwrap();
        assert!(node.get_plan_str().contains(r#"projection_exec"#));
        assert!(node.get_plan_str().contains(r#"hash_aggregate_exec"#));
        assert!(node.get_plan_str().contains(r#"hash_aggregate_exec"#));
        assert!(node.get_plan_str().contains(r#"memory_exec"#));
        assert_eq!(1, node.concurrency);

        Ok(())
    }

    #[tokio::test]
    async fn lambda_function_name() -> Result<()> {
        // query code
        let hash = Blake2b::digest(b"SELECT b FROM t ORDER BY b ASC LIMIT 3");
        let mut s1 = base64::encode(&hash);
        s1.truncate(16);

        // plan index
        let s2 = format!("{:0>2}", 0);
        //                  |||
        //                  ||+-- width
        //                  |+--- align
        //                  +---- fill
        assert_eq!("00", s2);

        // timestamp
        let s3 = chrono::offset::Utc::now();

        let name = format!("{}-{}-{:?}", s1, s2, s3);
        println!("{}", name);

        Ok(())
    }
}
