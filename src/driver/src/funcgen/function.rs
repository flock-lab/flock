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
        dag.add_parent(
            NodeIndex::new(0),
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
                        plan:       dag.node_weight(node).unwrap().plan.clone(),
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
