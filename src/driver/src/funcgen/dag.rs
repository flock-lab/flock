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

//! A directed acyclic graph (DAG) data structure to hold all sub-plans of the
//! query statement.

extern crate daggy;
use daggy::{Dag, NodeIndex, Walker};

use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

use serde_json::Value;

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Concurrency is the number of requests that your function is serving at any
/// given time. When your function is invoked, cloud function services allocates
/// an instance of it to process the event. When the function code finishes
/// running, it can handle another request. If the function is invoked again
/// while a request is still being processed, another instance is allocated,
/// which increases the function's concurrency.
/// cloud function with concurrency = 1
pub const CONCURRENCY_1: u8 = 1;
/// cloud function with concurrency = 8
pub const CONCURRENCY_8: u8 = 8;

type DagEdge = ();
type DagPlan = Dag<DagNode, DagEdge>;

/// A node representation of the subplan of a query statement.
#[derive(Debug, Clone)]
pub struct DagNode {
    /// Subplan string.
    pub plan:        String,
    /// Function concurrency in cloud environment.
    pub concurrency: u8,
}

impl From<String> for DagNode {
    fn from(s: String) -> DagNode {
        DagNode {
            plan:        s,
            concurrency: CONCURRENCY_8,
        }
    }
}

impl Deref for DagNode {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

/// A simple directed acyclic graph representation of the physical plan of a
/// query statement. This graph allows the traversal of subplans in a
/// topological order.  It is also possible to query subplan or dependencies
/// for a given subplan.
#[derive(Debug)]
pub struct QueryDag {
    dag: DagPlan,
}

impl Deref for QueryDag {
    type Target = DagPlan;

    fn deref(&self) -> &Self::Target {
        &self.dag
    }
}

impl DerefMut for QueryDag {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dag
    }
}

impl QueryDag {
    /// Creates a new `QueryDag`.
    pub fn new() -> Self {
        QueryDag {
            dag: DagPlan::new(),
        }
    }

    /// The total number of nodes in the Dag.
    pub fn node_count(&self) -> usize {
        self.dag.node_count()
    }

    /// Builds a Dag representation of a given plan.
    pub fn from(plan: &Arc<dyn ExecutionPlan>) -> Self {
        Self::build_dag(plan)
    }

    /// Returns the depth for the given node in the dag.
    pub fn depth(&self, node: NodeIndex) -> usize {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .count()
    }

    /// Adds a new node to the `QueryDag`.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Returns the index of the new node.
    pub fn add_node(&mut self, node: DagNode) -> NodeIndex {
        self.dag.add_node(node)
    }

    /// Adds a root node to the `QueryDag`.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Returns the index of the new root.
    pub fn add_parent(&mut self, child: NodeIndex, node: DagNode) -> NodeIndex {
        let (_, n) = self.dag.add_parent(child, (), node);
        n
    }

    /// Returns a node for a given id.
    pub fn get_node(&self, id: NodeIndex) -> Option<&DagNode> {
        self.dag.node_weight(id)
    }

    /// Adds a new child node to the node at the given `NodeIndex`.
    /// Returns the node's `NodeIndex`.
    ///
    /// child -> edge -> node
    ///
    /// Computes in **O(1)** time.
    pub fn add_child(&mut self, parent: NodeIndex, node: DagNode) -> NodeIndex {
        let (_, n) = self.dag.add_child(parent, (), node);
        n
    }

    /// Returns a **Vec** with all depended subplans for the given node.
    pub fn get_depended_plans(&self, node: NodeIndex) -> Vec<(DagNode, NodeIndex)> {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .map(|(_, n)| (self.dag.node_weight(n).unwrap().clone(), n))
            .collect()
    }

    /// Returns a **Vec** with all children for the given node.
    pub fn get_sub_plans(&self, node: NodeIndex) -> Vec<(DagNode, NodeIndex)> {
        let mut children = Vec::new();
        for (_, n) in self.dag.children(node).iter(&self.dag) {
            children.push((self.dag.node_weight(n).unwrap().clone(), n));
            children.append(&mut self.get_sub_plans(n));
        }
        children
    }

    /// Returns the internal daggy.
    pub fn context(&mut self) -> &mut DagPlan {
        &mut self.dag
    }

    /// Builds a new daggy from a physical plan.
    fn build_dag(plan: &Arc<dyn ExecutionPlan>) -> Self {
        let mut dag = QueryDag::new();
        Self::fission(&mut dag, &plan);
        assert!(dag.node_count() >= 1);
        dag
    }

    /// Adds a new node to the `QueryDag`.
    fn insert_dag(dag: &mut QueryDag, leaf: NodeIndex, node: Value, concurrency: u8) -> NodeIndex {
        if leaf == NodeIndex::end() {
            dag.add_node(DagNode {
                plan: format!("{}", node),
                concurrency,
            })
        } else {
            dag.add_child(
                leaf,
                DagNode {
                    plan: format!("{}", node),
                    concurrency,
                },
            )
        }
    }

    /// Transforms a physical plan for cloud environment execution.
    fn fission(dag: &mut QueryDag, plan: &Arc<dyn ExecutionPlan>) {
        let mut root = serde_json::to_value(&plan).unwrap();
        let mut json = &mut root;
        let mut leaf = NodeIndex::end();
        let mut curr = plan.clone();
        loop {
            match json["execution_plan"].as_str() {
                Some("hash_aggregate_exec") => match json["mode"].as_str() {
                    Some("Final") => {
                        // Split the plan into two subplans
                        let object = (*json["input"].take().as_object().unwrap()).clone();
                        // Add a input for the new subplan
                        let input: Arc<dyn ExecutionPlan> =
                            Arc::new(MemoryExec::try_new(&vec![], curr.schema(), None).unwrap());
                        json["input"] = serde_json::to_value(input).unwrap();
                        // Add the new subplan to DAG
                        leaf = Self::insert_dag(dag, leaf, root, CONCURRENCY_1);
                        // Point to the next subplan
                        root = Value::Object(object);
                        json = &mut root;
                    }
                    _ => json = &mut json["input"],
                },
                Some("hash_join_exec") => {
                    let object = (*json.take().as_object().unwrap()).clone();
                    let input: Arc<dyn ExecutionPlan> =
                        Arc::new(MemoryExec::try_new(&vec![], curr.schema(), None).unwrap());
                    *json = serde_json::to_value(input).unwrap();
                    leaf = Self::insert_dag(dag, leaf, root, CONCURRENCY_1);
                    root = Value::Object(object);
                    break;
                }
                _ => json = &mut json["input"],
            }
            if !json.is_object() {
                break;
            }
            curr = curr.children()[0].clone();
        }
        Self::insert_dag(dag, leaf, root, CONCURRENCY_8);
    }
}
