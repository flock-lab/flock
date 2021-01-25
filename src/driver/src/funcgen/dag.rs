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

type DagNode = String;
type DagEdge = ();
type DagPlan = Dag<DagNode, DagEdge>;

/// A simple directed acyclic graph representation of the physical plan of a
/// query statement. This graph allows the traversal of subplans in a
/// topological order.  It is also possible to query subplan or dependencies
/// for a given subplan.
#[derive(Debug)]
pub struct LambdaDag {
    dag: DagPlan,
}

impl Deref for LambdaDag {
    type Target = DagPlan;

    fn deref(&self) -> &Self::Target {
        &self.dag
    }
}

impl DerefMut for LambdaDag {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dag
    }
}

impl LambdaDag {
    /// Create a new `LambdaDag`.
    pub fn new() -> Self {
        LambdaDag {
            dag: DagPlan::new(),
        }
    }

    /// The total number of nodes in the Dag.
    pub fn node_count(&self) -> usize {
        self.dag.node_count()
    }

    /// Build a Dag representation of a given plan.
    pub fn from(plan: &Arc<dyn ExecutionPlan>) -> Self {
        Self::build_dag(plan)
    }

    /// Return the depth for the given node in the dag.
    pub fn depth(&self, node: NodeIndex) -> usize {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .count()
    }

    /// Add a new node to the `LambdaDag`.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Returns the index of the new node.
    pub fn add_node<S>(&mut self, node: S) -> NodeIndex
    where
        S: Into<DagNode>,
    {
        self.dag.add_node(node.into())
    }

    /// Return a node for a given id.
    pub fn get_node(&self, id: NodeIndex) -> Option<&DagNode> {
        self.dag.node_weight(id)
    }

    /// Add a new child node to the node at the given `NodeIndex`.
    /// Returns the node's `NodeIndex`.
    ///
    /// child -> edge -> node
    ///
    /// Computes in **O(1)** time.
    pub fn add_child(&mut self, parent: NodeIndex, node: DagNode) -> NodeIndex {
        let (_, n) = self.dag.add_child(parent, (), node);
        n
    }

    /// Return a **Vec** with all depended subplans for the given node.
    pub fn get_depended_plans(&self, node: NodeIndex) -> Vec<(DagNode, NodeIndex)> {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .map(|(_, n)| ((&*self.dag.node_weight(n).unwrap()).clone(), n))
            .collect()
    }

    /// Return a **Vec** with all children for the given node.
    pub fn get_sub_plans(&self, node: NodeIndex) -> Vec<(DagNode, NodeIndex)> {
        let mut children = Vec::new();
        for (_, n) in self.dag.children(node).iter(&self.dag) {
            children.push(((&*self.dag.node_weight(n).unwrap()).clone(), n));
            children.append(&mut self.get_sub_plans(n));
        }
        children
    }

    /// Return the internal daggy.
    pub fn context(&mut self) -> &mut DagPlan {
        &mut self.dag
    }

    /// Build a new daggy from a physical plan.
    fn build_dag(plan: &Arc<dyn ExecutionPlan>) -> Self {
        let mut dag = LambdaDag::new();
        Self::fission(&mut dag, &plan);
        dag
    }

    /// Add a new node to the `LambdaDag`.
    fn insert_dag(dag: &mut LambdaDag, leaf: NodeIndex, node: Value) -> NodeIndex {
        if leaf == NodeIndex::end() {
            dag.add_node(format!("{}", node))
        } else {
            dag.add_child(leaf, format!("{}", node))
        }
    }

    /// Transform a physical plan for cloud environment execution.
    fn fission(dag: &mut LambdaDag, plan: &Arc<dyn ExecutionPlan>) {
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
                        leaf = Self::insert_dag(dag, leaf, root);
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
                    leaf = Self::insert_dag(dag, leaf, root);
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
        Self::insert_dag(dag, leaf, root);
    }
}
