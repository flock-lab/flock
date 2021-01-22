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

use datafusion::physical_plan::ExecutionPlan;

use std::ops::Deref;
use std::sync::Arc;

type DagNode = Arc<dyn ExecutionPlan>;
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

    /// Build a Dag representation of the given plan.
    pub fn from(plan: Arc<dyn ExecutionPlan>) -> Self {
        let dag = Self::build_dag(plan);
        LambdaDag { dag }
    }

    /// Return the depth for the given node in the dag.
    pub fn depth(&self, node: NodeIndex) -> usize {
        self.dag
            .recursive_walk(node, |g, n| g.parents(n).walk_next(g))
            .iter(&self.dag)
            .count()
    }

    /// Add a new node to the `Dag`.
    ///
    /// Computes in **O(1)** time.
    ///
    /// Returns the index of the new node.
    pub fn add_node(&mut self, node: DagNode) -> NodeIndex {
        self.dag.add_node(node)
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
    fn build_dag(_plan: Arc<dyn ExecutionPlan>) -> DagPlan {
        unimplemented!();
    }
}
