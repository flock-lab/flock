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

//! This crate defines the execution plan on cloud platforms. The execution plan
//! can be stored in the `ExecutionContext` or can be stored in the object
//! store.

use crate::aws::s3;
use crate::error::Result;
use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use log::info;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

type S3BUCKET = String;
type S3KEY = String;

/// The execution plan on cloud.
#[derive(Default, Clone, Deserialize, Serialize)]
pub struct CloudExecutionPlan {
    /// The execution plans of the lambda function.
    pub execution_plans: Vec<Arc<dyn ExecutionPlan>>,
    /// The S3 URL of the physical plan. If the plan is too large to be
    /// serialized and stored in the environment variable, the system will
    /// store the plan in S3.
    pub object_storage:  Option<(S3BUCKET, S3KEY)>,
}

impl std::fmt::Debug for CloudExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let plan_str = self
            .execution_plans
            .iter()
            .map(|plan| format!("{}", displayable(plan.as_ref()).indent()))
            .collect::<Vec<String>>()
            .join("\n");
        write!(
            f,
            "CloudExecutionPlan {{ execution_plans: {}, object_storage: {:?} }}",
            plan_str, self.object_storage
        )
    }
}

impl CloudExecutionPlan {
    /// Create a new CloudExecutionPlan from the object storage.
    pub fn new(
        execution_plans: Vec<Arc<dyn ExecutionPlan>>,
        object_storage: Option<(S3BUCKET, S3KEY)>,
    ) -> Self {
        CloudExecutionPlan {
            execution_plans,
            object_storage,
        }
    }

    /// Create a new CloudExecutionPlan from

    /// Returns the execution plan.
    pub async fn get_execution_plans(&mut self) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        if self.execution_plans.is_empty()
            || (self.execution_plans.len() == 1
                && self.execution_plans[0].as_any().is::<EmptyExec>())
        {
            if self.object_storage.is_some() {
                info!("Loading plan from S3 {:?}", self.object_storage);
                let (bucket, key) = self.object_storage.as_ref().unwrap();
                self.execution_plans =
                    vec![serde_json::from_slice(&s3::get_object(bucket, key).await?)?];
            } else {
                panic!("The query plan is not stored in the environment variable and S3.");
            }
        }
        Ok(self.execution_plans.clone())
    }
}

macro_rules! query_has_op_function {
    ($OPERATOR:ident, $FUNC:ident) => {
        /// Returns true if the current execution plan contains a given operator.
        pub fn $FUNC(plan: &Arc<dyn ExecutionPlan>) -> bool {
            let mut curr = plan.clone();
            loop {
                if curr.as_any().downcast_ref::<$OPERATOR>().is_some() {
                    return true;
                }
                if curr.children().is_empty() {
                    break;
                }
                curr = curr.children()[0].clone();
            }
            false
        }
    };
}

query_has_op_function!(SortExec, contain_sort);
query_has_op_function!(HashJoinExec, contain_join);
query_has_op_function!(HashAggregateExec, contain_aggregate);

/// A wrapper to generate the execution plan from a SQL query.
pub async fn physical_plan<T: AsRef<str>>(
    ctx: &ExecutionContext,
    sql: T,
) -> Result<Arc<dyn ExecutionPlan>> {
    let logical_plan = ctx.create_logical_plan(sql.as_ref())?;
    let logical_plan = ctx.optimize(&logical_plan)?;
    Ok(ctx.create_physical_plan(&logical_plan).await?)
}
