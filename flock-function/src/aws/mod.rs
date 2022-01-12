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

use crate::launcher::Launcher;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use flock::distributed_plan::DistributedPlanner;
use flock::distributed_plan::QueryDag;
use flock::error::Result;
use flock::query::Query;

/// AwsLambdaLauncher defines the interface for deploying and executing
/// queries on AWS Lambda.
pub struct AwsLambdaLauncher {
    /// The DAG of a given query.
    pub dag: QueryDag,
}

#[async_trait]
impl Launcher for AwsLambdaLauncher {
    async fn new<T>(query: &Query<T>) -> Result<Self>
    where
        Self: Sized,
        T: AsRef<str> + Send + Sync + 'static,
    {
        let plan = query.plan()?;
        let planner = DistributedPlanner::new();
        let dag = planner.plan_query_stages(plan).await?;
        Ok(AwsLambdaLauncher { dag })
    }

    fn deploy(&self) -> Result<()> {
        unimplemented!();
    }

    async fn execute(&self) -> Result<Vec<RecordBatch>> {
        unimplemented!();
    }
}
