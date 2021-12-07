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

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::Partitioning;
use lazy_static::lazy_static;
use log::info;
use runtime::prelude::*;

lazy_static! {
    static ref PARALLELISM: usize = globals["lambda"]["parallelism"].parse::<usize>().unwrap();
}

/// The function invocation counter per lambda instance.
static mut INVOCATION_COUNTER_PER_INSTANCE: u32 = 0;

/// The function executor for the nexmark benchmark.
///
/// This function is invoked by the datafusion runtime. It is responsible for
/// executing the physical plan. It is also responsible for collecting the
/// results of the execution. After the execution is finished, the results are
/// written to the output. The results are written to the output in the form of
/// Arrow RecordBatch.
///
/// ## Arguments
/// `ctx`: The Flock runtime context.
/// `r1_records`: The input record batches for the first relation.
/// `r2_records`: The input record batches for the second relation.
///
/// ## Returns
/// A vector of Arrow RecordBatch.
pub async fn collect(
    ctx: &mut ExecutionContext,
    r1_records: Vec<Vec<RecordBatch>>,
    r2_records: Vec<Vec<RecordBatch>>,
) -> Result<Vec<RecordBatch>> {
    let mut inputs = vec![];
    if !(r1_records.is_empty() || r1_records.iter().all(|r| r.is_empty())) {
        inputs.push(
            LambdaExecutor::repartition(r1_records, Partitioning::RoundRobinBatch(*PARALLELISM))
                .await?,
        );
    }
    if !(r2_records.is_empty() || r2_records.iter().all(|r| r.is_empty())) {
        inputs.push(
            LambdaExecutor::repartition(r2_records, Partitioning::RoundRobinBatch(*PARALLELISM))
                .await?,
        );
    }

    if inputs.is_empty() {
        return Ok(vec![]);
    } else {
        ctx.feed_data_sources(&inputs);
        let output = ctx.execute().await?;
        if ctx.debug {
            println!("{}", pretty_format_batches(&output)?);
            unsafe {
                INVOCATION_COUNTER_PER_INSTANCE += 1;
                info!("# invocations: {}", INVOCATION_COUNTER_PER_INSTANCE);
            }
        }
        Ok(output)
    }
}
