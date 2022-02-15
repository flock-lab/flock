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

//! The time window types for the stream queries.

pub mod elementwise;
pub mod global;
pub mod hopping;
pub mod session;
pub mod tumbling;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::empty::EmptyExec;
use flock::prelude::*;

/// This function is used to coalesce smaller session windows or global windows
/// to bigger ones so that the number of events in each payload is greater than
/// the granule size, and close to the payload limit.
fn coalesce_windows(
    windows: Vec<Vec<Vec<RecordBatch>>>,
    granule_size: usize,
) -> Result<Vec<Vec<Vec<RecordBatch>>>> {
    #[allow(unused_assignments)]
    let mut curr_size = 0;
    let mut total_size = 0;
    let mut res = vec![];
    let mut tmp = vec![];
    for window in windows {
        curr_size = window
            .iter()
            .map(|v| v.iter().map(|b| b.num_rows()).sum::<usize>())
            .sum::<usize>();
        total_size += curr_size;
        if total_size <= granule_size * 2 || tmp.is_empty() {
            tmp.push(window);
        } else {
            res.push(tmp.into_iter().flatten().collect::<Vec<Vec<RecordBatch>>>());
            tmp = vec![window];
            total_size = curr_size;
        }
    }
    if !tmp.is_empty() {
        res.push(tmp.into_iter().flatten().collect::<Vec<Vec<RecordBatch>>>());
    }
    Ok(res)
}

/// Is distributed execution enabled?
fn is_distributed(ctx: &ExecutionContext) -> bool {
    ctx.plan.execution_plans[0]
        .as_any()
        .downcast_ref::<EmptyExec>()
        .is_none()
}
