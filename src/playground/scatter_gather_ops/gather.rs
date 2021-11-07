// Copyright (c) 2021 UMD Database Group. All Rights Reserved.
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
// Only bring in dependencies for the repl when the cli feature is enabled.

//! This function sums up all values it received from the `flock_pg_scatter`
//! function.

use chrono::Utc;
use lambda_runtime::{handler_fn, Context};
use serde_json::json;
use serde_json::Value;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

static mut SUM: i64 = 0;

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    unsafe {
        SUM += event["val"].as_i64().unwrap();
        println!("{}: {}", Utc::now(), SUM);
        Ok(json!({ "val": SUM }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}
