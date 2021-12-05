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

//! This function simulates a scatter operation by forwarding all inputs from
//! its running instances to a lambda function `flock_pg_gather`.

use chrono::Utc;
use lambda_runtime::{handler_fn, Context};
use rusoto_core::Region;
use rusoto_lambda::{InvokeAsyncRequest, Lambda, LambdaClient};
use serde_json::Value;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    println!("{}: {}", Utc::now(), event["val"]);
    LambdaClient::new(Region::UsEast1)
        .invoke_async(InvokeAsyncRequest {
            function_name: "flock_pg_gather".to_string(),
            invoke_args:   serde_json::to_vec(&event)?.into(),
        })
        .await?;
    Ok(event)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::run(handler_fn(handler)).await?;
    Ok(())
}
