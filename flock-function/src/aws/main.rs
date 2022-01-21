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

//! The main entry point for the generic lambda function.

#![feature(get_mut_unchecked)]

mod actor;
mod cloud_context;
mod nexmark;
mod s3;
mod window;
mod ysb;

use cloud_context::*;
use flock::prelude::*;
use hashring::HashRing;
use lambda_runtime::{service_fn, LambdaEvent};
use log::info;
use serde_json::Value;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

async fn handler(event: LambdaEvent<Payload>) -> Result<Value> {
    let payload = event.payload;
    let (ctx, arena) = init_exec_context!();
    update_consistent_hash_context(&payload.metadata)?;

    info!(
        "AWS Lambda function architecture: {}",
        std::env::consts::ARCH
    );

    match &payload.datasource {
        DataSource::Payload(_) => actor::handler(ctx, arena, payload).await,
        DataSource::NEXMarkEvent(_) => nexmark::handler(ctx, payload).await,
        DataSource::YSBEvent(_) => ysb::handler(ctx, payload).await,
        DataSource::S3(_) => s3::handler(ctx, payload).await,
        _ => unimplemented!(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    lambda_runtime::run(service_fn(handler)).await?;
    Ok(())
}
