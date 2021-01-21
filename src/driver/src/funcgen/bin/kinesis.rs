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

//! The generic lambda function to pull data from Kinesis Data Stream.

use aws_lambda_events::event::kinesis::KinesisEvent;

use datafusion::physical_plan::{common, ExecutionPlan};

use lambda::{handler_fn, Context};
use serde_json::Value;

use std::sync::Once;

use scq_lambda::dataframe::{from_kinesis_to_batch, DataFrame};
use scq_lambda::plan::*;
use scq_lambda::{exec_plan, init_plan};

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

/// Initialize the lambda function once and only once.
#[allow(dead_code)]
static INIT: Once = Once::new();

/// Empty Plan before initializing the cloud environment.
#[allow(dead_code)]
static mut PLAN: LambdaPlan = LambdaPlan::None;

#[allow(dead_code)]
async fn handler(event: KinesisEvent, _: Context) -> Result<Value, Error> {
    let (schema, plan) = init_plan!(INIT, PLAN);

    let (record_batch, _) = from_kinesis_to_batch(event);
    let result = exec_plan!(plan, vec![vec![record_batch]]);

    let dataframe = DataFrame::from(&result[0], schema);
    Ok(serde_json::to_value(&dataframe)?)
}
