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

//! The generic lambda function for sub-plan execution on AWS Lambda.

#![warn(missing_docs)]
// Clippy lints, some should be disabled incrementally
#![allow(
    clippy::float_cmp,
    clippy::module_inception,
    clippy::new_without_default,
    clippy::ptr_arg,
    clippy::type_complexity,
    clippy::wrong_self_convention
)]

use aws_lambda_events::event::kafka::KafkaEvent;
use aws_lambda_events::event::kinesis::KinesisEvent;
use lambda::{handler_fn, Context};
use runtime::prelude::*;
use serde_json::json;
use serde_json::Value;
use std::sync::Once;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Initializes the lambda function once and only once.
static INIT: Once = Once::new();

enum CloudFunctionContext {
    Lambda(Box<ExecutionContext>),
    Uninitialized,
}

/// Lambda execution context.
static mut EXECUTION_CONTEXT: CloudFunctionContext = CloudFunctionContext::Uninitialized;

/// Performs an initialization routine once and only once.
macro_rules! init_exec_context {
    () => {{
        unsafe {
            INIT.call_once(|| match config::global("context_name") {
                Some(name) => match std::env::var(name) {
                    Ok(s) => {
                        EXECUTION_CONTEXT =
                            CloudFunctionContext::Lambda(Box::new(ExecutionContext::unmarshal(&s)));
                    }
                    Err(_) => {
                        panic!("No execution context in the cloud environment.");
                    }
                },
                None => {
                    panic!("No execution context name!");
                }
            });
            match &mut EXECUTION_CONTEXT {
                CloudFunctionContext::Lambda(ctx) => ctx,
                CloudFunctionContext::Uninitialized => panic!("Uninitialized execution context!"),
            }
        }
    }};
}

#[tokio::main]
async fn main() -> Result<()> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn kinesis_handler(ctx: &mut ExecutionContext, event: Value) -> Result<Value> {
    let kinesis_event: KinesisEvent = serde_json::from_value(event).unwrap();
    let batch = match kinesis::to_batch(kinesis_event) {
        Some(batch) => batch,
        None => return Ok(json!("No Kinesis input!")),
    };
    let schema = batch.schema();

    ctx.feed_one_source(&vec![vec![batch]]);
    let batches = ctx.execute().await?;

    let payload = Payload::from(&batches[0], schema, Uuid::default());
    Ok(serde_json::to_value(&payload)?)
}

async fn kafka_handler(ctx: &mut ExecutionContext, event: Value) -> Result<Value> {
    let kafka_event: KafkaEvent = serde_json::from_value(event).unwrap();
    let batch = match kafka::to_batch(kafka_event) {
        Some(batch) => batch,
        None => return Ok(json!("No Kafka input!")),
    };
    let schema = batch.schema();

    ctx.feed_one_source(&vec![vec![batch]]);
    let batches = ctx.execute().await?;

    let payload = Payload::from(&batches[0], schema, Uuid::default());
    Ok(serde_json::to_value(&payload)?)
}

async fn payload_handler(ctx: &mut ExecutionContext, event: Value) -> Result<Value> {
    let (batch, uuid) = Payload::to_batch(event);
    let schema = batch.schema();

    ctx.feed_one_source(&vec![vec![batch]]);
    let batches = ctx.execute().await?;

    let payload = Payload::from(&batches[0], schema, uuid);
    Ok(serde_json::to_value(&payload)?)
}

async fn handler(event: Value, _: Context) -> Result<Value> {
    let mut ctx = init_exec_context!();

    match &ctx.datasource {
        DataSource::Payload => payload_handler(&mut ctx, event).await,
        DataSource::KinesisEvent(_) => kinesis_handler(&mut ctx, event).await,
        DataSource::Json => Ok(event),
        DataSource::KafkaEvent(_) => kafka_handler(&mut ctx, event).await,
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::ExecutionPlan;
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn generic_lambda() -> Result<()> {
        let plan = r#"{"execution_plan":"coalesce_batches_exec","input":{"execution_plan":"memory_exec","schema":{"fields":[{"name":"c1","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c2","data_type":"Float64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"c3","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}],"metadata":{}},"projection":null},"target_batch_size":16384}"#;
        let name = "hello".to_owned();
        let next =
            CloudFunction::Solo("SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836Z".to_owned());
        let datasource = DataSource::Json;

        let plan: Arc<dyn ExecutionPlan> = serde_json::from_str(&plan).unwrap();
        let lambda_context = ExecutionContext {
            plan,
            name,
            next,
            datasource,
        };

        let encoded = lambda_context.marshal(Encoding::Snappy);

        // Configures the cloud environment
        std::env::set_var(config::global("context_name").unwrap(), encoded);

        // First lambda call
        let event = json!({
            "db": "cmsc624"
        });
        assert_eq!(
            handler(event.clone(), Context::default())
                .await
                .expect("expected Ok(_) value"),
            event
        );

        // Second lambda call
        let event = json!({
            "net": "cmsc711"
        });
        assert_eq!(
            handler(event.clone(), Context::default())
                .await
                .expect("expected Ok(_) value"),
            event
        );

        Ok(())
    }
}
