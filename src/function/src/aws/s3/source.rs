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

//! The entry point for the NEXMark benchmark on cloud functions.

use crate::actor::*;
use chrono::Utc;
use datafusion::physical_plan::Partitioning;
use driver::deploy::common::*;
use log::info;
use runtime::prelude::*;
use rusoto_core::ByteStream;
use rusoto_s3::{PutObjectRequest, S3};
use serde_json::json;
use serde_json::Value;
use std::sync::Arc;

/// The endpoint of the data source generator function invocation. The data
/// source generator function is responsible for generating the data packets for
/// the query no matter what type of query it is.
///
/// # Arguments
/// * `ctx` - The runtime context of the function.
/// * `payload` - The payload of the function.
///
/// # Returns
/// A JSON object that contains the return value of the function invocation.
pub async fn handler(_ctx: &ExecutionContext, payload: Payload) -> Result<Value> {
    // Copy data source from the payload.
    let mut source = match payload.datasource.clone() {
        DataSource::S3(source) => source,
        _ => unreachable!(),
    };

    // Each source function is a data generator.
    let gen = source.config.get_as_or("threads", 1);
    let sec = source.config.get_as_or("seconds", 10);
    let eps = source.config.get_as_or("events-per-second", 1000);

    source.config.insert("threads", format!("{}", 1));
    source
        .config
        .insert("events-per-second", format!("{}", eps / gen));
    assert!(eps / gen > 0);

    let events = Arc::new(source.generate_data()?);
    let query_number = payload.query_number.expect("Query number is missing.");

    info!("Nexmark Benchmark [S3]: Query {:?}", query_number);
    info!("{:?}", source);
    info!("[OK] Generate nexmark events.");

    let (mut ring, group_name) = infer_actor_info(&payload.metadata)?;
    let uuid = UuidBuilder::new_with_ts(&group_name, Utc::now().timestamp(), 1).next();
    let sync = true;

    let function_name = if ring.len() == 1 {
        group_name.clone()
    } else {
        ring.get(&uuid.tid).expect("hash ring failure.").to_string()
    };

    let bytes = match source.window {
        StreamWindow::HoppingWindow(..) | StreamWindow::TumblingWindow(..) => {
            assert!(sec == 10);
            let mut r1 = vec![];
            let mut r2 = vec![];
            for epoch in 0..sec {
                let (mut a, mut b) = events.select_event_to_batches(
                    epoch,
                    0, // generator id
                    payload.query_number,
                    sync,
                )?;
                r1.append(Arc::get_mut(&mut a).unwrap());
                r2.append(Arc::get_mut(&mut b).unwrap());
            }
            if r1.len() != 1 {
                r1 = LambdaExecutor::repartition(r1, Partitioning::RoundRobinBatch(1)).await?;
            }
            if r2.len() != 1 {
                r2 = LambdaExecutor::repartition(r2, Partitioning::RoundRobinBatch(1)).await?;
            }
            assert!(r1.len() == 1);
            assert!(r2.len() == 1);
            serde_json::to_vec(&to_payload(&r1[0], &r2[0], uuid.clone(), sync))?
        }
        StreamWindow::ElementWise => {
            assert!(sec == 1);
            serde_json::to_vec(&events.select_event_to_payload(
                0,
                0,
                payload.query_number,
                uuid.clone(),
                sync,
            )?)?
        }
        _ => unimplemented!(),
    };

    info!(
        "[OK] {} function payload: {} bytes",
        function_name,
        bytes.len()
    );
    info!("Writing {} function payload to S3...", function_name);
    let s3_key = format!("{}_payload", function_name);
    FLOCK_S3_CLIENT
        .put_object(PutObjectRequest {
            bucket: FLOCK_S3_BUCKET.clone(),
            key: s3_key.clone(),
            body: Some(ByteStream::from(bytes)),
            ..Default::default()
        })
        .await
        .map_err(|e| FlockError::AWS(e.to_string()))?;

    info!("[OK] {} function payload written to S3.", function_name);

    Ok(json! ({
        "function": function_name.clone(),
        "bucket": FLOCK_S3_BUCKET.clone(),
        "key": s3_key.clone(),
        "uuid": serde_json::to_string(&uuid)?,
        "encoding": serde_json::to_string(&Encoding::default())?,
    }))
}
