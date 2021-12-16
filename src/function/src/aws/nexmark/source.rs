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

use crate::window::*;
use log::info;
use runtime::prelude::*;
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
pub async fn handler(ctx: &ExecutionContext, payload: Payload) -> Result<Value> {
    // Copy data source from the payload.
    let mut source = match payload.datasource.clone() {
        DataSource::NEXMarkEvent(source) => source,
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

    info!("Nexmark Benchmark: Query {:?}", query_number);
    info!("{:?}", source);
    info!("[OK] Generate nexmark events.");

    match source.window {
        StreamWindow::TumblingWindow(Schedule::Seconds(window_size)) => {
            tumbling_window_tasks(payload, events, sec, window_size).await?;
        }
        StreamWindow::HoppingWindow((window_size, hop_size)) => {
            hopping_window_tasks(payload, events, sec, window_size, hop_size).await?;
        }
        StreamWindow::ElementWise => {
            elementwise_tasks(payload, events, sec).await?;
        }
        _ => unimplemented!(),
    };

    Ok(json!({"name": &ctx.name, "type": "nexmark_bench".to_string()}))
}
