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

use crate::window::*;
use hashring::HashRing;
use log::info;
use runtime::prelude::*;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn get_worker_function(
    metadata: Option<HashMap<String, String>>,
) -> Result<(HashRing<String>, String)> {
    let metadata = metadata.expect("Metadata is missing.");
    let next_function: CloudFunction = serde_json::from_str(
        &metadata
            .get(&format!("workers"))
            .expect("workers is missing."),
    )?;

    let (group_name, group_size) = match &next_function {
        CloudFunction::Lambda(name) => (name.clone(), 1),
        CloudFunction::Group((name, size)) => (name.clone(), *size),
        CloudFunction::None => (String::new(), 0),
    };

    // The *consistent hash* technique distributes the data packets in a time window
    // to the same function name in the function group. Because each function in the
    // function group has a concurrency of *1*, all data packets from the same query
    // can be routed to the same function execution environment.
    let mut ring: HashRing<String> = HashRing::new();
    if group_size == 1 {
        // lambda function concurrency greater than 1
        ring.add(group_name.clone());
    } else if group_size > 1 {
        // each function concurrency in the group is 1
        (0..group_size).for_each(|i| {
            ring.add(format!("{}-{:02}", group_name, i));
        });
    }

    Ok((ring, group_name))
}

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
    let mut source = match payload.datasource {
        Some(DataSource::NexMarkEvent(source)) => source,
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

    // To make data source generator function work generally, we *cannot*
    // use `CONSISTENT_HASH_CONTEXT` from cloud environment. The cloud
    // environment is used to specialize the plan for each function (stage
    // of the query). We WANT to use the same data source function to handle
    // all NexMark queries.
    let (mut ring, group_name) = get_worker_function(payload.metadata)?;

    match source.window {
        StreamWindow::TumblingWindow(Schedule::Seconds(window_size)) => {
            tumbling_window_tasks(
                query_number,
                events,
                sec,
                window_size,
                &mut ring,
                group_name,
            )
            .await?;
        }
        StreamWindow::HoppingWindow((window_size, hop_size)) => {
            hopping_window_tasks(
                query_number,
                events,
                sec,
                window_size,
                hop_size,
                &mut ring,
                group_name,
            )
            .await?;
        }
        StreamWindow::SlidingWindow((_window, _hop)) => {
            unimplemented!();
        }
        StreamWindow::ElementWise => {
            elementwise_tasks(query_number, events, sec, &mut ring, group_name).await?;
        }
        _ => unimplemented!(),
    };

    Ok(json!({"name": &ctx.name, "type": format!("nexmark_bench")}))
}
