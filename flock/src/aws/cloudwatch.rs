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

//! This crate contains all wrapped functions of the AWS CloudWatch services.

use crate::configs::*;
use crate::error::{FlockError, Result};
use chrono::Duration as Delta;
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use humantime::parse_duration;
use log::info;
use rusoto_logs::{
    CloudWatchLogs, CloudWatchLogsClient, DescribeLogGroupsRequest, FilterLogEventsRequest,
};
use std::time::Duration;

/// AWS Response for CloudWatch logs.
pub enum AWSResponse {
    /// The token to use when requesting the next set of items. The token
    /// expires after 24 hours.
    Token(String),
    /// The last time the log group was updated.
    LastLog(Option<i64>),
}

/// Fetches the lambda function's latest log.
///
/// # Arguments
/// * `group` - The name of the log group.
/// * `mtime` - The time of the latest log.
pub async fn fetch(group: &str, mtime: Duration) -> Result<()> {
    let mut logged = false;
    let timeout = parse_duration("1min").unwrap();
    let sleep_for = parse_duration("5s").ok();
    let mut token: Option<String> = None;
    let mut req = create_filter_request(group, mtime, None, token);
    loop {
        if logged {
            break;
        }

        match fetch_logs(&FLOCK_WATCHLOGS_CLIENT, req, timeout)
            .await
            .map_err(|e| FlockError::Internal(e.to_string()))?
        {
            AWSResponse::Token(x) => {
                info!("Got a Token response");
                logged = true;
                token = Some(x);
                req = create_filter_request(group, mtime, None, token);
            }
            AWSResponse::LastLog(t) => match sleep_for {
                Some(x) => {
                    info!("Got a lastlog response");
                    token = None;
                    req = create_filter_from_timestamp(group, t, None, token);
                    info!("Waiting {:?} before requesting logs again...", x);
                    tokio::time::sleep(x).await;
                }
                None => break,
            },
        };
    }

    Ok(())
}

/// Calculates the start time of the log request.
/// The start time is the time of the latest log minus the given duration.
///
/// # Arguments
/// * `from` - The time of the latest log.
/// * `delta` - The duration to subtract from the latest log.
///
/// # Returns
/// The start time of the log request.
fn calculate_start_time(from: DateTime<Local>, delta: Duration) -> Option<i64> {
    let chrono_delta = Delta::from_std(delta).unwrap();
    let start_time = from.checked_sub_signed(chrono_delta).unwrap();
    let utc_time = DateTime::<Utc>::from_utc(start_time.naive_utc(), Utc);
    Some(utc_time.timestamp_millis())
}

/// Prints the timestamp in a human readable format.
fn print_date(time: Option<i64>) -> String {
    match time {
        Some(x) => DateTime::<Local>::from(DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp(x / 1000, 0),
            Utc,
        ))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string(),
        None => "".to_owned(),
    }
}

/// Creates a log filter request for the given log group name.
///
/// # Arguments
/// * `group` - The name of the log group.
/// * `start` - The start time of the log request.
/// * `filter` - The filter of the log request.
/// * `token` - The token of the log request.
///
/// # Returns
/// The log filter request.
pub fn create_filter_request(
    group: &str,
    start: Duration,
    filter: Option<String>,
    token: Option<String>,
) -> FilterLogEventsRequest {
    FilterLogEventsRequest {
        start_time: calculate_start_time(Local::now(), start),
        next_token: token,
        limit: Some(100),
        filter_pattern: filter,
        log_group_name: group.to_string(),
        ..Default::default()
    }
}

/// Creates a log filter request for the given log group name.
///
/// # Arguments
/// * `group` - The name of the log group.
/// * `start` - The start time of the log request.
/// * `filter` - The filter of the log request.
/// * `token` - The token of the log request.
pub fn create_filter_from_timestamp(
    group: &str,
    start: Option<i64>,
    filter: Option<String>,
    token: Option<String>,
) -> FilterLogEventsRequest {
    FilterLogEventsRequest {
        start_time: start,
        next_token: token,
        limit: Some(100),
        filter_pattern: filter,
        log_group_name: group.to_string(),
        ..Default::default()
    }
}

/// Fetches the log events from the given log group.
///
/// # Arguments
/// * `client` - The client to use to fetch the log events.
/// * `req` - The request to fetch the log events.
/// * `timeout` - The timeout of the request.
///
/// # Returns
/// The log events.
pub async fn fetch_logs(
    client: &CloudWatchLogsClient,
    req: FilterLogEventsRequest,
    timeout: Duration,
) -> Result<AWSResponse> {
    info!("Sending log request {:?}", &req);
    match tokio::time::timeout(timeout, client.filter_log_events(req.clone()))
        .await
        .map_err(|e| FlockError::Internal(format!("Error fetching logs: {}", e)))?
    {
        Ok(response) => {
            info!("[OK] Got response from AWS CloudWatch Logs.");
            let mut events = response.events.unwrap();
            events.sort_by_key(|x| x.timestamp.map_or(-1, |x| x));
            for event in &events {
                let message = event.message.as_ref().map_or("".into(), |x| x.clone());
                print!("{} {}", print_date(event.timestamp), message);
            }
            let last = events.last().map(|x| x.timestamp);
            match response.next_token {
                Some(x) => Ok(AWSResponse::Token(x)),
                None => match last.flatten() {
                    Some(t) => Ok(AWSResponse::LastLog(Some(t))),
                    None => Ok(AWSResponse::LastLog(req.start_time)),
                },
            }
        }
        Err(x) => Err(FlockError::Internal(format!("Error fetching logs: {}", x))),
    }
}

/// Lists all log groups in the current region.
///
/// # Arguments
/// * `client` - The client to use to list the log groups.
pub async fn list_log_groups(client: &CloudWatchLogsClient) -> Result<()> {
    let mut req = DescribeLogGroupsRequest::default();
    loop {
        info!("Sending list log groups request {:?}", &req);
        let resp = client
            .describe_log_groups(req)
            .await
            .map_err(|e| FlockError::Internal(format!("Error listing log groups: {}", e)))?;
        match resp.log_groups {
            Some(x) => {
                for group in x {
                    println!("{}", group.log_group_name.unwrap())
                }
            }
            None => break,
        }
        match resp.next_token {
            Some(x) => {
                req = DescribeLogGroupsRequest::default();
                req.next_token = Some(x)
            }
            None => break,
        }
    }
    Ok(())
}
