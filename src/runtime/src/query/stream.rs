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

//! An in-application stream represents unbounded data that flows continuously
//! through your application. Therefore, to get result sets from this
//! continuously updating input, you often bound queries using a window defined
//! in terms of time or rows. These are also called windowed SQL.
//!
//! - For a row-based windowed query, you specify the window size in terms of
//!   the number of rows.
//! - For a time-based windowed query, you specify the window size in terms of
//!   time (for example, a one-minute window).
//!
//! Schedule expressions using rate or cron:
//! <https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents-expressions.html>
//!
//! Reference:
//! <https://docs.microsoft.com/en-us/stream-analytics-query/windowing-azure-stream-analytics>

use super::Query;
use crate::datasource::DataSource;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;

/// You can set up a rule to run an AWS Lambda function on a schedule.
#[rustfmt::skip]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum Schedule {
    /// Where Unit can be second(s), minute(s), hour(s), or day(s). For a singular value
    /// the unit must be singular (for example,rate(1 day)), otherwise plural
    /// (for example, rate(5 days)).
    ///
    /// # Example
    ///
    /// | Frequency        | Expression      |
    /// |------------------|-----------------|
    /// | Every 1 seconds  | rate(1 seconds) |
    /// | Every 5 minutes  | rate(5 minutes) |
    /// | Every hour       | rate(1 hour)    |
    /// | Every seven days | rate(7 days)    |
    ///
    /// Standard rate for frequencies of up to once per minute.
    Rate(String),
    /// Where Unit can and only can be second(s),
    Seconds(i64),
    /// Cron expressions have the following format:
    ///
    /// Cron(`Minutes` `Hours` `Day-of-month` `Month` `Day-of-week` `Year`)
    ///
    /// # Example
    ///
    /// | Frequency                                            | Expression                   |
    /// |------------------------------------------------------|------------------------------|
    /// | 10:15 AM (UTC) every day                             | cron(15 10 * * ? *)          |
    /// | 6:00 PM Monday through Friday                        | cron(0 18 ? * MON-FRI *)     |
    /// | 8:00 AM on the first day of the month                | cron(0 8 1 * ? *)            |
    /// | Every 10 min on weekdays                             | cron(0/10 * ? * MON-FRI *)   |
    /// | Every 5 minutes between 8:00 AM and 5:55 PM weekdays | cron(0/5 8-17 ? * MON-FRI *) |
    /// | 9:00 AM on the first Monday of each month            | cron(0 9 ? * 2#1 *)          |
    ///
    /// Cron expressionsfor frequencies of up to once per minute.
    Cron(String),
    /// The window size in terms of the number of rows.
    Rows(u64),
}

/// A enum `StreamWindow` to define different window types.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum StreamWindow {
    /// A query that aggregates data using distinct time-based windows that open
    /// and close at regular intervals. In this case, each record on an
    /// in-application stream belongs to a specific window. It is processed only
    /// once (when the query processes the window to which the record belongs).
    TumblingWindow(Schedule),
    /// A query that aggregates data continuously, using a fixed time or
    /// rowcount interval.
    SlidingWindow(Schedule),
    /// Session windows group events that arrive at similar times, filtering out
    /// periods of time where there is no data. Session window function has
    /// three main parameters: timeout, maximum duration, and partitioning key
    /// (optional).
    SessionWindow,
    /// Stagger window is a windowing method that is suited for analyzing
    /// groups of data that arrive at inconsistent times. It is well suited for
    /// any time-series analytics use case, such as a set of related sales or
    /// log records. Stagger windows address the issue of related records not
    /// falling into the same time-restricted window, such as when tumbling
    /// windows were used.
    StaggerWinodw,
}

/// SQL queries in your application code execute continuously over
/// in-application streams.
#[derive(Debug)]
pub struct StreamQuery {
    /// ANSI 2008 SQL standard with extensions.
    /// SQL is a domain-specific language used in programming and designed for
    /// managing data held in a relational database management system, or for
    /// stream processing in a relational data stream management system.
    pub ansi_sql:   String,
    /// A schema that is the skeleton structure that represents the logical view
    /// of streaming data.
    pub schema:     Option<SchemaRef>,
    /// The windows group stream elements by time or rows.
    pub window:     StreamWindow,
    /// A streaming data source.
    pub datasource: DataSource,
}

impl StreamQuery {
    /// Returns the time window for a given query.
    pub fn window(&self) -> &StreamWindow {
        &self.window
    }
}

impl Query for StreamQuery {
    /// Returns a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns a SQL query.
    fn sql(&self) -> &String {
        &self.ansi_sql
    }

    /// Returns the data schema for a given query.
    fn schema(&self) -> &Option<SchemaRef> {
        &self.schema
    }

    /// Returns the entire physical plan for a given query.
    fn plan(&self) -> Arc<dyn ExecutionPlan> {
        unimplemented!();
    }

    /// Returns the data source for a given query.
    fn datasource(&self) -> &DataSource {
        &self.datasource
    }
}