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

//! A Query API to associate front-end CLI with back-end code generation and
//! continuous deployment.

use crate::codegen::workspace::Workspace;
use scq_lambda::dataframe::DataSource;
use scq_lambda::error::Result;

/// A SQL query to pull the desired data.
pub trait Query {
    /// Deploy the query to the cloud.
    fn deploy() -> Result<()>;
}

/// An in-application stream represents unbounded data that flows continuously
/// through your application. Therefore, to get result sets from this
/// continuously updating input, you often bound queries using a window defined
/// in terms of time or rows. These are also called windowed SQL.
///
/// - For a row-based windowed query, you specify the window size in terms of
///   the number of rows.
/// - For a time-based windowed query, you specify the window size in terms of
///   time (for example, a one-minute window).
///
/// Reference:
/// https://docs.microsoft.com/en-us/stream-analytics-query/windowing-azure-stream-analytics
pub enum StreamWindow {
    /// A query that aggregates data using distinct time-based windows that open
    /// and close at regular intervals. In this case, each record on an
    /// in-application stream belongs to a specific window. It is processed only
    /// once (when the query processes the window to which the record belongs).
    TumblingWindow(String, u64),
    /// A query that aggregates data continuously, using a fixed time or
    /// rowcount interval.
    SlidingWindow(String, u64),
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
#[allow(dead_code)]
pub struct StreamQuery {
    /// ANSI 2008 SQL standard with extensions.
    /// SQL is a domain-specific language used in programming and designed for
    /// managing data held in a relational database management system, or for
    /// stream processing in a relational data stream management system.
    pub ansi_sql:   String,
    /// The windows group stream elements by time or rows.
    pub window:     StreamWindow,
    /// A streaming data source.
    pub datasource: DataSource,
    /// A Cargo workspace for code generation and compilation.
    pub workspace:  Workspace,
}

/// Batch processing is the processing of a large volume of data all at once.
/// You can store the preceding reference data as an object in Amazon Simple
/// Storage Service (Amazon S3).  ServerlessCQ reads the Amazon S3 object and
/// creates an in-application reference table that you can query in your
/// application code. In your application code, you write a join query to join
/// the in-application stream with the in-application reference table, to obtain
/// more accurate results.
#[allow(dead_code)]
pub struct BatchQuery {}
