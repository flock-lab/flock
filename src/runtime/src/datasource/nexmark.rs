// Copyright 2020 UMD Database Group. All Rights Reserved.
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

//! Nexmark benchmark suite

use arrow::json::{self, reader::infer_json_schema};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::query::StreamWindow;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::BufReader;
use std::sync::Arc;

/// A struct to manage all Nexmark info in cloud environment.
#[derive(Default, Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct NexMarkSource {
    /// How long should the query run repeatedly.
    pub seconds: usize,
    /// The windows group stream elements by time or rows.
    pub window:  StreamWindow,
}

impl NexMarkSource {
    /// Fetches data records from Kinesis Data Streams.
    pub fn fetch_data(&self) -> Result<RecordBatch> {
        unimplemented!();
    }
}
