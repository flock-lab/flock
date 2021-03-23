// Copyright 2021 UMD Database Group. All Rights Reserved.
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

//! The global data structure inside the lambda function is used to aggregate
//! the data frames of the previous stage of dataflow to ensure the integrity of
//! the window data for stream processing.

use arrow::record_batch::RecordBatch;
use bitmap::Bitmap;
use dashmap::DashMap;

/// Reassemble data fragments to separate window sessions.
pub struct Arena(DashMap<String, WindowSession>);

/// In time-streaming scenarios, performing operations on the data contained in
/// temporal windows is a common pattern.
#[derive(Debug)]
pub struct WindowSession {
    /// The window size (# data fragments / payloads).
    /// Note: [size] == [Uuid.seq_len]
    pub size:     usize,
    /// Reassembled record batches.
    pub batches:  Vec<RecordBatch>,
    /// Validity bitmap is to track which data fragments in the window have not
    /// been received yet.
    pub validity: Bitmap,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
}

pub mod bitmap;
