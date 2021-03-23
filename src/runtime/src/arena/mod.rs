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

use crate::error::{Result, SquirtleError};
use crate::payload::{Payload, Uuid, UuidBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bitmap::Bitmap;
use dashmap::DashMap;
use serde_json::Value;
use std::ops::{Deref, DerefMut};

/// Reassemble data fragments to separate window sessions.
pub struct Arena(DashMap<String, WindowSession>);

/// In time-streaming scenarios, performing operations on the data contained in
/// temporal windows is a common pattern.
#[derive(Debug)]
pub struct WindowSession {
    /// The window size (# data fragments / payloads).
    /// Note: [size] == [Uuid.seq_len]
    pub size:    usize,
    /// Reassembled record batches.
    pub batches: Vec<Vec<RecordBatch>>,
    /// Validity bitmap is to track which data fragments in the window have not
    /// been received yet.
    pub bitmap:  Bitmap,
}

impl WindowSession {
    /// Return the data schema for the current window session.
    pub fn schema(&self) -> Result<SchemaRef> {
        if self.batches.is_empty() || self.batches[0].is_empty() {
            return Err(SquirtleError::Internal(
                "Record batches are empty.".to_string(),
            ));
        }
        Ok(self.batches[0][0].schema())
    }
}

impl Arena {
    /// Create a new [`Arena`].
    pub fn new() -> Arena {
        Arena(DashMap::<String, WindowSession>::new())
    }

    /// Ressemble the payload to a specific window session.
    ///
    /// Return true, if the window data collection is complete,
    pub fn reassemble(&mut self, event: Value) -> bool {
        let mut ready = false;
        let (fragment, uuid) = Payload::to_batch(event);
        match &mut (*self).get_mut(&uuid.tid) {
            Some(window) => {
                assert!(uuid.seq_len == window.size);
                if !window.bitmap.is_set(uuid.seq_num) {
                    window.batches.push(fragment);
                    window.bitmap.set(uuid.seq_num);
                    ready = window.size == window.batches.len();
                }
            }
            None => {
                let mut window = WindowSession {
                    size:    uuid.seq_len,
                    batches: vec![fragment],
                    bitmap:  Bitmap::new(uuid.seq_len),
                };

                ready = window.size == 1;
                window.bitmap.set(uuid.seq_num);
                (*self).insert(uuid.tid, window);
            }
        }
        ready
    }
}

impl Deref for Arena {
    type Target = DashMap<String, WindowSession>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Arena {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::Encoding;
    use crate::error::Result;
    use arrow::csv;
    use arrow::datatypes::{DataType, Field, Schema};

    fn init_batches() -> Vec<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let records: &[u8] =
            include_str!("../../../test/data/uk_cities_with_headers.csv").as_bytes();
        let mut reader = csv::Reader::new(
            records,
            std::sync::Arc::new(schema),
            true,
            None,
            5,
            None,
            None,
        );

        let mut batches = vec![];
        while let Some(Ok(batch)) = reader.next() {
            batches.push(batch);
        }
        batches
    }

    #[tokio::test]
    async fn test_arena() -> Result<()> {
        let batches = init_batches();
        assert_eq!(8, batches.len());

        let uuids = UuidBuilder::new(
            "SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836",
            batches.len(),
        );

        let mut arena = Arena::new();
        batches.into_iter().enumerate().for_each(|(i, batch)| {
            let value = Payload::to_value(&[batch], uuids.get(i), Encoding::default());
            if i < 7 {
                assert_eq!(false, arena.reassemble(value));
            } else {
                assert_eq!(true, arena.reassemble(value));
            }
        });

        let tid = uuids.get(0).tid;
        assert!((*arena).get(&tid).is_some());

        if let Some(window) = (*arena).get(&tid) {
            assert_eq!(8, window.size);
            assert_eq!(8, window.batches.len());
            (0..8).for_each(|i| assert_eq!(true, window.bitmap.is_set(i)));
        }

        Ok(())
    }
}

pub mod bitmap;
