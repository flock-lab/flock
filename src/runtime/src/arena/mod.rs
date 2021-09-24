// Copyright (c) 2021 UMD Database Group. All Rights Reserved.
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

/// `DashMap` is a thread-safe hash map inside the lambda function that is used
/// to aggregate the data frames of the previous stage of dataflow to ensure the
/// integrity of the window data for stream processing.
///
/// # Key-value pairs
/// * The key is the hash value of a SQL query statement concatenated with the
///   query time.
/// * The value is the data frames of the previous stage of dataflow for a given
///   query at a given time wrapped by `WindowSession`.
pub struct Arena(DashMap<String, WindowSession>);

/// `WindowSession` is an abstraction of a temporal window that is used to store
/// the data frames of the previous stage of dataflow to ensure the integrity of
/// the window data for stream processing. Performing operations on the data
/// contained in temporal windows is a common pattern in stream processing.
#[derive(Debug)]
pub struct WindowSession {
    /// The number of data fragments in the payload.
    /// [`WindowSession::size`] equals to [`Uuid::seq_len`].
    pub size:    usize,
    /// Reassembled record batches in the temporal window.
    pub batches: Vec<Vec<RecordBatch>>,
    /// Bitmap indicating the existence of data fragments in the temporal
    /// window.
    pub bitmap:  Bitmap,
}

impl WindowSession {
    /// Return the schema of data fragments in the temporal window.
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
    /// Create a new `Arena`.
    pub fn new() -> Arena {
        Arena(DashMap::<String, WindowSession>::new())
    }

    /// Get the data fragments in the temporal window via the key.
    pub fn batches(&mut self, tid: String) -> Vec<Vec<RecordBatch>> {
        if let Some((_, v)) = (*self).remove(&tid) {
            v.batches
        } else {
            vec![]
        }
    }

    /// Reassemble the data fragments in the temporal window.
    ///
    /// # Arguments
    /// * `event` - the serialized data fragments from the previous stage of
    /// dataflow.
    ///
    /// # Returns
    /// * Return true if the window data collection is complete, otherwise
    ///   return false. Uuid is also returned no matter whether the window data
    ///   collection is complete.
    pub fn reassemble(&mut self, event: Value) -> (bool, Uuid) {
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
                (*self).insert(uuid.tid.clone(), window);
            }
        }
        (ready, uuid)
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
            let (ready, _) = arena.reassemble(value);
            if i < 7 {
                assert_eq!(false, ready);
            } else {
                assert_eq!(true, ready);
            }
        });

        let tid = uuids.get(0).tid;
        assert!((*arena).get(&tid).is_some());

        if let Some(window) = (*arena).get(&tid) {
            assert_eq!(8, window.size);
            assert_eq!(8, window.batches.len());
            (0..8).for_each(|i| assert_eq!(true, window.bitmap.is_set(i)));
        }

        assert_eq!(8, arena.batches(tid).len());
        assert_eq!(0, arena.batches("no exists".to_string()).len());

        Ok(())
    }
}

pub mod bitmap;
