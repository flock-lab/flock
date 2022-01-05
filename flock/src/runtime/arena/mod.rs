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

//! The global data structure inside the lambda function is used to aggregate
//! the data frames of the previous stage of dataflow to ensure the integrity of
//! the window data for stream processing.

use crate::error::{FlockError, Result};
use crate::runtime::payload::{Payload, Uuid};
use bitmap::Bitmap;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
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
    /// The number of data fragments in the window.
    /// [`WindowSession::size`] equals to [`Uuid::seq_len`].
    pub size:       usize,
    /// Aggregate record batches for the first relation.
    pub r1_records: Vec<Vec<RecordBatch>>,
    /// Aggregate record batches for the second relation.
    pub r2_records: Vec<Vec<RecordBatch>>,
    /// Bitmap indicating the data existence in the window.
    pub bitmap:     Bitmap,
}

impl WindowSession {
    /// Return the schema of data fragments in the temporal window.
    pub fn schema(&self) -> Result<(SchemaRef, Option<SchemaRef>)> {
        if self.r1_records.is_empty() || self.r1_records[0].is_empty() {
            return Err(FlockError::Internal(
                "Record batches are empty.".to_string(),
            ));
        }
        if !self.r2_records.is_empty() && !self.r2_records[0].is_empty() {
            Ok((self.r1_records[0][0].schema(), None))
        } else {
            Ok((
                self.r1_records[0][0].schema(),
                Some(self.r2_records[0][0].schema()),
            ))
        }
    }
}

impl Arena {
    /// Create a new `Arena`.
    pub fn new() -> Arena {
        Arena(DashMap::<String, WindowSession>::new())
    }

    /// Get the data fragments in the temporal window via the key.
    pub fn batches(&mut self, tid: String) -> Vec<Vec<Vec<RecordBatch>>> {
        if let Some((_, window)) = (*self).remove(&tid) {
            vec![window.r1_records, window.r2_records]
        } else {
            vec![vec![], vec![]]
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
    pub fn reassemble(&mut self, event: Payload) -> (bool, Uuid) {
        let mut ready = false;
        let (r1, r2, uuid) = event.to_record_batch();
        match &mut (*self).get_mut(&uuid.tid) {
            Some(window) => {
                assert!(uuid.seq_len == window.size);
                if !window.bitmap.is_set(uuid.seq_num) {
                    window.r1_records.push(r1);
                    window.r2_records.push(r2);
                    assert!(window.r1_records.len() == window.r2_records.len());
                    window.bitmap.set(uuid.seq_num);
                    ready = window.size == window.r1_records.len();
                }
            }
            None => {
                let mut window = WindowSession {
                    size:       uuid.seq_len,
                    r1_records: vec![r1],
                    r2_records: vec![r2],
                    bitmap:     Bitmap::new(uuid.seq_len),
                };
                // SEQ_NUM is used to indicate the data existence in the window via bitmap.
                window.bitmap.set(uuid.seq_num);
                ready = window.size == 1;
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
    use crate::error::Result;
    use crate::runtime::payload::UuidBuilder;
    use crate::transmute::to_payload;
    use datafusion::arrow::csv;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn init_batches() -> Vec<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);

        let records: &[u8] = include_str!("../../tests/data/uk_cities_with_headers.csv").as_bytes();
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

        let uuids = UuidBuilder::new_with_ts(
            "SX72HzqFz1Qij4bP-00-2021-01-28T19:27:50.298504836",
            1024,
            batches.len(),
        );

        let mut arena = Arena::new();
        batches.into_iter().enumerate().for_each(|(i, batch)| {
            let payload = to_payload(&[batch], &[], uuids.get(i), false);
            let (ready, _) = arena.reassemble(payload);
            if i < 7 {
                assert!(!ready);
            } else {
                assert!(ready);
            }
        });

        let tid = uuids.get(0).tid;
        assert!((*arena).get(&tid).is_some());

        if let Some(window) = (*arena).get(&tid) {
            assert_eq!(8, window.size);
            assert_eq!(8, window.r1_records.len());
            (0..8).for_each(|i| assert!(window.bitmap.is_set(i)));
        }

        assert_eq!(8, arena.batches(tid)[0].len());
        assert_eq!(0, arena.batches("no exists".to_string())[0].len());

        Ok(())
    }
}

pub mod bitmap;
