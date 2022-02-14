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

mod bitmap;
pub use bitmap::Bitmap;

use crate::encoding::Encoding;
use crate::error::{FlockError, Result};
use crate::runtime::payload::{DataFrame, Payload};
use crate::transmute::*;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow_flight::utils::flight_data_to_arrow_batch;
use datafusion::arrow_flight::FlightData;
use hashbrown::HashMap;
use rayon::prelude::*;
use std::ops::{Deref, DerefMut};
use tokio::task::JoinHandle;

type QueryId = String;
type ShuffleId = usize;

/// The window identifier to identify the window in the global arena.
pub type WindowId = (QueryId, ShuffleId);

/// The aggregator function has three status to determine the next step.
#[derive(PartialEq)]
pub enum HashAggregateStatus {
    /// The window data is not ready to be processed.
    Processed,
    /// The window data is ready to be processed.
    Ready,
    /// The window data has been processed.
    NotReady,
}

/// `Arena` is a global hash map inside the lambda function that is used to
/// aggregate the data frames of the previous stage of dataflow to ensure the
/// integrity of the window data for stream processing.
///
/// # Key-value pairs
/// * The key is the hash value of a SQL query statement concatenated with the
///   query time.
/// * The value is the data frames of the previous stage of dataflow for a given
///   query at a given time wrapped by `WindowSession`.
pub struct Arena(HashMap<WindowId, WindowSession>);

/// `WindowSession` is an abstraction of a temporal window that is used to store
/// the data frames of the previous stage of dataflow to ensure the integrity of
/// the window data for stream processing. Performing operations on the data
/// contained in temporal windows is a common pattern in stream processing.
#[derive(Debug)]
pub struct WindowSession {
    /// The number of data fragments in the window.
    /// [`WindowSession::size`] equals to [`Uuid::seq_len`].
    pub size:           usize,
    /// Aggregate the encoded data frames for the first relation.
    /// https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
    pub r1_flight_data: Vec<Vec<DataFrame>>,
    /// The schema of the first relation.
    pub r1_schema:      Vec<u8>,
    /// Aggregate the encoded data frames for the second relation.
    /// https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/
    pub r2_flight_data: Vec<Vec<DataFrame>>,
    /// The schema of the second relation.
    pub r2_schema:      Vec<u8>,
    /// Bitmap indicating the data existence in the window.
    pub bitmap:         Bitmap,
    /// The compression method.
    pub encoding:       Encoding,
}

impl WindowSession {
    /// Return the schema of data fragments in the temporal window.
    pub fn schema(&self) -> Result<(SchemaRef, Option<SchemaRef>)> {
        if self.r1_schema.is_empty() {
            return Err(FlockError::Internal(
                "Record batches are empty.".to_string(),
            ));
        }

        if self.r2_schema.is_empty() {
            Ok((schema_from_bytes(&self.r1_schema)?, None))
        } else {
            Ok((
                schema_from_bytes(&self.r1_schema)?,
                Some(schema_from_bytes(&self.r2_schema)?),
            ))
        }
    }
}

impl Arena {
    /// Create a new `Arena`.
    pub fn new() -> Arena {
        Arena(HashMap::<WindowId, WindowSession>::new())
    }

    /// Take a window from the arena.
    pub async fn take(&mut self, window_id: &WindowId) -> Result<Vec<Vec<Vec<RecordBatch>>>> {
        let to_batches = |df: Vec<DataFrame>, schema: SchemaRef| -> Vec<RecordBatch> {
            df.into_par_iter()
                .map(|d| {
                    flight_data_to_arrow_batch(
                        &FlightData {
                            data_body:         d.body,
                            data_header:       d.header,
                            app_metadata:      vec![],
                            flight_descriptor: None,
                        },
                        schema.clone(),
                        &[],
                    )
                    .unwrap()
                })
                .collect()
        };

        if let Some(window) = (*self).remove(window_id) {
            let (schema1, schema2) = window.schema()?;

            let mut tasks: Vec<JoinHandle<Vec<Vec<RecordBatch>>>> = vec![];

            let encoding = window.encoding.clone();
            tasks.push(tokio::spawn(async move {
                window
                    .r1_flight_data
                    .into_par_iter()
                    .filter(|d| !d.is_empty())
                    .map(|d| to_batches(unmarshal(d, encoding.clone()), schema1.clone()))
                    .collect()
            }));

            if schema2.is_some() {
                let encoding = window.encoding.clone();
                let schema2 = schema2.unwrap();
                tasks.push(tokio::spawn(async move {
                    window
                        .r2_flight_data
                        .into_par_iter()
                        .filter(|d| !d.is_empty())
                        .map(|d| to_batches(unmarshal(d, encoding.clone()), schema2.clone()))
                        .collect()
                }));
            }

            Ok(futures::future::join_all(tasks)
                .await
                .into_iter()
                .map(|r| r.unwrap())
                .collect())
        } else {
            Ok(vec![vec![], vec![]])
        }
    }

    /// Return the Bitmap reference of the temporal window.
    pub fn get_bitmap(&self, window_id: &WindowId) -> Option<&Bitmap> {
        self.get(window_id).map(|window| &window.bitmap)
    }

    /// Return true if the temporal window is empty.
    pub fn is_complete(&self, window_id: &WindowId) -> bool {
        self.get(window_id)
            .map(|window| window.size == window.r1_flight_data.len())
            .unwrap_or(false)
    }

    /// Collect the data fragments for temporal windows.
    ///
    /// # Arguments
    /// * `payload` - The payload of the data frame.
    ///
    /// # Returns
    /// * Return true if the window data collection is complete, otherwise
    ///   return false. Uuid is also returned no matter whether the window data
    ///   collection is complete.
    pub fn collect(&mut self, payload: Payload) -> HashAggregateStatus {
        let uuid = payload.uuid.clone();
        let window_id = payload.get_window_id();
        match &mut (*self).get_mut(&window_id) {
            Some(window) => {
                assert!(uuid.seq_len == window.size);
                if !window.bitmap.is_set(uuid.seq_num) {
                    window.r1_flight_data.push(payload.data);
                    window.r2_flight_data.push(payload.data2);
                    assert!(window.r1_flight_data.len() == window.r2_flight_data.len());
                    window.bitmap.set(uuid.seq_num);
                    if window.size == window.r1_flight_data.len() {
                        HashAggregateStatus::Ready
                    } else {
                        HashAggregateStatus::NotReady
                    }
                } else {
                    HashAggregateStatus::Processed
                }
            }
            None => {
                let mut window = WindowSession {
                    size:           uuid.seq_len,
                    r1_flight_data: vec![payload.data],
                    r2_flight_data: vec![payload.data2],
                    r1_schema:      payload.schema,
                    r2_schema:      payload.schema2,
                    bitmap:         Bitmap::new(uuid.seq_len + 1), // Starts from 1.
                    encoding:       payload.encoding,
                };
                // SEQ_NUM is used to indicate the data existence in the window via bitmap.
                window.bitmap.set(uuid.seq_num);
                (*self).insert(window_id, window);
                if uuid.seq_len == 1 {
                    HashAggregateStatus::Ready
                } else {
                    HashAggregateStatus::NotReady
                }
            }
        }
    }
}

impl Deref for Arena {
    type Target = HashMap<WindowId, WindowSession>;

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
            let payload = to_payload(&[batch], &[], uuids.get(i + 1), false);
            let status = arena.collect(payload);
            if i < 7 {
                assert!(status == HashAggregateStatus::NotReady);
            } else {
                assert!(status == HashAggregateStatus::Ready);
            }
        });

        let qid = uuids.get(1).qid;
        let window_id = (qid, 0);
        assert!((*arena).get(&window_id).is_some());

        if let Some(window) = (*arena).get(&window_id) {
            assert_eq!(8, window.size);
            assert_eq!(8, window.r1_flight_data.len());
            (0..8).for_each(|i| assert!(window.bitmap.is_set(i + 1)));
        }

        assert_eq!(8, arena.take(&window_id).await?[0].len());
        assert_eq!(0, arena.take(&("no exists".to_owned(), 0)).await?[0].len());

        Ok(())
    }
}
