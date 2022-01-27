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

//! Evaluate a new way to serialize the payload.

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::json::reader::Decoder;
use datafusion::arrow::record_batch::RecordBatch;
use flock::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{map::Map as JsonMap, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// A payload alternative to the old one.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsonPayload {
    pub data:         Vec<JsonMap<String, Value>>,
    pub schema:       Vec<u8>,
    pub data2:        Vec<JsonMap<String, Value>>,
    pub schema2:      Vec<u8>,
    pub uuid:         Uuid,
    pub encoding:     Encoding,
    pub datasource:   DataSource,
    pub query_number: Option<usize>,
    pub shuffle_id:   Option<usize>,
    pub metadata:     Option<HashMap<String, String>>,
}

impl JsonPayload {
    /// Convert incoming payload to record batches in Arrow.
    #[allow(dead_code)]
    pub fn record_batches(self) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let to_batches =
            |map: Vec<JsonMap<String, Value>>, schema: Arc<Schema>| -> Vec<RecordBatch> {
                let decoder = Decoder::new(schema, 1024, None);
                map.into_par_iter()
                    .map(|value| {
                        let mut value_iter = vec![Ok(serde_json::Value::Object(value))].into_iter();
                        decoder.next_batch(&mut value_iter).unwrap().unwrap()
                    })
                    .collect()
            };

        let mut res = (vec![], vec![]);
        if !self.data.is_empty() {
            let schema = schema_from_bytes(&self.schema).unwrap();
            res.0 = to_batches(self.data, schema);
        }
        if !self.data2.is_empty() {
            let schema = schema_from_bytes(&self.schema2).unwrap();
            res.1 = to_batches(self.data2, schema);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::csv;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::json::writer::record_batches_to_json_rows;
    use std::time::Instant;

    fn init_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tripduration", DataType::Utf8, false),
            Field::new("starttime", DataType::Utf8, false),
            Field::new("stoptime", DataType::Utf8, false),
            Field::new("start station id", DataType::Int32, false),
            Field::new("start station name", DataType::Utf8, false),
            Field::new("start station latitude", DataType::Float64, false),
            Field::new("start station longitude", DataType::Float64, false),
            Field::new("end station id", DataType::Int32, false),
            Field::new("end station name", DataType::Utf8, false),
            Field::new("end station latitude", DataType::Float64, false),
            Field::new("end station longitude", DataType::Float64, false),
            Field::new("bikeid", DataType::Int32, false),
            Field::new("usertype", DataType::Utf8, false),
            Field::new("birth year", DataType::Int32, false),
            Field::new("gender", DataType::Int8, false),
        ]));

        let batch_size = 1024;
        let records: &[u8] =
            include_str!("../../../flock/src/tests/data/JC-202011-citibike-tripdata.csv")
                .as_bytes();
        let mut reader = csv::Reader::new(records, schema, true, None, batch_size, None, None);

        let mut batches = vec![];
        while let Some(Ok(batch)) = reader.next() {
            batches.push(batch);
        }
        batches
    }

    #[tokio::test]
    async fn test_payload() {
        let batches = init_batches();
        let schema = batches[0].schema();

        // Serialize record batches to json.
        let map = record_batches_to_json_rows(&batches);

        // Deserialize json to record batches.
        let decoder = Decoder::new(schema.clone(), 1024, None);
        assert_eq!(decoder.schema(), schema);

        let mut value_iter = map.into_iter().map(|x| Ok(serde_json::Value::Object(x)));
        let mut i = 0;
        loop {
            let batch = decoder.next_batch(&mut value_iter).unwrap();
            if batch.is_none() {
                break;
            }
            let batch = batch.unwrap();
            assert_eq!(batches[i], batch);
            i += 1;
        }
    }

    #[tokio::test]
    async fn compare_serde_json_payload() {
        let batches_1 = init_batches();
        let schema_1 = batches_1[0].schema();

        let batches_2 = init_batches();
        let schema_2 = batches_2[0].schema();

        // Json payload.
        {
            let mut payload = JsonPayload::default();

            let now = Instant::now();
            payload.data = record_batches_to_json_rows(&batches_1);
            payload.schema = schema_to_bytes(schema_1);
            payload.data2 = record_batches_to_json_rows(&batches_2);
            payload.schema2 = schema_to_bytes(schema_2);
            let ser_payload = serde_json::to_vec(&payload).unwrap();
            println!(
                "Json Payload: {} bytes, Serialize: {} ms",
                ser_payload.len(),
                now.elapsed().as_millis()
            );

            let now = Instant::now();
            let payload: JsonPayload = serde_json::from_slice(&ser_payload).unwrap();
            let (batches_1, batches_2) = payload.record_batches();
            println!(
                "Json Payload: {} bytes, Deserialize: {} ms",
                ser_payload.len(),
                now.elapsed().as_millis()
            );

            println!(
                "batches 1's rows: {}",
                batches_1.iter().map(|x| x.num_rows()).sum::<usize>()
            );
            println!(
                "batches 2's rows: {}",
                batches_2.iter().map(|x| x.num_rows()).sum::<usize>()
            );
            assert_eq!(batches_1, batches_2);
        }

        // Arrrow Flight Payload.
        {
            let now = Instant::now();
            let payload = to_payload(&batches_1, &batches_2, Uuid::default(), true);
            let ser_payload = serde_json::to_vec(&payload).unwrap();
            println!(
                "Arrow Flight Payload: {} bytes, Serialize: {} ms",
                ser_payload.len(),
                now.elapsed().as_millis()
            );

            let now = Instant::now();
            let payload: Payload = serde_json::from_slice(&ser_payload).unwrap();
            let (batches_1, batches_2) = payload.to_record_batch();
            println!(
                "Arrow Flight Payload: {} bytes, Deserialize: {} ms",
                ser_payload.len(),
                now.elapsed().as_millis()
            );
            println!(
                "batches 1's rows: {}",
                batches_1.iter().map(|x| x.num_rows()).sum::<usize>()
            );
            println!(
                "batches 2's rows: {}",
                batches_2.iter().map(|x| x.num_rows()).sum::<usize>()
            );
            assert_eq!(batches_1, batches_2);
        }
    }
}
