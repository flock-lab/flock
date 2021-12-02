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
// Only bring in dependencies for the repl when the cli feature is enabled.

//! This module contains various utility functions.

use crate::encoding::Encoding;
use crate::error::{FlockError, Result};
use crate::payload::{DataFrame, Payload, Uuid};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::flight_data_from_arrow_batch;
use arrow_flight::FlightData;
use arrow_flight::SchemaAsIpc;
use rayon::prelude::*;
use serde_json::Value;
use std::sync::Arc;

/// Deserialize `DataFrame` from cloud functions.
pub fn unmarshal(data: Vec<DataFrame>, encoding: Encoding) -> Vec<DataFrame> {
    match encoding {
        Encoding::Snappy | Encoding::Lz4 | Encoding::Zstd => data
            .par_iter()
            .map(|d| DataFrame {
                header: encoding.decompress(&d.header),
                body:   encoding.decompress(&d.body),
            })
            .collect(),
        Encoding::None => data,
        _ => unimplemented!(),
    }
}

/// Serialize the schema
pub fn schema_to_bytes(schema: SchemaRef) -> Vec<u8> {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();
    flight_data.data_header
}

/// Deserialize the schema
pub fn schema_from_bytes(bytes: &[u8]) -> Result<Arc<Schema>> {
    let schema = arrow::ipc::convert::schema_from_bytes(bytes).map_err(FlockError::Arrow)?;
    Ok(Arc::new(schema))
}

/// Convert incoming payload to record batch in Arrow.
pub fn to_batch(event: Value) -> (Vec<RecordBatch>, Vec<RecordBatch>, Uuid) {
    let payload: Payload = serde_json::from_value(event).unwrap();
    payload.to_record_batch()
}

/// Convert record batch to payload for network transmission.
pub fn to_value(batches: &[RecordBatch], uuid: Uuid, encoding: Encoding) -> Value {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let data_frames = batches
        .par_iter()
        .map(|b| {
            let (_, flight_data) = flight_data_from_arrow_batch(b, &options);
            if encoding != Encoding::None {
                DataFrame {
                    header: encoding.compress(&flight_data.data_header),
                    body:   encoding.compress(&flight_data.data_body),
                }
            } else {
                DataFrame {
                    header: flight_data.data_header,
                    body:   flight_data.data_body,
                }
            }
        })
        .collect();

    serde_json::to_value(&Payload {
        data: data_frames,
        schema: schema_to_bytes(batches[0].schema()),
        uuid,
        encoding,
        ..Default::default()
    })
    .unwrap()
}

/// Convert record batches to payload using the default encoding.
pub fn to_payload(batch1: &[RecordBatch], batch2: &[RecordBatch], uuid: Uuid) -> Payload {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let encoding = Encoding::default();
    let dataframe = |batches: &[RecordBatch]| -> Vec<DataFrame> {
        batches
            .par_iter()
            .map(|b| {
                let (_, flight_data) = flight_data_from_arrow_batch(b, &options);
                if encoding != Encoding::None {
                    DataFrame {
                        header: encoding.compress(&flight_data.data_header),
                        body:   encoding.compress(&flight_data.data_body),
                    }
                } else {
                    DataFrame {
                        header: flight_data.data_header,
                        body:   flight_data.data_body,
                    }
                }
            })
            .collect()
    };

    let mut payload = Payload {
        data: dataframe(batch1),
        schema: schema_to_bytes(batch1[0].schema()),
        uuid,
        encoding: encoding.clone(),
        ..Default::default()
    };
    if !payload.data2.is_empty() {
        payload.data2 = dataframe(batch2);
        payload.schema2 = schema_to_bytes(batch2[0].schema());
    }
    payload
}

/// Convert record batch to payload for network transmission.
pub fn to_vec(batches: &[RecordBatch], uuid: Uuid, encoding: Encoding) -> Vec<u8> {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let data_frames = batches
        .par_iter()
        .map(|b| {
            let (_, flight_data) = flight_data_from_arrow_batch(b, &options);
            if encoding != Encoding::None {
                DataFrame {
                    header: encoding.compress(&flight_data.data_header),
                    body:   encoding.compress(&flight_data.data_body),
                }
            } else {
                DataFrame {
                    header: flight_data.data_header,
                    body:   flight_data.data_body,
                }
            }
        })
        .collect();

    serde_json::to_vec(&Payload {
        data: data_frames,
        schema: schema_to_bytes(batches[0].schema()),
        uuid,
        encoding,
        ..Default::default()
    })
    .unwrap()
}

/// Convert record batch to bytes for network transmission.
pub fn to_bytes(batch: &RecordBatch, uuid: Uuid, encoding: Encoding) -> bytes::Bytes {
    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let schema = schema_to_bytes(batch.schema());
    let (_, flight_data) = flight_data_from_arrow_batch(batch, &options);

    let data_frames = {
        if encoding != Encoding::None {
            DataFrame {
                header: encoding.compress(&flight_data.data_header),
                body:   encoding.compress(&flight_data.data_body),
            }
        } else {
            DataFrame {
                header: flight_data.data_header,
                body:   flight_data.data_body,
            }
        }
    };

    serde_json::to_vec(&Payload {
        data: vec![data_frames],
        schema,
        uuid,
        encoding,
        ..Default::default()
    })
    .unwrap()
    .into()
}
