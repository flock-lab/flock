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

//! `Encoding` is a compression/decompression module to reduce the total size of
//! all environment variables so that they doesn't exceed 4 KB.

use serde::{Deserialize, Serialize};

/// A compressor/decompressor type.
#[derive(Debug, Deserialize, Serialize)]
pub enum Encoding {
    /// Snappy is a LZ77-type compressor with a fixed, byte-oriented encoding.
    /// It does not aim for maximum compression, or compatibility with any other
    /// compression library; instead, it aims for very high speeds and
    /// reasonable compression.
    /// https://github.com/burntsushi/rust-snappy
    Snappy,
    /// LZ4 is a very fast lossless compression algorithm, providing compression
    /// speed at 400 MB/s per core, with near-linear scalability for
    /// multi-threaded applications. It also features an extremely fast decoder,
    /// with speed in multiple GB/s per core, typically reaching RAM speed
    /// limits on multi-core systems.
    /// https://github.com/bozaro/lz4-rs
    Lz4,
    /// A streaming compression/decompression library DEFLATE-based streams.
    /// https://github.com/rust-lang/flate2-rs
    Zlib,
    /// No compression/decompression applied to the context.
    None,
}

impl Encoding {
    /// Compress data
    pub fn encoder(&self, s: &str) -> String {
        match *self {
            Encoding::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                let en = encoder.compress_vec(s.as_bytes()).unwrap();
                unsafe { std::str::from_utf8_unchecked(&en).to_string() }
            }
            _ => {
                unimplemented!();
            }
        }
    }

    /// Decompress data
    pub fn decoder(&self, s: &str) -> String {
        match *self {
            Encoding::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                let de = decoder.decompress_vec(s.as_bytes()).unwrap();
                unsafe { std::str::from_utf8_unchecked(&de).to_string() }
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn encode_plan() -> Result<()> {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("d", DataType::Int32, false),
        ]));

        // define data.
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;
        // define data.
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;

        let mut ctx = ExecutionContext::new();

        let table1 = MemTable::try_new(schema1, vec![vec![batch1]])?;
        let table2 = MemTable::try_new(schema2, vec![vec![batch2]])?;

        ctx.register_table("t1", Box::new(table1));
        ctx.register_table("t2", Box::new(table2));

        let sql = concat!(
            "SELECT a, b, d ",
            "FROM t1 JOIN t2 ON a = c ",
            "ORDER BY b ASC ",
            "LIMIT 3"
        );

        let plan = ctx.create_logical_plan(&sql)?;
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;

        let en = Encoding::Snappy;

        let json = serde_json::to_string(&plan).unwrap();
        assert_eq!(1773, json.len());

        let en_json = en.encoder(&json);
        assert_eq!(528, en_json.len());

        let de_json = en.decoder(&en_json);
        assert_eq!(1773, de_json.len());
        assert_eq!(json, de_json);

        Ok(())
    }
}
