// Copyright (c) 2020 UMD Database Group. All rights reserved.
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

#[allow(unused_imports)]
#[macro_use]
extern crate more_asserts;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use datafusion::datasource::MemTable;
    use datafusion::execution::context::ExecutionContext;
    use datafusion::physical_plan::collect;

    #[tokio::test]
    async fn simple_avg() -> Result<(), Error> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![4, 5]))],
        )?;

        let mut ctx = ExecutionContext::new();

        let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("t", Box::new(provider));

        let sql = "SELECT AVG(a) FROM t";
        let logical_plan = ctx.create_logical_plan(&sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;

        let result = collect(physical_plan).await?;

        let batch = &result[0];
        assert_eq!(1, batch.num_columns());
        assert_eq!(1, batch.num_rows());

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to cast version");
        assert_eq!(values.len(), 1);
        // avg(1,2,3,4,5) = 3.0
        assert_lt!(values.value(0) - 3.0_f64, f64::EPSILON);
        Ok(())
    }
}
