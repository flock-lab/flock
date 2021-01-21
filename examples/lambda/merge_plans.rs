// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;

use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::{collect, ExecutionPlan, LambdaExecPlan};

use datafusion::prelude::*;
use std::collections::VecDeque;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    Ok(())
}

async fn quick_test(sql: &String) -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Float64, false),
        Field::new("c3", DataType::Utf8, false),
        Field::new("c4", DataType::UInt64, false),
        Field::new("c5", DataType::Utf8, false),
        Field::new("neg", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![90, 90, 91, 101, 92, 102, 93, 103])),
            Arc::new(Float64Array::from(vec![
                92.1, 93.2, 95.3, 96.4, 98.5, 99.6, 100.7, 101.8,
            ])),
            Arc::new(StringArray::from(vec![
                "a", "a", "d", "b", "b", "d", "c", "c",
            ])),
            Arc::new(UInt64Array::from(vec![33, 1, 54, 33, 12, 75, 2, 87])),
            Arc::new(StringArray::from(vec![
                "rapport",
                "pedantic",
                "mimesis",
                "haptic",
                "baksheesh",
                "amok",
                "devious",
                "c",
            ])),
            Arc::new(Int64Array::from(vec![
                -90, -90, -91, -101, -92, -102, -93, -103,
            ])),
        ],
    )?;

    let mut ctx = ExecutionContext::new();
    // batch? Only support 1 RecordBatch now.
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])?;
    ctx.register_table("test_table", Box::new(provider));

    let logical_plan = ctx.create_logical_plan(sql)?;
    let optimized_plan = ctx.optimize(&logical_plan)?;
    let phy_plan = ctx.create_physical_plan(&optimized_plan)?;

    let mut plan = phy_plan.clone();

    let mut merged_plan_deq_1: VecDeque<Arc<dyn ExecutionPlan>> = VecDeque::new();
    let mut merged_plan_deq_2: VecDeque<Arc<dyn ExecutionPlan>> = VecDeque::new();

    let mut merged_plan1: Option<Arc<dyn ExecutionPlan>> = None;
    let mut merged_plan2: Option<Arc<dyn ExecutionPlan>> = None;

    let mut hash_agg_appeared: bool = false;

    loop {
        if let Some(_) = plan.as_any().downcast_ref::<FilterExec>() {
            merged_plan_deq_1.push_back(plan.clone());
        } else if let Some(_) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
            merged_plan_deq_1.push_back(plan.clone());
        } else if let Some(_) = plan.as_any().downcast_ref::<HashAggregateExec>() {
            if hash_agg_appeared {
                merged_plan_deq_1.push_back(plan.clone());
            } else {
                merged_plan_deq_2.push_back(plan.clone());
            }
            hash_agg_appeared = true;
        } else if let Some(_) = plan.as_any().downcast_ref::<ProjectionExec>() {
            merged_plan_deq_2.push_back(plan.clone());
        } else if let Some(_) = plan.as_any().downcast_ref::<SortExec>() {
            merged_plan_deq_2.push_back(plan.clone());
        } else if let Some(_) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        } else if let Some(_) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
            merged_plan_deq_2.push_back(plan.clone());
        }

        if plan.children().len() == 0 {
            break;
        }
        plan = plan.children()[0].clone();
    }

    let mut result: Vec<RecordBatch> = Vec::new();

    if merged_plan_deq_1.len() != 0 {
        merged_plan1 = get_merged_plans(&mut merged_plan_deq_1, batch).await;
        let mut prev_output: Option<RecordBatch> = None;
        match merged_plan1 {
            Some(merged_plan) => {
                result = collect(merged_plan).await?;
                prev_output = Some(result[0].clone());
            }
            None => panic!("Wrong plan."),
        }
        if merged_plan_deq_2.len() != 0 {
            match prev_output {
                Some(record_batch) => {
                    merged_plan2 = get_merged_plans(&mut merged_plan_deq_2, record_batch).await;
                    match merged_plan2 {
                        Some(merged_plan) => {
                            result = collect(merged_plan).await?;
                        }
                        None => panic!("Wrong plan."),
                    }
                }
                None => panic!("Need record batch"),
            }
        }
    } else {
        merged_plan2 = get_merged_plans(&mut merged_plan_deq_2, batch).await;
        match merged_plan2 {
            Some(merged_plan) => {
                result = collect(merged_plan).await?;
            }
            None => panic!("Wrong plan."),
        }
    }
    println!("Lambda results:");
    pretty::print_batches(&result)?;

    let expected_result = collect(phy_plan).await?;
    println!("Expected results:");
    pretty::print_batches(&expected_result)?;
    Ok(result)
}

async fn get_merged_plans(
    plans: &mut VecDeque<Arc<dyn ExecutionPlan>>,
    batch: RecordBatch,
) -> Option<Arc<dyn ExecutionPlan>> {
    let prev_plan = plans.pop_back().unwrap();

    let mut merged_plan: Option<Arc<dyn ExecutionPlan>> = None;

    if let Some(plan) = prev_plan.as_any().downcast_ref::<FilterExec>() {
        let mut first_plan = plan.new_plan();
        first_plan.feed_batches(vec![vec![batch]]);
        merged_plan = Some(Arc::new(first_plan));
    } else if let Some(plan) = prev_plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        let mut first_plan = plan.new_plan();
        first_plan.feed_batches(vec![vec![batch]]);
        merged_plan = Some(Arc::new(first_plan));
    } else if let Some(plan) = prev_plan.as_any().downcast_ref::<HashAggregateExec>() {
        let mut first_plan = plan.new_plan();
        first_plan.feed_batches(vec![vec![batch]]);
        merged_plan = Some(Arc::new(first_plan));
    } else if let Some(plan) = prev_plan.as_any().downcast_ref::<ProjectionExec>() {
        let mut first_plan = plan.new_plan();
        first_plan.feed_batches(vec![vec![batch]]);
        merged_plan = Some(Arc::new(first_plan));
    } else if let Some(plan) = prev_plan.as_any().downcast_ref::<SortExec>() {
        let mut first_plan = plan.new_plan();
        first_plan.feed_batches(vec![vec![batch]]);
        merged_plan = Some(Arc::new(first_plan));
    } else if let Some(plan) = prev_plan.as_any().downcast_ref::<GlobalLimitExec>() {
        let mut first_plan = plan.new_plan();
        first_plan.feed_batches(vec![vec![batch]]);
        merged_plan = Some(Arc::new(first_plan));
    }

    while plans.len() != 0 {
        let inner_plan = merged_plan.clone().unwrap();
        let outer_plan = plans.pop_back().unwrap();
        merged_plan = merge_plans(inner_plan, outer_plan).await;
    }
    merged_plan
}

async fn merge_plans(
    inner_plan: Arc<dyn ExecutionPlan>,
    outer_plan: Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let mut current_orphan: Option<Arc<dyn ExecutionPlan>> = None;

    if let Some(plan) = outer_plan.as_any().downcast_ref::<FilterExec>() {
        let mut current_plan = plan.new_plan();

        current_plan.feed_input(inner_plan);
        current_orphan = Some(Arc::new(current_plan));
    } else if let Some(plan) = outer_plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        let mut current_plan = plan.new_plan();

        current_plan.feed_input(inner_plan);
        current_orphan = Some(Arc::new(current_plan));
    } else if let Some(plan) = outer_plan.as_any().downcast_ref::<HashAggregateExec>() {
        let mut current_plan = plan.new_plan();

        current_plan.feed_input(inner_plan);
        current_orphan = Some(Arc::new(current_plan));
    } else if let Some(plan) = outer_plan.as_any().downcast_ref::<ProjectionExec>() {
        let mut current_plan = plan.new_plan();

        current_plan.feed_input(inner_plan);
        current_orphan = Some(Arc::new(current_plan));
    } else if let Some(plan) = outer_plan.as_any().downcast_ref::<SortExec>() {
        let mut current_plan = plan.new_plan();

        current_plan.feed_input(inner_plan);
        current_orphan = Some(Arc::new(current_plan));
    } else if let Some(plan) = outer_plan.as_any().downcast_ref::<GlobalLimitExec>() {
        let mut current_plan = plan.new_plan();

        current_plan.feed_input(inner_plan);
        current_orphan = Some(Arc::new(current_plan));
    }

    current_orphan
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mem -> Proj
    #[tokio::test]
    async fn simple_select() {
        let sql = concat!("SELECT c1 FROM test_table").to_string();
        quick_test(&sql).await;
    }

    #[tokio::test]
    async fn select_alias() {
        let sql = concat!("SELECT c1 as col_1 FROM test_table").to_string();
        quick_test(&sql).await;
    }

    #[tokio::test]
    async fn cast() {
        let sql = concat!("SELECT CAST(c2 AS int) FROM test_table").to_string();
        quick_test(&sql).await;
    }
    #[tokio::test]
    async fn math() {
        let sql = concat!("SELECT c1+c2 FROM test_table").to_string();
        quick_test(&sql).await;
    }

    #[tokio::test]
    async fn math_sqrt() {
        let sql = concat!("SELECT c1>=c2 FROM test_table").to_string();
        quick_test(&sql).await;
    }

    // Filter
    // Memory -> Filter -> CoalesceBatches -> Projection
    #[tokio::test]
    async fn filter_query() {
        let sql = concat!("SELECT c1, c2 ", "FROM test_table ", "WHERE c2 < 99").to_string();
        quick_test(&sql).await;
    }

    // Mem -> Filter -> Coalesce
    #[tokio::test]
    async fn filter_select_all() {
        let sql = concat!("SELECT * ", "FROM test_table ", "WHERE c2 < 99").to_string();
        quick_test(&sql).await;
    }

    // Aggregate
    // Mem -> HashAgg -> HashAgg
    #[tokio::test]
    async fn aggregate_query_no_group_by_count_distinct_wide() {
        let sql = concat!("SELECT COUNT(DISTINCT c1) FROM test_table").to_string();
        quick_test(&sql).await;
    }

    #[tokio::test]
    async fn aggregate_query_no_group_by() {
        let sql = concat!("SELECT MIN(c1), AVG(c4), COUNT(c3) FROM test_table").to_string();
        quick_test(&sql).await;
    }

    // Aggregate + Group By
    // Mem -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn aggregate_query_group_by() {
        let sql = concat!(
            "SELECT MIN(c1), AVG(c4), COUNT(c3) as c3_count ",
            "FROM test_table ",
            "GROUP BY c3"
        )
        .to_string();
        quick_test(&sql).await;
    }

    // Sort
    // Mem -> Project -> Sort
    #[tokio::test]
    async fn sort() {
        let sql = concat!("SELECT c1, c2, c3 ", "FROM test_table ", "ORDER BY c1 ",).to_string();
        quick_test(&sql).await;
    }

    // Sort limit
    // Mem -> Project -> Sort -> GlobalLimit
    #[tokio::test]
    async fn sort_and_limit_by_int() {
        let sql = concat!(
            "SELECT c1, c2, c3 ",
            "FROM test_table ",
            "ORDER BY c1 ",
            "LIMIT 4"
        )
        .to_string();
        quick_test(&sql).await;
    }

    // Agg + Filter
    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn aggregate_query_filter() {
        let sql = concat!(
            "SELECT MIN(c1), AVG(c4), COUNT(c3) as c3_count ",
            "FROM test_table ",
            "WHERE c2 < 99"
        )
        .to_string();
        quick_test(&sql).await;
    }

    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj
    #[tokio::test]
    async fn agg_query2() {
        let sql = concat!(
            "SELECT MAX(c1), MIN(c2), c3 ",
            "FROM test_table ",
            "WHERE c2 < 101 AND c1 > 91 ",
            "GROUP BY c3"
        )
        .to_string();
        quick_test(&sql).await;
    }

    // Mem -> Filter -> Coalesce -> HashAgg -> HashAgg -> Proj -> Sort -> GlobalLimit
    #[tokio::test]
    async fn filter_agg_sort() {
        let sql = concat!(
            "SELECT MAX(c1), MIN(c2), c3 ",
            "FROM test_table ",
            "WHERE c2 < 101 ",
            "GROUP BY c3 ",
            "ORDER BY c3 ",
            "LIMIT 3"
        )
        .to_string();
        quick_test(&sql).await;
    }
}
