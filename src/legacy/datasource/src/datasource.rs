// Copyright 2020 UMD Database Group. All Rights Reserved.
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

//! Data source traits

use arrow::datatypes::SchemaRef;
use std::any::Any;

/// This table statistics are estimates.
/// It can not be used directly in the precise compute
#[derive(Clone)]
pub struct Statistics {
    /// total byte of the table rows
    pub total_byte_size: usize,
}

/// Source table
pub trait TableProvider {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Returns the table Statistics
    /// Statistics should be optional because not all data sources can provide
    /// statistics.
    fn statistics(&self) -> Option<Statistics>;
}
