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

//! A "prelude" for users of the flock crate.
//!
//! Like the standard library's prelude, this module simplifies importing of
//! common items. Unlike the standard prelude, the contents of this module must
//! be imported manually:
//!
//! ```
//! use flock::prelude::*;
//! ```

pub use crate::configs::*;
pub use crate::datasink::{DataSink, DataSinkFormat, DataSinkType};
pub use crate::datasource::{nexmark, tpch, ysb, DataSource, DataStream, RelationPartitions};
pub use crate::encoding::Encoding;
pub use crate::error::{FlockError, Result};
pub use crate::query::{Query, Table};
pub use crate::runtime::arena::{Arena, WindowSession};
pub use crate::runtime::context::{self, CloudFunction, ExecutionContext};
pub use crate::runtime::payload::{DataFrame, Payload, Uuid, UuidBuilder};
pub use crate::runtime::plan::{physical_plan, CloudExecutionPlan};
pub use crate::stream::{Schedule, Window};
pub use crate::transmute::*;
