// Copyright (c) 2020 UMD Database Group. All Rights Reserved.
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

//! A "prelude" for users of the runtime crate.
//!
//! Like the standard library's prelude, this module simplifies importing of
//! common items. Unlike the standard prelude, the contents of this module must
//! be imported manually:
//!
//! ```
//! use runtime::prelude::*;
//! ```

pub use crate::arena::{Arena, WindowSession};
pub use crate::config;
pub use crate::config::GLOBALS as globals;
pub use crate::context::{CloudFunction, ExecutionContext};
pub use crate::datasource::{kafka, kinesis, nexmark, DataSource};
pub use crate::encoding::Encoding;
pub use crate::error::{FlockError, Result};
pub use crate::executor::{plan::physical_plan, ExecutionStrategy, Executor, LambdaExecutor};
pub use crate::payload::{Payload, Uuid, UuidBuilder};
pub use crate::query::{BatchQuery, Query, Schedule, StreamQuery, StreamWindow};
