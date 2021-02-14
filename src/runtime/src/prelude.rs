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

//! A "prelude" for users of the runtime crate.
//!
//! Like the standard library's prelude, this module simplifies importing of
//! common items. Unlike the standard prelude, the contents of this module must
//! be imported manually:
//!
//! ```
//! use runtime::prelude::*;
//! ```

pub use crate::config;
pub use crate::context::{CloudFunction, ExecutionContext};
pub use crate::datasource::{kafka, kinesis, DataSource};
pub use crate::encoding::Encoding;
pub use crate::error::{Result, SquirtleError};
pub use crate::payload::{Payload, Uuid, UuidBuilder};
pub use crate::plan::*;
pub use crate::query::{BatchQuery, Query, Schedule, StreamQuery, StreamWindow};
pub use crate::{exec_plan, init_plan};
