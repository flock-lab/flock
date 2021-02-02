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

#![warn(missing_docs)]
// Clippy lints, some should be disabled incrementally
#![allow(
    clippy::float_cmp,
    clippy::module_inception,
    clippy::new_without_default,
    clippy::ptr_arg,
    clippy::type_complexity,
    clippy::wrong_self_convention
)]
#![feature(get_mut_unchecked)]

//! The runtime contains the context information needed by the lambda function,
//! such as execution plan and the next lambda functions, which instructs the
//! lambda instance to perform the correct operation.

pub mod context;
pub mod datasource;
pub mod encoding;
pub mod error;
pub mod payload;
pub mod plan;

pub use datasource::DataSource;
pub use payload::Payload;
