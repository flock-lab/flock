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

#![warn(missing_docs)]
// Clippy lints, some should be disabled incrementally
#![allow(
    clippy::float_cmp,
    clippy::module_inception,
    clippy::new_without_default,
    clippy::ptr_arg,
    clippy::type_complexity,
    clippy::wrong_self_convention,
    clippy::should_implement_trait
)]
#![feature(get_mut_unchecked)]

//! The runtime contains the context information needed by the lambda function,
//! such as execution plan and the next lambda functions, which instructs the
//! lambda instance to perform the correct operation.

pub mod arena;
pub mod config;
pub mod context;
pub mod datasource;
pub mod encoding;
pub mod error;
pub mod executor;
pub mod payload;
pub mod prelude;
pub mod query;
pub mod transform;
