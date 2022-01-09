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

#![warn(missing_docs, clippy::needless_borrow)]
// Clippy lints, some should be disabled incrementally
#![allow(
    clippy::float_cmp,
    clippy::from_over_into,
    clippy::module_inception,
    clippy::new_without_default,
    clippy::type_complexity,
    clippy::upper_case_acronyms,
    clippy::comparison_to_empty
)]
#![feature(get_mut_unchecked)]

//! `flock-function` is a library for implementing Flock functions on cloud
//! function services.

pub mod aws;
pub mod azure;
pub mod gcp;
mod launcher;
