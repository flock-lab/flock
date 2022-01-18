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

//! S3 state backend to manage the state of the execution engine.

use super::StateBackend;

/// S3StateBackend is a state backend that stores query states in Amazon S3.
pub struct S3StateBackend {}

impl StateBackend for S3StateBackend {
    /// The type of the state backend.
    fn state_backend_name() -> &'static str {
        "S3StateBackend"
    }
}
