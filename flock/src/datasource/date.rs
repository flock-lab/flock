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

//! A simple date time for data sources.

use serde::{Deserialize, Serialize};

/// The event's date time.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Hash, Copy, Default,
)]
pub struct DateTime(pub usize);

impl DateTime {
    /// Creates a new date time.
    pub fn new(date_time: usize) -> DateTime {
        DateTime(date_time)
    }
}

impl ::std::ops::Deref for DateTime {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ::std::ops::Add for DateTime {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        DateTime(self.0 + other.0)
    }
}

impl ::std::ops::Sub for DateTime {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        DateTime(self.0 - other.0)
    }
}
