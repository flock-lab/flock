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

//! Use EFS state backend to manage the state of the execution engine.

use super::StateBackend;
use crate::error::Result;
use crate::runtime::payload::Payload;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;

/// EfsStateBackend is a state backend that stores query states in Amazon
/// Elastic File System (EFS).
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct EfsStateBackend {}

#[async_trait]
#[typetag::serde(name = "efs_state_backend")]
impl StateBackend for EfsStateBackend {
    fn name(&self) -> String {
        "EfsStateBackend".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    async fn write(&self, _: &'static str, _: &'static str, _: Vec<u8>) -> Result<()> {
        unimplemented!();
    }

    async fn read(&self, _: &'static str, _: &'static [&'static str]) -> Result<Vec<Payload>> {
        unreachable!()
    }
}

impl EfsStateBackend {
    /// Creates a new EFSStateBackend.
    pub fn new() -> Self {
        Self {}
    }
}
