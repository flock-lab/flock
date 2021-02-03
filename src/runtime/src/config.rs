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

//! Configuration settings that affect all crates in current system.

use lazy_static::lazy_static;
use serde_json::Value;

lazy_static! {
    /// Global constants across crates.
    pub static ref GLOBALS: Value = serde_json::from_str(include_str!("global.json")).unwrap();
}

/// Display the current configuration settings.
pub fn show() {
    println!(" * Settings :: \n\x1b[31m{:#?}\x1b[0m", GLOBALS.to_string());
}

/// A wrapper function to get the global information.
pub fn global<S>(s: S) -> Option<&'static str>
where
    S: Into<String>,
{
    GLOBALS[s.into()].as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    async fn setting_shows() -> Result<()> {
        show();
        Ok(())
    }
}
