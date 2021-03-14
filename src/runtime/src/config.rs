// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
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

use ini::Ini;
use lazy_static::lazy_static;

lazy_static! {
    /// Global settings.
    pub static ref GLOBALS: Ini = Ini::load_from_str(include_str!("config/squirtle.toml")).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    async fn setting_shows() -> Result<()> {
        let conf = Ini::load_from_str(include_str!("config/squirtle.toml")).unwrap();

        for (sec, prop) in &conf {
            println!("Section: {:?}", sec);
            for (key, value) in prop.iter() {
                println!("{:?}:{:?}", key, value);
            }
        }

        assert_eq!(
            5242880,
            (&conf["lambda"]["join_threshold"]).parse::<i32>().unwrap()
        );
        assert_eq!(
            10485760,
            (&conf["lambda"]["aggregate_threshold"])
                .parse::<i32>()
                .unwrap()
        );
        assert_eq!(
            20971520,
            (&conf["lambda"]["regular_threshold"])
                .parse::<i32>()
                .unwrap()
        );

        Ok(())
    }
}
