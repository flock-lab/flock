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

//! Configuration settings that affect all crates in current system.

use ini::Ini;
use lazy_static::lazy_static;

lazy_static! {
    /// Global settings.
    pub static ref FLOCK_CONF: Ini = Ini::load_from_str(include_str!("./config.toml")).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    async fn setting_shows() -> Result<()> {
        let conf = Ini::load_from_str(include_str!("./config.toml")).unwrap();

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
