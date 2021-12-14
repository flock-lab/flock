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

use crate::fsql::fsql;
use crate::rainbow::rainbow_println;
use crate::s3;
use anyhow::{bail, Result};
use clap::{crate_version, App, Arg};
use std::env;

pub static S3_BUCKET: &str = "umd-flock";

#[tokio::main]
pub async fn main() -> Result<()> {
    // Command line arg parsing and configuration.
    let matches = App::new("Flock")
        .version(crate_version!())
        .about("Command Line Interactive Contoller for Flock")
        .arg(
            Arg::with_name("function_code")
                .short("u")
                .long("upload")
                .value_name("FILE")
                .help("Upload lambda execution code to S3.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("function_key")
                .short("k")
                .long("key")
                .value_name("STRING")
                .help("AWS S3 key for this function code.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    rainbow_println(include_str!("./flock"));

    match matches.value_of("function_code") {
        Some(bin_path) => {
            rainbow_println("============================================================");
            rainbow_println("                Upload function code to S3                  ");
            rainbow_println("============================================================");
            println!();
            println!();
            if !std::path::Path::new(bin_path).exists() {
                bail!("The function code ({}) doesn't exist.", bin_path);
            }
            let key = matches
                .value_of("function_key")
                .expect("function_key is required.");
            s3::put_function_object(S3_BUCKET, key, bin_path).await?;
        }
        None => {
            fsql().await;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rainbow_print() {
        let text = include_str!("./flock");
        rainbow_println(text);
    }
}
