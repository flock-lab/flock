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
use crate::nexmark;
use crate::rainbow::rainbow_println;
use crate::s3;
use anyhow::Result;
use clap::{crate_version, App, AppSettings, Arg, SubCommand};
use ini::Ini;
use lazy_static::lazy_static;
use std::env;

lazy_static! {
    /// Global settings.
    pub static ref FLOCK_CONF: Ini = Ini::load_from_str(include_str!("../../flock/src/config.toml")).unwrap();
    pub static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["flock"]["s3_bucket"].to_string();
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // Command line arg parsing and configuration.
    let matches = App::new("Flock")
        .version(crate_version!())
        .about("Command Line Interactive Contoller for Flock")
        .version(crate_version!())
        .author("UMD Database Group")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("nexmark")
                .about("The NEXMark Benchmark Tool")
                .setting(AppSettings::DisableVersion)
                .arg(
                    Arg::with_name("run")
                        .short("r")
                        .long("run")
                        .help("Runs the nexmark lambda function"),
                ),
        )
        .subcommand(
            SubCommand::with_name("upload")
                .about("Uploads a function code to AWS S3")
                .setting(AppSettings::DisableVersion)
                .arg(
                    Arg::with_name("code path")
                        .short("p")
                        .long("path")
                        .value_name("FILE")
                        .help("Sets the path to the function code")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("s3 key")
                        .short("k")
                        .long("key")
                        .value_name("S3_KEY")
                        .help("Sets the S3 key to upload the function code to")
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("fsql")
                .about("The terminal-based front-end to Flock")
                .setting(AppSettings::DisableVersion),
        )
        .get_matches();

    rainbow_println(include_str!("./flock"));

    match matches.subcommand() {
        ("nexmark", Some(nexmark_matches)) => {
            if nexmark_matches.is_present("run") {
                nexmark::run()?;
            }
        }
        ("upload", Some(upload_matches)) => {
            s3::put_function_object(
                &FLOCK_S3_BUCKET,
                upload_matches
                    .value_of("s3 key")
                    .expect("No function s3 key provided"),
                upload_matches
                    .value_of("code path")
                    .expect("No function code path provided"),
            )
            .await?;
        }
        ("fsql", Some(_)) => {
            fsql().await?;
        }
        _ => {
            println!("{}", matches.usage());
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
