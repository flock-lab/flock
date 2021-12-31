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

use anyhow::Result;
use clap::{Arg, ArgMatches};
use std::io::Write;

pub fn get_args() -> Vec<Arg<'static, 'static>> {
    let config = Arg::with_name("config")
        .short("c")
        .long("config")
        .value_name("FILE")
        .help("Sets a custom config file")
        .takes_value(true);
    get_logging_args().into_iter().chain(vec![config]).collect()
}

fn get_logging_args() -> Vec<Arg<'static, 'static>> {
    [
        Arg::with_name("log-level")
            .short("L")
            .long("log-level")
            .possible_values(&["error", "warn", "info", "debug", "trace", "off"])
            .help("Log level [default: info]")
            .global(true)
            .takes_value(true),
        Arg::with_name("trace")
            .long("trace")
            .help("Log ultra-verbose (trace level) information")
            .global(true)
            .takes_value(false),
        Arg::with_name("silent")
            .long("silent")
            .help("Suppress all output")
            .global(true)
            .takes_value(false),
    ]
    .to_vec()
}

pub fn get_logging(
    global_matches: &ArgMatches,
    matches: &ArgMatches,
) -> Result<env_logger::Builder> {
    let mut builder = env_logger::Builder::new();

    let level = if matches.is_present("trace") {
        log::LevelFilter::Trace
    } else if matches.is_present("silent") {
        log::LevelFilter::Off
    } else {
        match matches
            .value_of("log-level")
            .or_else(|| global_matches.value_of("log-level"))
        {
            Some("error") => log::LevelFilter::Error,
            Some("warn") => log::LevelFilter::Warn,
            Some("debug") => log::LevelFilter::Debug,
            Some("trace") => log::LevelFilter::Trace,
            Some("off") => log::LevelFilter::Off,
            _ => log::LevelFilter::Info,
        }
    };
    builder.filter(None, level);
    builder.filter_module("ws", log::LevelFilter::Warn);

    if level == log::LevelFilter::Trace {
        builder.format_timestamp_secs();
    } else {
        builder.format(|f, record| {
            writeln!(
                f,
                "[{}] {}",
                record.level().to_string().to_lowercase(),
                record.args()
            )
        });
    }

    Ok(builder)
}
