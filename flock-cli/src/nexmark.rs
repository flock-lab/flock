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

//! This crate runs the NexMark Benchmark on cloud function services.

use anyhow::{anyhow, Context as _, Ok, Result};
use benchmarks::{nexmark_benchmark, rainbow_println, NexmarkBenchmarkOpt};
use clap::{App, AppSettings, Arg, ArgMatches};
use log::warn;

pub fn command(matches: &ArgMatches) -> Result<()> {
    let (command, matches) = match matches.subcommand() {
        Some((command, matches)) => (command, matches),
        None => unreachable!(),
    };

    match command {
        "run" => run(matches),
        _ => {
            warn!("{} command is not implemented", command);
            Ok(())
        }
    }
    .with_context(|| anyhow!("{} command failed", command))?;

    Ok(())
}

pub fn command_args() -> App<'static> {
    App::new("nexmark")
        .about("The NEXMark Benchmark Tool")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(run_args())
}

fn run_args() -> App<'static> {
    App::new("run")
        .about("Runs the NEXMark Benchmark")
        .arg(
            Arg::new("query number")
                .short('q')
                .long("query")
                .help("Sets the NEXMark benchmark query number")
                .takes_value(true)
                .possible_values(&[
                    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13",
                ])
                .default_value("3"),
        )
        .arg(
            Arg::new("duration")
                .short('s')
                .long("seconds")
                .help("Runs the NEXMark benchmark for a number of seconds")
                .takes_value(true)
                .default_value("20"),
        )
        .arg(
            Arg::new("data generators")
                .short('g')
                .long("generators")
                .help("Runs the NEXMark benchmark with a number of data generators")
                .takes_value(true)
                .default_value("1"),
        )
        .arg(
            Arg::new("events per second")
                .short('e')
                .long("events-per-second")
                .help("Runs the NEXMark benchmark with a number of events per second")
                .takes_value(true)
                .default_value("1000"),
        )
        .arg(
            Arg::new("data sink type")
                .short('t')
                .long("data-sink-type")
                .help("Runs the NEXMark benchmark with a data sink type")
                .takes_value(true)
                .possible_values(&["sqs", "s3", "dynamodb", "efs", "blackhole"])
                .default_value("blackhole"),
        )
        .arg(
            Arg::new("async type")
                .short('a')
                .long("async-type")
                .help("Runs the NEXMark benchmark with async function invocations"),
        )
        .arg(
            Arg::new("memory size")
                .short('m')
                .long("memory-size")
                .help("Sets the memory size (MB) for the worker function")
                .takes_value(true)
                .default_value("128"),
        )
        .arg(
            Arg::new("architecture")
                .short('r')
                .long("arch")
                .help("Sets the architecture for the worker function")
                .takes_value(true)
                .possible_values(&["x86_64", "arm64"])
                .default_value("x86_64"),
        )
        .arg(
            Arg::new("distributed")
                .short('d')
                .long("distributed")
                .help("Runs the NEXMark benchmark with distributed workers"),
        )
        .arg(
            Arg::new("state backend")
                .short('b')
                .long("state-backend")
                .help("Sets the state backend for the worker function")
                .takes_value(true)
                .possible_values(&["hashmap", "s3", "efs"])
                .default_value("hashmap"),
        )
}

pub fn run(matches: &ArgMatches) -> Result<()> {
    let mut opt = NexmarkBenchmarkOpt::default();

    if matches.is_present("query number") {
        opt.query_number = matches
            .value_of("query number")
            .unwrap()
            .parse::<usize>()
            .with_context(|| anyhow!("invalid query number"))?;
    }

    if matches.is_present("duration") {
        opt.seconds = matches
            .value_of("duration")
            .unwrap()
            .parse::<usize>()
            .with_context(|| anyhow!("Invalid duration"))?;
    }

    if matches.is_present("data generators") {
        opt.generators = matches
            .value_of("data generators")
            .unwrap()
            .parse::<usize>()
            .with_context(|| anyhow!("Invalid data generators"))?;
    }

    if matches.is_present("events per second") {
        opt.events_per_second = matches
            .value_of("events per second")
            .unwrap()
            .parse::<usize>()
            .with_context(|| anyhow!("Invalid events per second"))?;
    }

    if matches.is_present("data sink type") {
        opt.data_sink_type = matches
            .value_of("data sink type")
            .unwrap()
            .parse::<String>()
            .with_context(|| anyhow!("Invalid data sink type"))?;
    }

    if matches.is_present("async type") {
        opt.async_type = matches
            .value_of("async type")
            .unwrap()
            .parse::<bool>()
            .with_context(|| anyhow!("Invalid async type"))?;
    }

    if matches.is_present("memory size") {
        opt.memory_size = matches
            .value_of("memory size")
            .unwrap()
            .parse::<i64>()
            .with_context(|| anyhow!("Invalid memory size"))?;
    }

    if matches.is_present("architecture") {
        opt.architecture = matches
            .value_of("architecture")
            .unwrap()
            .parse::<String>()
            .with_context(|| anyhow!("Invalid architecture"))?;
    }

    if matches.is_present("distributed") {
        opt.distributed = true;
    }

    if matches.is_present("state backend") {
        opt.state_backend = matches
            .value_of("state backend")
            .unwrap()
            .parse::<String>()
            .with_context(|| anyhow!("Invalid state backend"))?;
    }

    rainbow_println(include_str!("./flock"));

    futures::executor::block_on(nexmark_benchmark(&mut opt)).map_err(|e| e.into())
}
