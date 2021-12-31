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

use crate::rainbow::rainbow_println;
use anyhow::{anyhow, bail, Context as _, Result};
use benchmarks::{nexmark_benchmark, NexmarkBenchmarkOpt};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};

pub fn command(matches: &ArgMatches) -> Result<()> {
    let (command, matches) = match matches.subcommand() {
        (command, Some(matches)) => (command, matches),
        (_, None) => unreachable!(),
    };

    match command {
        "run" => run(matches),
        _ => bail!(matches.usage().to_owned()),
    }
    .with_context(|| anyhow!("{} command failed", command))?;

    Ok(())
}

pub fn command_args() -> App<'static, 'static> {
    SubCommand::with_name("nexmark")
        .about("The NEXMark Benchmark Tool")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(run_args())
}

fn run_args() -> App<'static, 'static> {
    SubCommand::with_name("run")
        .about("Runs the NEXMark Benchmark")
        .arg(
            Arg::with_name("query number")
                .short("q")
                .long("query")
                .help("Sets the NEXMark benchmark query number [0-12]")
                .takes_value(true)
                .default_value("3"),
        )
        .arg(
            Arg::with_name("debug")
                .short("d")
                .long("debug")
                .help("Runs the NEXMark benchmark in debug mode"),
        )
        .arg(
            Arg::with_name("duration")
                .short("s")
                .long("seconds")
                .help("Runs the NEXMark benchmark for a number of seconds")
                .takes_value(true)
                .default_value("20"),
        )
        .arg(
            Arg::with_name("data generators")
                .short("g")
                .long("generators")
                .help("Runs the NEXMark benchmark with a number of data generators")
                .takes_value(true)
                .default_value("1"),
        )
        .arg(
            Arg::with_name("events per second")
                .short("e")
                .long("events-per-second")
                .help("Runs the NEXMark benchmark with a number of events per second")
                .takes_value(true)
                .default_value("1000"),
        )
        .arg(
            Arg::with_name("data sink type")
                .short("t")
                .long("data-sink-type")
                .help("Runs the NEXMark benchmark with a data sink type")
                .takes_value(true)
                .possible_values(&["sqs", "s3", "dynamodb", "efs", "blackhole"])
                .default_value("blackhole"),
        )
        .arg(
            Arg::with_name("async type")
                .short("a")
                .long("async-type")
                .help("Runs the NEXMark benchmark with async function invocations"),
        )
        .arg(
            Arg::with_name("memory size")
                .short("m")
                .long("memory-size")
                .help("Sets the memory size (MB) for the worker function")
                .takes_value(true)
                .default_value("128"),
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

    if matches.is_present("debug") {
        opt.debug = true;
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

    rainbow_println(include_str!("./flock"));

    futures::executor::block_on(nexmark_benchmark(&mut opt)).map_err(|e| e.into())
}
