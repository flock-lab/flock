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

//! Flock CLI measures the performance of relational operators on different
//! architectures such as x86, and ARM.

use anyhow::{anyhow, Context as _, Ok, Result};
use benchmarks::{arch_benchmark, rainbow_println, ArchBenchmarkOpt};
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
    App::new("arch")
        .about("Benchmark the performance of relational operators on different architectures")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(run_args())
}

fn run_args() -> App<'static> {
    App::new("run")
        .about("Runs the arch Benchmark")
        .arg(
            Arg::new("num events")
                .short('e')
                .long("events")
                .help("Runs the arch benchmark with a number of events")
                .takes_value(true)
                .default_value("1000"),
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
}

pub fn run(matches: &ArgMatches) -> Result<()> {
    let mut opt = ArchBenchmarkOpt::default();

    if matches.is_present("num events") {
        opt.events = matches
            .value_of("num events")
            .unwrap()
            .parse::<usize>()
            .with_context(|| anyhow!("Invalid num events"))?;
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

    rainbow_println(include_str!("./flock"));

    futures::executor::block_on(arch_benchmark(&mut opt)).map_err(|e| e.into())
}
