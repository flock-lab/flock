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

use anyhow::Result;
use clap::{App, Arg, ArgMatches, SubCommand};

pub fn command(matches: &ArgMatches) -> Result<()> {
    if matches.is_present("run") {
        run()?;
    }
    Ok(())
}

pub fn command_args() -> App<'static, 'static> {
    SubCommand::with_name("nexmark")
        .about("The NEXMark Benchmark Tool")
        .arg(
            Arg::with_name("run")
                .short("r")
                .long("run")
                .help("Runs the nexmark lambda function"),
        )
}

pub fn run() -> Result<()> {
    unimplemented!();
}
