// Copyright (c) 2020 UMD Database Group. All rights reserved.
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

#[path = "./error.rs"]
mod error;
use error::Result;

use clap::{crate_version, App, Arg};
use lazy_static::lazy_static;
use rustyline::Editor;
use std::env;

lazy_static! {
    static ref SERVERLESS_CQ: &'static str =
    "
███████╗███████╗██████╗ ██╗   ██╗███████╗██████╗ ██╗     ███████╗███████╗███████╗ ██████╗ ██████╗
██╔════╝██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗██║     ██╔════╝██╔════╝██╔════╝██╔════╝██╔═══██╗
███████╗█████╗  ██████╔╝██║   ██║█████╗  ██████╔╝██║     █████╗  ███████╗███████╗██║     ██║   ██║
╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██╔══╝  ██╔══██╗██║     ██╔══╝  ╚════██║╚════██║██║     ██║▄▄ ██║
███████║███████╗██║  ██║ ╚████╔╝ ███████╗██║  ██║███████╗███████╗███████║███████║╚██████╗╚██████╔╝
╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝╚══════╝ ╚═════╝ ╚══▀▀═╝

ServerlessCQ: Continuous Queries on Cloud Function Services (https://github.com/DSLAM-UMD/ServerlessCQ)

Copyright (c) 2020 UMD Database Group. All rights reserved.

This is the standard command line interactive contoller for ServerlessCQ.
This has all the right tools installed required to execute a query against cloud function services.
    ";
}

#[tokio::main]
pub async fn main() {
    // Command line arg parsing for scqsql itself
    let matches = App::new("ServerlessCQ")
        .version(crate_version!())
        .about("Command Line Interactive Contoller for ServerlessCQ")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Specify a custom config file to find AWS credentials configs")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("exec")
                .short("e")
                .long("exec")
                .value_name("COMMAND")
                .help("Execute the specified command then exit")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("filename")
                .short("f")
                .long("file")
                .value_name("FILE")
                .help("Read commands from the file rather than standard input")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    println!("{}", *SERVERLESS_CQ);
    let _config = matches.value_of("config").unwrap_or("default.conf");

    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    let mut query = "".to_owned();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(ref line) if is_exit_command(line) && query.is_empty() => {
                break;
            }
            Ok(ref line) if line.trim_end().ends_with(';') => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone());
                match exec_and_print(query).await {
                    Ok(_) => {}
                    Err(err) => println!("{:?}", err),
                }
                query = "".to_owned();
            }
            Ok(ref line) => {
                query.push_str(line);
                query.push(' ');
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}

#[allow(unused_variables)]
async fn exec_and_print(sql: String) -> Result<()> {
    Ok(())
}
