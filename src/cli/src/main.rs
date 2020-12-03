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

//! The Command Line Interactive Contoller for ServerlessCQ
mod cmd;
mod env;
mod output;

#[macro_use]
extern crate lazy_static;

use clap::{crate_version, App, Arg};
#[allow(unused_imports)]
use cmd::Cmd;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

async fn print_ast() -> Result<(), Error> {
    let dialect = GenericDialect {}; // or AnsiDialect

    let sql = "SELECT a, b, 123, myfunc(b) \
                   FROM table_1 \
                   WHERE a > b AND b < 100 \
                   ORDER BY a DESC, b";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!(
        "SQL:\n'{}'",
        ast.iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );

    println!(
        "Serialized AST as JSON:\n{}",
        serde_json::to_string_pretty(&ast).unwrap()
    );
    // println!("AST: {:#?}", ast);
    Ok(())
}

lazy_static! {
    static ref SCQSQL: &'static str =
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
async fn main() -> Result<(), Error> {
    env_logger::init();
    // Command line arg parsing for scqsql itself
    let matches = App::new("SCQSQL")
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

    let _config = matches.value_of("config").unwrap_or("default.conf");

    println!("{}", *SCQSQL);
    print_ast().await?;
    Ok(())
}
