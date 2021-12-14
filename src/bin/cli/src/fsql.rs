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

//! fsql is a terminal-based front-end to Flock.

use crate::rainbow::rainbow_println;
use anyhow::{anyhow, Result};
use rustyline::Editor;

/// The main entry point for fsql.
pub async fn fsql() -> Result<()> {
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

    rl.save_history(".history").map_err(|err| anyhow!(err))
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}

async fn exec_and_print(_: String) -> Result<()> {
    rainbow_println("CLI is under construction. Please try Flock API directly.");
    Ok(())
}
