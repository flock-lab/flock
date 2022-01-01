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

//! Flock CLI creates/lists/deletes AWS Lambda functions.

use anyhow::{Ok, Result};
use benchmarks::rainbow_println;
use clap::{App, Arg, ArgMatches, SubCommand};
use rusoto_core::Region;
use rusoto_lambda::{DeleteFunctionRequest, Lambda, LambdaClient, ListFunctionsRequest};

pub fn command(matches: &ArgMatches) -> Result<()> {
    if matches.is_present("delete function") {
        futures::executor::block_on(delete_function(matches.value_of("delete function")))?;
    } else if matches.is_present("list functions") {
        futures::executor::block_on(list_functions(matches.value_of("list functions")))?;
    } else if matches.is_present("delete all functions") {
        futures::executor::block_on(delete_all_functions())?;
    } else if matches.is_present("list all functions") {
        futures::executor::block_on(list_all_functions())?;
    }

    Ok(())
}

pub fn command_args() -> App<'static, 'static> {
    SubCommand::with_name("lambda")
        .about("The AWS Lambda Tool for Flock")
        .arg(
            Arg::with_name("delete function")
                .short("d")
                .long("delete")
                .value_name("FUNCTION_NAME")
                .help("Deletes a lambda function")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("delete all functions")
                .short("D")
                .long("delete-all")
                .help("Deletes all lambda functions"),
        )
        .arg(
            Arg::with_name("list functions")
                .short("l")
                .long("list")
                .help("Lists all lambda functions (-l all, or a function name)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("list all functions")
                .short("L")
                .long("list-all")
                .help("Lists all lambda functions"),
        )
}

/// Delete Lambda functions matching the given pattern.
///
/// # Arguments
/// * `pattern` - The pattern to match the function names. If None, all
///   functions are deleted.
async fn delete_function(pattern: Option<&str>) -> Result<()> {
    let tasks = list_functions(pattern)
        .await?
        .into_iter()
        .map(|name| {
            tokio::spawn(async move {
                let request = DeleteFunctionRequest {
                    function_name: name,
                    ..Default::default()
                };
                LambdaClient::new(Region::default())
                    .delete_function(request)
                    .await
            })
        })
        .collect::<Vec<_>>();

    futures::future::join_all(tasks).await;

    rainbow_println(format!(
        "[OK] deleted all functions matching the pattern: {:?}",
        pattern
    ));

    Ok(())
}

/// Deletes all AWS Lambda functions.
///
/// # Arguments
/// * `pattern` - The pattern to match the function names. If None, all
///   functions are deleted.
async fn delete_all_functions() -> Result<()> {
    delete_function(None).await?;
    rainbow_println("[OK] deleted all functions");
    Ok(())
}

/// Lists Lambda functions matching the pattern.
///
/// # Arguments
/// * `pattern` - A pattern to filter the function names. If `None`, all
///   functions are listed.
///
/// # Returns
/// A vector of function names.
async fn list_functions(pattern: Option<&str>) -> Result<Vec<String>> {
    let client = LambdaClient::new(Region::default());
    let mut request = ListFunctionsRequest {
        ..Default::default()
    };

    rainbow_println("[OK] listing functions");

    let mut function_names = vec![];

    // `list_functions()` returns a list of Lambda functions, with the version
    // specific configuration of each. Lambda returns up to 50 functions per
    // call. Set `FunctionVersion` to `ALL` to include all published versions of
    // each function in addition to the unpublished version.
    loop {
        let response = client.list_functions(request.clone()).await?;
        if let Some(functions) = response.functions {
            for function in functions {
                if let Some(function_name) = function.function_name {
                    function_names.push(function_name);
                }
            }
        }
        if response.next_marker.is_none() {
            break;
        }
        request.marker = response.next_marker;
    }

    if function_names.is_empty() {
        rainbow_println("No functions found.");
        return Ok(function_names);
    }

    match pattern {
        Some(pattern) => {
            if pattern == "all" || pattern == "ALL" {
                rainbow_println(function_names.join("\n"));
            } else {
                let mut matching_functions = vec![];
                for function_name in function_names {
                    if function_name.contains(pattern) {
                        matching_functions.push(function_name);
                    }
                }
                if matching_functions.is_empty() {
                    rainbow_println(format!("No functions found matching {}", pattern));
                } else {
                    for function_name in &matching_functions {
                        rainbow_println(function_name);
                    }
                }
                function_names = matching_functions;
            }
        }
        None => {
            rainbow_println(function_names.join("\n"));
        }
    }

    Ok(function_names)
}

/// Lists all AWS Lambda functions.
async fn list_all_functions() -> Result<Vec<String>> {
    list_functions(None).await
}
