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

use crate::rainbow::rainbow_println;
use anyhow::{Ok, Result};
use rusoto_core::Region;
use rusoto_lambda::{DeleteFunctionRequest, Lambda, LambdaClient, ListFunctionsRequest};

/// Delete a Lambda function.
///
/// # Arguments
/// * `name` - The name of the function to delete.
pub async fn delete_function(name: &str) -> Result<()> {
    let request = DeleteFunctionRequest {
        function_name: name.to_string(),
        ..Default::default()
    };

    LambdaClient::new(Region::default())
        .delete_function(request)
        .await?;

    rainbow_println(&format!("[OK] deleted function {}", name));

    Ok(())
}

/// Deletes all AWS Lambda functions.
pub async fn delete_all_functions() -> Result<()> {
    let tasks = list_functions(None)
        .await?
        .into_iter()
        .map(|name| tokio::spawn(async move { delete_function(&name).await }))
        .collect::<Vec<_>>();

    futures::future::join_all(tasks).await;

    rainbow_println("[OK] deleted all functions");

    Ok(())
}

/// Lists all Lambda functions.
///
/// # Arguments
/// * `pattern` - A pattern to filter the function names. If `None`, all
///   functions are listed. `*` and  `all` are wildcards for all functions.
///
/// # Returns
/// A vector of function names.
pub async fn list_functions(pattern: Option<&str>) -> Result<Vec<String>> {
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
            if pattern == "*" || pattern == "all" || pattern == "ALL" {
                rainbow_println(&function_names.join("\n"));
            } else {
                let mut matching_functions = vec![];
                for function_name in function_names {
                    if function_name.contains(pattern) {
                        matching_functions.push(function_name);
                    }
                }
                if matching_functions.is_empty() {
                    rainbow_println(&format!("No functions found matching {}", pattern));
                } else {
                    for function_name in &matching_functions {
                        rainbow_println(function_name);
                    }
                }
                function_names = matching_functions;
            }
        }
        None => {
            rainbow_println(&function_names.join("\n"));
        }
    }

    Ok(function_names)
}
