// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
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

//! Generate lambda functions

use handlebars::Handlebars;
use runtime::error::{Result, SquirtleError};
use serde_json::json;

const LAMBDA_TEMPLATE: &str = include_str!("templates/lambda.hbs");

/// `LambdaRequest` is used to init `Lambda`.
pub struct LambdaRequest<'a> {
    /// The serialized JSON format of the physical plan.
    pub plan_json: &'a str,
    /// The struct name of the physical plan, which is from `datafusion` crate.
    /// For example, FilterExec, HashAggregateExec, ProjectionExec.
    pub plan_name: &'a str,
    /// The filename of the generated lambda function.
    pub file_name: &'a str,
}

/// A Lambda-formatted struct to generate lambda functions with hard-coded
/// information.
#[allow(dead_code)]
pub struct Lambda {
    /// The serialized JSON format of the physical plan.
    pub plan_json:    String,
    /// The struct name of the physical plan, which is from `datafusion` crate.
    /// For example, FilterExec, HashAggregateExec, ProjectionExec.
    pub plan_name:    String,
    /// The filename of the generated lambda function.
    pub file_name:    String,
    /// The filepath of the generated lambda function (src/file_name.rs).
    pub file_path:    String,
    /// The generated source code.
    pub code:         String,
    /// Cloud function names of the next physical plan for invocation
    /// asynchronously.
    pub async_invoke: Option<Vec<String>>,
}

impl Lambda {
    /// Create a new `Lambda` without DAG and cloud information.
    pub fn try_new(request: LambdaRequest) -> Result<Lambda> {
        if request.plan_json.is_empty()
            || request.plan_name.is_empty()
            || request.file_name.is_empty()
        {
            return Err(SquirtleError::CodeGeneration(
                "LambdaRequest can't contain any empty field.".to_string(),
            ));
        }

        let mut hbs = Handlebars::new();
        hbs.register_template_string(&request.file_name, LAMBDA_TEMPLATE)
            .unwrap();
        let code = json!({
            "plan_json": request.plan_json,
            "plan_name": request.plan_name,
        });
        let code = hbs.render(&request.file_name, &code).unwrap();

        let mut path = String::from("src/");
        path.push_str(&request.file_name);
        path.push_str(".rs");

        Ok(Lambda {
            plan_json: request.plan_json.to_string(),
            plan_name: request.plan_name.to_string(),
            file_name: request.file_name.to_string(),
            code,
            file_path: path,
            async_invoke: None,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore] // it is too expensive
    fn lambda_template() {
        use crate::codegen::workspace;
        use crate::codegen::LambdaRequest;

        let plan_json = r#"{
    "predicate":{
    "physical_expr":"binary_expr",
    "left":{
        "physical_expr":"column",
        "name":"c2"
    },
    "op":"Lt",
    "right":{
        "physical_expr":"cast_expr",
        "expr":{
            "physical_expr":"literal",
            "value":{
                "Int64":99
            }
        },
        "cast_type":"Float64"
    }
    },
    "input":{
    "execution_plan":"memory_exec",
    "schema":{
        "fields":[
            {
                "name":"c1",
                "data_type":"Int64",
                "nullable":false,
                "dict_id":0,
                "dict_is_ordered":false
            },
            {
                "name":"c2",
                "data_type":"Float64",
                "nullable":false,
                "dict_id":0,
                "dict_is_ordered":false
            },
            {
                "name":"c3",
                "data_type":"Utf8",
                "nullable":false,
                "dict_id":0,
                "dict_is_ordered":false
            }
        ],
        "metadata":{

        }
    },
    "projection":[
        0,
        1,
        2
    ]
    }
}
"#;

        let plan_name = "FilterExec";
        let file_name = "mem_filter";
        let proj_name = "lambda_codegen";

        let req = LambdaRequest {
            plan_json,
            plan_name,
            file_name,
        };

        workspace(proj_name).lambda(req).build();
    }
}
