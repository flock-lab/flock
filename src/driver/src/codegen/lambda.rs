// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
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

//! Generate a AWS Lambda function.

#[cfg(test)]
mod tests {
    use codegen::*;
    use regex::Regex;

    #[test]
    fn single_struct() {
        let mut scope = Scope::new();
        scope
            .new_struct("Foo")
            .field("one", "usize")
            .field("two", "String");

        assert_eq!(
            "struct Foo { one: usize, two: String, }",
            scope_format!(scope)
        );
    }

    #[test]
    fn struct_with_pushed_field() {
        let mut scope = Scope::new();
        let mut struct_ = Struct::new("Foo");
        let field = Field::new("one", "usize");
        struct_.push_field(field);
        scope.push_struct(struct_);

        assert_eq!("struct Foo { one: usize, }", scope_format!(scope));
    }

    #[test]
    fn single_lambda() {
        let mut scope = Scope::new();

        let license = r#"
// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
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

"#;
        scope.import("lambda", "handler_fn");
        scope.import("lambda", "Context");
        scope.import("serde_json", "json");
        scope.import("serde_json", "Value");

        scope.raw(r#"type Error = Box<dyn std::error::Error + Sync + Send + 'static>;"#);

        scope
            .new_fn("handler")
            .set_async(true)
            .arg("event", Type::new("Value"))
            .arg("_", Type::new("Context"))
            .ret(Type::new("Result<Value, Error>"))
            .line("let message = event[\"input\"].as_str().unwrap();")
            .line("let event = json!({ \"input\": format!(\"{} {}\", \"Hello!\", message) });")
            .line("Ok(event)");

        scope
            .new_fn("main")
            .set_async(true)
            .ret(Type::new("Result<(), Error>"))
            .attr("tokio::main")
            .line("lambda::run(handler_fn(handler)).await?;")
            .line("Ok(())");

        println!("{}", format!("{}{}", license, scope.to_string()));
    }
}
