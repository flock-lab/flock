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

//! The basic manipulation utilities for code generation.

/// Regex to replace multiple spaces with a single space in a string
#[macro_export]
macro_rules! scope_format {
    ($scope:ident) => {
        Regex::new(r"\s+")
            .unwrap()
            .replace_all($scope.to_string().as_str(), " ")
    };
}

#[cfg(test)]
mod tests {
    use codegen::Scope;
    use regex::Regex;

    #[test]
    fn single_struct() {
        let mut scope = Scope::new();

        scope
            .new_struct("Foo")
            .field("one", "usize")
            .field("two", "String");

        let expect = "struct Foo { one: usize, two: String, }";
        assert_eq!(scope_format!(scope), expect);
    }
}
