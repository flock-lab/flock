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

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
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
