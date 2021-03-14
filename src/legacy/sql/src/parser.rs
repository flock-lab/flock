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

//! SQL Parser
//!
//! Declares a SQL parser based on sqlparser that handles custom formats that we
//! need.

use sqlparser::{
    ast::Statement as SQLStatement,
    dialect::{keywords::Keyword, Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

/// Squirtle extension DDL for `EXPLAIN` and `EXPLAIN VERBOSE`
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainPlan {
    /// If true, dumps more intermediate plans and results of optimizaton passes
    pub verbose:   bool,
    /// The statement for which to generate an planning explanation
    pub statement: Box<Statement>,
}

/// Squirtle Statement representations.
///
/// Tokens parsed by `SCQParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// ANSI SQL AST node
    Statement(SQLStatement),
    /// Extension: `EXPLAIN <SQL>`
    Explain(ExplainPlan),
}

/// SQL Parser
pub struct SCQParser {
    parser: Parser,
}

impl SCQParser {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        SCQParser::new_with_dialect(sql, dialect)
    }

    /// Parse the specified tokens with dialect
    pub fn new_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<Self, ParserError> {
        let mut stream = false;
        let mut tsql = sql.to_owned();
        tsql = tsql.chars().filter(|c| !c.is_whitespace()).collect();
        if tsql.to_uppercase().contains("SELECTSTREAM") {
            stream = true;
        }

        let mut sql = sql.to_owned();
        if stream {
            sql = sql.to_uppercase().replacen("STREAM", "", 1);
        }

        let mut tokenizer = Tokenizer::new(dialect, sql.as_str());
        let tokens = tokenizer.tokenize()?;
        Ok(SCQParser {
            parser: Parser::new(tokens),
        })
    }

    /// Parse a SQL statement and produce a set of statements with dialect
    pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = &GenericDialect {};
        SCQParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<Vec<Statement>, ParserError> {
        let mut parser = SCQParser::new_with_dialect(sql, dialect)?;
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::NoKeyword if w.value.to_uppercase() == "EXPLAIN" => {
                        self.parser.next_token();
                        self.parse_explain()
                    }
                    _ => {
                        // use the native parser
                        Ok(Statement::Statement(self.parser.parse_statement()?))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(Statement::Statement(self.parser.parse_statement()?))
            }
        }
    }

    /// Parse an SQL EXPLAIN statement.
    pub fn parse_explain(&mut self) -> Result<Statement, ParserError> {
        // Parser is at the token immediately after EXPLAIN
        // Check for EXPLAIN VERBOSE
        let verbose = match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::NoKeyword if w.value.to_uppercase() == "VERBOSE" => {
                    self.parser.next_token();
                    true
                }
                _ => false,
            },
            _ => false,
        };

        let statement = Box::new(self.parse_statement()?);
        let explain_plan = ExplainPlan { statement, verbose };
        Ok(Statement::Explain(explain_plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stream_query() -> Result<(), ParserError> {
        let sql1 = "SELECT STREAM * FROM Orders;";
        let sql2 = "SELECT * FROM Orders;";
        let statements1 = SCQParser::parse_sql(sql1.to_uppercase().as_str())?;
        let statements2 = SCQParser::parse_sql(sql2.to_uppercase().as_str())?;
        assert_eq!(statements1.len(), statements2.len());
        assert_eq!(statements1[0], statements2[0]);
        Ok(())
    }
}
