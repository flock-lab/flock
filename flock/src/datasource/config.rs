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

//! A simple configuration parser for data sources.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::str::FromStr;

/// This is a simple command line options parser.
#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq)]
pub struct Config {
    /// The data source configuration.
    pub args: HashMap<String, String>,
}

impl Config {
    /// Creates a new `Config`.
    pub fn new() -> Self {
        Config {
            args: HashMap::new(),
        }
    }

    /// Parses the command line arguments into a new Config object.
    ///
    /// Its parsing strategy is as follows:
    ///   If an argument starts with --, the remaining string is used as the key
    ///   and the next argument as the associated value.
    ///   Otherwise the argument is used as the next positional value, counting
    ///   from zero.
    pub fn from<I: Iterator<Item = String>>(mut cmd_args: I) -> Result<Self> {
        let mut args = HashMap::new();
        let mut i = 0;
        while let Some(arg) = cmd_args.next() {
            if let Some(key) = arg.strip_prefix("--") {
                match cmd_args.next() {
                    Some(value) => args.insert(key.to_string(), value),
                    None => return Err(Error::new(ErrorKind::Other, "No corresponding value.")),
                };
            } else {
                args.insert(format!("{}", i), arg);
                i += 1;
            }
        }
        Ok(Config { args })
    }

    /// Inserts the given value for the given key.
    ///
    /// If the key already exists, its value is overwritten.
    pub fn insert(&mut self, key: &str, value: String) {
        self.args.insert(String::from(key), value);
    }

    /// Returns the value for the given key, if available.
    pub fn get(&self, key: &str) -> Option<String> {
        self.args.get(key).cloned()
    }

    /// Returns the value for the given key automatically parsed if possible.
    pub fn get_as<T: FromStr>(&self, key: &str) -> Option<T> {
        self.args.get(key).and_then(|x| x.parse::<T>().ok())
    }

    /// Returns the value for the given key or a default value if the key does
    /// not exist.
    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.args
            .get(key)
            .map_or(String::from(default), |x| x.clone())
    }

    /// Returns the value for the given key automatically parsed, or a default
    /// value if the key does not exist.
    pub fn get_as_or<T: FromStr>(&self, key: &str, default: T) -> T {
        self.get_as(key).unwrap_or(default)
    }
}
