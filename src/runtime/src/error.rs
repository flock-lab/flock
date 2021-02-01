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

//! Squirtle error types

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;

use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use sqlparser::parser::ParserError;

/// Result type for operations that could result in an [SquirtleError]
pub type Result<T> = result::Result<T, SquirtleError>;

/// Squirtle error
#[derive(Debug)]
pub enum SquirtleError {
    /// Error associated to I/O operations and associated traits.
    IoError(io::Error),
    /// Error returned when SQL is syntatically incorrect.
    SQL(ParserError),
    /// Error returned when Arrow is unexpectedly executed.
    Arrow(ArrowError),
    /// Error returned when DataFusion is unexpectedly executed.
    DataFusion(DataFusionError),
    /// Error returned on a branch that we know it is possible but to which we
    /// still have no implementation for. Often, these errors are tracked in our
    /// issue tracker.
    NotImplemented(String),
    /// Error returned as a consequence of an error in Squirtle.
    /// This error should not happen in normal usage of Squirtle.
    /// Squirtle has internal invariants that we are unable to ask the
    /// compiler to check for us. This error is raised when one of those
    /// invariants is not verified during execution.
    Internal(String),
    /// This error happens whenever a plan is not valid.
    /// Examples include impossible casts, schema inference not possible and
    /// non-unique column names.
    Plan(String),
    /// Error returned during execution of the query.
    /// Examples include files not found, errors in parsing certain types.
    Execution(String),
    /// Error returned during function generation.
    FunctionGeneration(String),
}

impl From<io::Error> for SquirtleError {
    fn from(e: io::Error) -> Self {
        SquirtleError::IoError(e)
    }
}

impl From<ParserError> for SquirtleError {
    fn from(e: ParserError) -> Self {
        SquirtleError::SQL(e)
    }
}

impl From<DataFusionError> for SquirtleError {
    fn from(e: DataFusionError) -> Self {
        SquirtleError::DataFusion(e)
    }
}

impl From<ArrowError> for SquirtleError {
    fn from(e: ArrowError) -> Self {
        SquirtleError::Arrow(e)
    }
}

impl Display for SquirtleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            SquirtleError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            SquirtleError::SQL(ref desc) => write!(f, "SQL error: {:?}", desc),
            SquirtleError::Arrow(ref desc) => write!(f, "Arrow error: {}", desc),
            SquirtleError::DataFusion(ref desc) => write!(f, "DataFusion error: {:?}", desc),
            SquirtleError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
            SquirtleError::Internal(ref desc) => write!(
                f,
                "Internal error: {}. This was likely caused by a bug in Squirtle's \
                    code and we would welcome that you file an bug report in our issue tracker",
                desc
            ),
            SquirtleError::Plan(ref desc) => write!(f, "Error during planning: {}", desc),
            SquirtleError::Execution(ref desc) => write!(f, "Execution error: {}", desc),
            SquirtleError::FunctionGeneration(ref desc) => {
                write!(f, "Function generation error: {}", desc)
            }
        }
    }
}

impl error::Error for SquirtleError {}
