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

//! Flock error types

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;

use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use sqlparser::parser::ParserError;

/// Result type for operations that could result in an [FlockError]
pub type Result<T> = result::Result<T, FlockError>;

/// Flock error
#[derive(Debug)]
pub enum FlockError {
    /// Error associated to Lambda runtime execution.
    LambdaError(Box<dyn std::error::Error + Send + Sync>),
    /// Error associated to I/O operations and associated traits.
    IoError(io::Error),
    /// Error returned when SQL is syntatically incorrect.
    SQL(ParserError),
    /// Error returned when Arrow is unexpectedly executed.
    Arrow(ArrowError),
    /// Error returned when DataFusion is unexpectedly executed.
    DataFusion(DataFusionError),
    /// Error returned when Base64 decoding fails.
    Base64(base64::DecodeError),
    /// Error returned when serde_json failed to serialize or deserialize data.
    SerdeJson(serde_json::Error),
    /// Error returned on a branch that we know it is possible but to which we
    /// still have no implementation for. Often, these errors are tracked in our
    /// issue tracker.
    NotImplemented(String),
    /// Error returned as a consequence of an error in Flock.
    /// This error should not happen in normal usage of Flock.
    /// Flock has internal invariants that we are unable to ask the
    /// compiler to check for us. This error is raised when one of those
    /// invariants is not verified during execution.
    Internal(String),
    /// This error happens whenever a plan is not valid.
    /// Examples include impossible casts, schema inference not possible and
    /// non-unique column names.
    Plan(String),
    /// Error returned when the DAG partition failed in Flock.
    /// This error should not happen in normal usage of Flock.
    DagPartition(String),
    /// Error returned during execution of the query.
    /// Examples include files not found, errors in parsing certain types.
    Execution(String),
    /// Error returned during function generation.
    FunctionGeneration(String),
    /// Error returned when the data sink fails to write data.
    /// This error should not happen in normal usage of Flock.
    DataSink(String),
    /// Error returned when accessing the AWS services fails.
    AWS(String),
}

impl From<io::Error> for FlockError {
    fn from(e: io::Error) -> Self {
        FlockError::IoError(e)
    }
}

impl From<ParserError> for FlockError {
    fn from(e: ParserError) -> Self {
        FlockError::SQL(e)
    }
}

impl From<DataFusionError> for FlockError {
    fn from(e: DataFusionError) -> Self {
        FlockError::DataFusion(e)
    }
}

impl From<ArrowError> for FlockError {
    fn from(e: ArrowError) -> Self {
        FlockError::Arrow(e)
    }
}

impl From<serde_json::Error> for FlockError {
    fn from(e: serde_json::Error) -> Self {
        FlockError::SerdeJson(e)
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for FlockError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        FlockError::LambdaError(e)
    }
}

impl From<base64::DecodeError> for FlockError {
    fn from(e: base64::DecodeError) -> Self {
        FlockError::Base64(e)
    }
}

impl From<&str> for FlockError {
    fn from(e: &str) -> Self {
        FlockError::Internal(e.to_string())
    }
}

impl Display for FlockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            FlockError::Base64(ref desc) => write!(f, "Base64 error: {}", desc),
            FlockError::LambdaError(ref desc) => write!(f, "Lambda error: {}", desc),
            FlockError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            FlockError::SQL(ref desc) => write!(f, "SQL error: {:?}", desc),
            FlockError::Arrow(ref desc) => write!(f, "Arrow error: {}", desc),
            FlockError::DataFusion(ref desc) => write!(f, "DataFusion error: {:?}", desc),
            FlockError::SerdeJson(ref desc) => write!(f, "serde_json error: {:?}", desc),
            FlockError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
            FlockError::Internal(ref desc) => write!(
                f,
                "Internal error: {}. This was likely caused by a bug in Flock's \
                    code and we would welcome that you file an bug report in our issue tracker",
                desc
            ),
            FlockError::Plan(ref desc) => write!(f, "Error during planning: {}", desc),
            FlockError::DagPartition(ref desc) => {
                write!(f, "Error during DAG partitioning: {}", desc)
            }
            FlockError::Execution(ref desc) => write!(f, "Execution error: {}", desc),
            FlockError::FunctionGeneration(ref desc) => {
                write!(f, "Function generation error: {}", desc)
            }
            FlockError::DataSink(ref desc) => write!(f, "Data sink error: {}", desc),
            FlockError::AWS(ref desc) => write!(f, "AWS error: {}", desc),
        }
    }
}

impl error::Error for FlockError {}
