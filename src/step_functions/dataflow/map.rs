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

#[path = "./common.rs"]
mod common;
use common::Common;

#[path = "./graph.rs"]
mod graph;
use graph::Dataflow;

#[path = "./paths.rs"]
mod paths;
use paths::{ResultPath, ResultSelector};

/// The Map state ("Type": "Map") can be used to run a set of steps for each
/// element of an input array. While the Parallel state executes multiple
/// branches of steps using the same input, a Map state will execute the same
/// steps for multiple entries of an array in the state input.
///
/// Consider the following input data for a Map state.
/// {
///  "ship-date": "2016-03-14T01:59:00Z",
///  "detail": {
///    "delivery-partner": "UQS",
///    "shipped": [
///      { "prod": "R31", "dest-code": 9511, "quantity": 1344 },
///      { "prod": "S39", "dest-code": 9511, "quantity": 40 },
///      { "prod": "R31", "dest-code": 9833, "quantity": 12 },
///      { "prod": "R40", "dest-code": 9860, "quantity": 887 },
///      { "prod": "R40", "dest-code": 9511, "quantity": 1220 }
///    ]
///  }
/// }
///
/// Given the previous input, the Map state in the following example will invoke
/// a AWS Lambda function (ship-val) once for each item of the array in the
/// shipped field.
///
/// "Validate-All": {
///  "Type": "Map",
///  "InputPath": "$.detail",
///  "ItemsPath": "$.shipped",
///  "MaxConcurrency": 0,
///  "Iterator": {
///    "StartAt": "Validate",
///    "States": {
///      "Validate": {
///        "Type": "Task",
///        "Resource" : "arn:aws:lambda:us-east-1:xxxx:function:ship-val",
///        "End": true
///      }
///    }
///  },
///  "ResultPath": "$.detail.shipped",
///  "End": true
/// }
///
/// Each iteration of the Map state will send an item in the array (selected
/// with the ItemsPath field) as input to the Lambda function. For instance, the
/// input to one invocation of Lambda would be the following.
///
/// {
///  "prod": "R31",
///  "dest-code": 9511,
///  "quantity": 1344
/// }
#[allow(dead_code)]
pub struct Map {
    /// Common state fields.
    pub common:          Common,
    /// The Iterator field’s value is an object that defines a state machine
    /// which will process each element of the array.
    pub iterator:        Dataflow,
    /// The ItemsPath field’s value is a reference path identifying where in the
    /// effective input the array field is found. For more information, see
    /// ItemsPath. States within an Iterator field can only transition to
    /// each other, and no state outside the ItemsPath field can transition to a
    /// state within it.
    /// If any iteration fails, entire Map state fails, and all iterations are
    /// terminated.
    pub items_path:      Option<String>,
    /// The MaxConcurrencyfield’s value is an integer that provides an upper
    /// bound on how many invocations of the Iterator may run in parallel. For
    /// instance, a MaxConcurrency value of 10 will limit your Map state to 10
    /// concurrent iterations running at one time.
    /// The default value is 0, which places no quota on parallelism and
    /// iterations are invoked as concurrently as possible.
    pub max_concurrency: Option<String>,
    /// Specifies where (in the input) to place the results of executing the
    /// task that's specified in Resource. The input is then filtered as
    /// specified by the OutputPath field (if present) before being used as the
    /// state's output.
    pub result_path:     Option<ResultPath>,
    /// Use the `ResultSelector` field to manipulate a state's result before
    /// `ResultPath` is applied. The `ResultSelector` field lets you create a
    /// collection of key value pairs, where the values are static or selected
    /// from the state's result. The output of ResultSelector replaces the
    /// state's result and is passed to ResultPath.
    pub result_selector: Option<ResultSelector>,
}
