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

/// Batch processing is the processing of a large volume of data all at once.
/// You can store the preceding reference data as an object in Amazon Simple
/// Storage Service (Amazon S3).  Squirtle reads the Amazon S3 object and
/// creates an in-application reference table that you can query in your
/// application code. In your application code, you write a join query to join
/// the in-application stream with the in-application reference table, to obtain
/// more accurate results.
#[allow(dead_code)]
pub struct BatchQuery {}
