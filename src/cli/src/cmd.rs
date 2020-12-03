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

use prettytable::format;
use rustyline::completion::Pair as RustlinePair;

use crate::env::Env;
use crate::output::ScqWriter;

lazy_static! {
    static ref TBLFMT: format::TableFormat = format::FormatBuilder::new()
        .separators(
            &[format::LinePosition::Title, format::LinePosition::Bottom],
            format::LineSeparator::new('-', '+', '+', '+')
        )
        .padding(1, 1)
        .build();
}

pub trait Cmd {
    // break if returns true
    fn exec(
        &self,
        env: &mut Env,
        args: &mut dyn Iterator<Item = &str>,
        writer: &mut ScqWriter,
    ) -> bool;
    fn is(&self, l: &str) -> bool;
    fn get_name(&self) -> &'static str;
    fn try_complete(&self, index: usize, prefix: &str, env: &Env) -> Vec<RustlinePair>;
    fn complete_option(&self, prefix: &str) -> Vec<RustlinePair>;
    fn write_help(&self, writer: &mut ScqWriter);
    fn about(&self) -> &'static str;
}
