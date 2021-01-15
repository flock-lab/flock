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

//! Generate a rust crate per query

use crate::build::project;
use crate::codegen::lambda::{Lambda, LambdaRequest};
use crate::codegen::toml::Toml;

/// A Cargo workspace for cloud workflow generation.
pub struct Workspace {
    /// A TOML-formatted struct to generate build scripts.
    toml:  Toml,
    /// A vector to generate all lambda functions.
    files: Vec<Lambda>,
    /// The workspace name.
    name:  String,
}

impl Workspace {
    /// Create a new workspace.
    pub fn new(name: &str) -> Self {
        Self {
            toml:  Toml::new(name),
            files: vec![],
            name:  (*name).to_string(),
        }
    }

    /// Add a lambda function to the workspace.
    pub fn lambda(&mut self, request: LambdaRequest) -> &mut Self {
        self.toml.add_bin(&request.file_name);
        self.files.push(Lambda::try_new(request).unwrap());
        self
    }

    /// Build the workspace
    pub fn build(&mut self) {
        let mut p = project(&self.name)
            .file("Cargo.toml", self.toml.cargo())
            .file("Xargo.toml", self.toml.xargo());

        for lambda in self.files.iter() {
            p = p.file(&lambda.file_path, &lambda.code);
        }

        let args = format!(
            "build --target x86_64-unknown-linux-gnu --release -j{}",
            num_cpus::get()
        );
        p.build().cargo(&args).run();
    }

    /// Minimize the lambda function's binary size
    pub fn strip(&self) {
        unimplemented!();
    }
}
