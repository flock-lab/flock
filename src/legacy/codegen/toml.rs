// Copyright 2020 UMD Database Group. All Rights Reserved.
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

//! Generate toml files for each query.

use handlebars::Handlebars;
use serde_json::json;

const TOML: &str = r#"
[[bin]]
name = "{{ name }}"
path = "src/{{ name }}.rs"
"#;

/// A TOML-formatted struct to generate the cargo workspace.
pub struct Toml {
    /// Cargo.toml in a crate
    cargo: String,
    /// Xargo.toml in a crate
    xargo: String,
    /// All binaries to be generated
    bins:  Vec<String>,
}

impl Toml {
    /// Create a new toml
    pub fn new(name: &str) -> Self {
        let mut hbs = Handlebars::new();
        hbs.register_template_string(name, include_str!("templates/Cargo.hbs"))
            .unwrap();
        let ws_name = json!({
            "name": name.to_string(),
        });

        Self {
            cargo: hbs.render(name, &ws_name).unwrap(),
            xargo: include_str!("templates/Xargo.hbs").to_string(),
            bins:  vec![],
        }
    }

    /// Add a new binary description.
    pub fn add_bin(&mut self, name: &str) {
        let mut hbs = Handlebars::new();
        hbs.register_template_string(name, TOML).unwrap();
        let bin_name = json!({
            "name": name.to_string(),
        });
        let bin = hbs.render(name, &bin_name).unwrap();
        self.bins.push(bin);
    }

    /// Return a Cargo.toml for code generation.
    pub fn cargo(&mut self) -> &str {
        self.cargo.push_str(self.bins.join("\n").as_str());
        self.bins.clear();
        self.cargo.as_str()
    }

    /// Return a Xargo.toml for code generation.
    pub fn xargo(&self) -> &str {
        self.xargo.as_str()
    }
}
