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

use ansi_term::Colour::{Blue, Green, Red};
use chrono::Local;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempdir::TempDir;

/// Keep track of our repl environment
#[allow(dead_code)]
pub struct Env {
    pub quit: bool,
    pub need_new_editor: bool,
    pub ctrlcbool: Arc<AtomicBool>,
    pub prompt: String,
    range_str: Option<String>,
    pub tempdir: std::io::Result<TempDir>,
}

lazy_static! {
    static ref CTC_BOOL: Arc<AtomicBool> = {
        let b = Arc::new(AtomicBool::new(false));
        let r = b.clone();
        ctrlc::set_handler(move || {
            r.store(true, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");
        b
    };
}

#[allow(dead_code)]
impl Env {
    pub fn new() -> Env {
        let now = Local::now();
        let specific_time = now.date().and_hms(18, 0, 0);
        let duration = specific_time.signed_duration_since(now);
        let hm = format!("{}:{}", duration.num_hours(), duration.num_minutes() % 60);
        let env = Env {
            quit: false,
            need_new_editor: false,
            ctrlcbool: CTC_BOOL.clone(),
            prompt: format!(
                "[{}] [{}] [{}] > ",
                Blue.paint(hm),
                Green.paint("scqsql"),
                Red.paint("=#")
            ),
            range_str: None,
            tempdir: TempDir::new("scqsql"),
        };
        env
    }
}
