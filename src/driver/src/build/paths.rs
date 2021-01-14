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

use filetime::{self, FileTime};
use lazy_static::lazy_static;
use std::env;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::Command;

static CARGO_INTEGRATION_SCQ_DIR: &str = "scq";

lazy_static! {
    static ref GLOBAL_ROOT: PathBuf = {
        let mut path = t!(env::current_exe());
        path.pop(); // chop off exe name
        path.pop(); // chop off 'debug'

        if path.file_name().and_then(|s| s.to_str()) != Some("target") {
            path.pop();
        }

        path.push(CARGO_INTEGRATION_SCQ_DIR);
        path.mkdir_p();
        path
    };
}

pub fn root() -> PathBuf {
    GLOBAL_ROOT.to_path_buf()
}

pub fn home() -> PathBuf {
    let mut path = root();
    path.push("home");
    path.mkdir_p();
    path
}

pub trait CargoPathExt {
    fn rm_rf(&self);
    fn mkdir_p(&self);

    fn move_into_the_past(&self) {
        self.move_in_time(|sec, nsec| (sec - 3600, nsec))
    }

    fn move_into_the_future(&self) {
        self.move_in_time(|sec, nsec| (sec + 3600, nsec))
    }

    fn move_in_time<F>(&self, travel_amount: F)
    where
        F: Fn(i64, u32) -> (i64, u32);

    fn is_symlink(&self) -> bool;
}

impl CargoPathExt for Path {
    fn rm_rf(&self) {
        let meta = match self.symlink_metadata() {
            Ok(meta) => meta,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return;
                }
                panic!("failed to remove {:?}, could not read: {:?}", self, e);
            }
        };
        // There is a race condition between fetching the metadata and
        // actually performing the removal, but we don't care all that much
        // for our tests.
        if meta.is_dir() {
            if let Err(e) = remove_dir_all::remove_dir_all(self) {
                panic!("failed to remove {:?}: {:?}", self, e)
            }
        } else if let Err(e) = fs::remove_file(self) {
            panic!("failed to remove {:?}: {:?}", self, e)
        }
    }

    fn mkdir_p(&self) {
        fs::create_dir_all(self)
            .unwrap_or_else(|e| panic!("failed to mkdir_p {}: {}", self.display(), e))
    }

    fn move_in_time<F>(&self, travel_amount: F)
    where
        F: Fn(i64, u32) -> (i64, u32),
    {
        if self.is_file() {
            time_travel(self, &travel_amount);
        } else {
            recurse(self, &self.join("target"), &travel_amount);
        }

        fn recurse<F>(p: &Path, bad: &Path, travel_amount: &F)
        where
            F: Fn(i64, u32) -> (i64, u32),
        {
            if p.is_file() {
                time_travel(p, travel_amount)
            } else if !p.starts_with(bad) {
                for f in t!(fs::read_dir(p)) {
                    let f = t!(f).path();
                    recurse(&f, bad, travel_amount);
                }
            }
        }

        fn time_travel<F>(path: &Path, travel_amount: &F)
        where
            F: Fn(i64, u32) -> (i64, u32),
        {
            let stat = t!(path.symlink_metadata());

            let mtime = FileTime::from_last_modification_time(&stat);

            let (sec, nsec) = travel_amount(mtime.unix_seconds(), mtime.nanoseconds());
            let newtime = FileTime::from_unix_time(sec, nsec);

            // Sadly change_file_times has a failure mode where a readonly file
            // cannot have its times changed on windows.
            do_op(path, "set file times", |path| {
                filetime::set_file_times(path, newtime, newtime)
            });
        }
    }

    fn is_symlink(&self) -> bool {
        fs::symlink_metadata(self)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    }
}

fn do_op<F>(path: &Path, desc: &str, mut f: F)
where
    F: FnMut(&Path) -> io::Result<()>,
{
    match f(path) {
        Ok(()) => {}
        Err(ref e) if e.kind() == ErrorKind::PermissionDenied => {
            let mut p = t!(path.metadata()).permissions();
            p.set_readonly(false);
            t!(fs::set_permissions(path, p));

            // Unix also requires the parent to not be readonly for example when
            // removing files
            let parent = path.parent().unwrap();
            let mut p = t!(parent.metadata()).permissions();
            p.set_readonly(false);
            t!(fs::set_permissions(parent, p));

            f(path).unwrap_or_else(|e| {
                panic!("failed to {} {}: {}", desc, path.display(), e);
            })
        }
        Err(e) => {
            panic!("failed to {} {}: {}", desc, path.display(), e);
        }
    }
}

/// Get the filename for a library.
///
/// `kind` should be one of: "lib", "rlib", "staticlib", "dylib", "proc-macro"
///
/// For example, dynamic library named "foo" would return:
/// - macOS: "libfoo.dylib"
/// - Windows: "foo.dll"
/// - Unix: "libfoo.so"
pub fn get_lib_filename(name: &str, kind: &str) -> String {
    let prefix = get_lib_prefix(kind);
    let extension = get_lib_extension(kind);
    format!("{}{}.{}", prefix, name, extension)
}

pub fn get_lib_prefix(kind: &str) -> &str {
    match kind {
        "lib" | "rlib" => "lib",
        "staticlib" | "dylib" | "proc-macro" => {
            if cfg!(windows) {
                ""
            } else {
                "lib"
            }
        }
        _ => unreachable!(),
    }
}

pub fn get_lib_extension(kind: &str) -> &str {
    match kind {
        "lib" | "rlib" => "rlib",
        "staticlib" => {
            if cfg!(windows) {
                "lib"
            } else {
                "a"
            }
        }
        "dylib" | "proc-macro" => {
            if cfg!(windows) {
                "dll"
            } else if cfg!(target_os = "macos") {
                "dylib"
            } else {
                "so"
            }
        }
        _ => unreachable!(),
    }
}

/// Returns the sysroot as queried from rustc.
pub fn sysroot() -> String {
    let output = Command::new("rustc")
        .arg("--print=sysroot")
        .output()
        .expect("rustc to run");
    assert!(output.status.success());
    let sysroot = String::from_utf8(output.stdout).unwrap();
    sysroot.trim().to_string()
}

/// Returns the binary's absolute path.
pub fn find_it<P>(exe_name: P) -> Option<PathBuf>
where
    P: AsRef<Path>,
{
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .filter_map(|dir| {
                let full_path = dir.join(&exe_name);
                if full_path.is_file() {
                    Some(full_path)
                } else {
                    None
                }
            })
            .next()
    })
}
