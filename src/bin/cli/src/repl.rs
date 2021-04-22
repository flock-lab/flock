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

use clap::{crate_version, App, Arg};
use futures::executor::block_on;
use rusoto_core::Region;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::{S3Client, S3};
use rustyline::Editor;
use std::env;
use std::f64::consts::PI;
use std::fs;
use std::io::Write;
use std::path::Path;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;
pub static S3_BUCKET: &str = "umd-squirtle";

#[tokio::main]
pub async fn main() {
    // Command line arg parsing for scqsql itself
    let matches = App::new("Squirtle")
        .version(crate_version!())
        .about("Command Line Interactive Contoller for Squirtle")
        .arg(
            Arg::with_name("function_code")
                .short("u")
                .long("upload")
                .value_name("FILE")
                .help("Upload lambda execution code to S3.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("function_key")
                .short("k")
                .long("key")
                .value_name("STRING")
                .help("AWS S3 key for this function code.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    rainbow_println(include_str!("./squirtle.txt"));

    match matches.value_of("function_code") {
        Some(bin_path) => {
            rainbow_println("============================================");
            rainbow_println("         Upload function code to S3         ");
            rainbow_println("============================================\n\n");
            if !std::path::Path::new(bin_path).exists() {
                rainbow_println(&format!(
                    "[ERROR]: function code '{}' doesn't exist!",
                    bin_path
                ));
                rainbow_println("[EXIT]: ..............");
                std::process::exit(-1);
            }

            if let Some(key) = matches.value_of("function_key") {
                put_object_to_s3(S3_BUCKET, key, bin_path).unwrap();
            } else {
                rainbow_println("[ERROR]: AWS S3 key is missing!");
                rainbow_println("[EXIT]: ..............");
                std::process::exit(-1);
            }
        }
        None => {
            cli().await;
        }
    }
}

async fn cli() {
    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    let mut query = "".to_owned();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(ref line) if is_exit_command(line) && query.is_empty() => {
                break;
            }
            Ok(ref line) if line.trim_end().ends_with(';') => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone());
                match exec_and_print(query).await {
                    Ok(_) => {}
                    Err(err) => println!("{:?}", err),
                }
                query = "".to_owned();
            }
            Ok(ref line) => {
                query.push_str(line);
                query.push(' ');
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}

async fn exec_and_print(_: String) -> Result<(), Error> {
    rainbow_println("CLI is under construction. Please try Squirtle API directly.");
    Ok(())
}

/// Prints the text in the rainbow fansion.
pub fn rainbow_println(line: &str) {
    let frequency: f64 = 0.1;
    let spread: f64 = 3.0;
    for (i, c) in line.char_indices() {
        let (r, g, b) = rgb(frequency, spread, i as f64);
        print!("\x1b[38;2;{};{};{}m{}\x1b[0m", r, g, b, c);
    }
    println!();
}

/// Generates RGB for rainbow print.
fn rgb(freq: f64, spread: f64, i: f64) -> (u8, u8, u8) {
    let j = i / spread;
    let red = (freq * j + 0.0).sin() * 127.0 + 128.0;
    let green = (freq * j + 2.0 * PI / 3.0).sin() * 127.0 + 128.0;
    let blue = (freq * j + 4.0 * PI / 3.0).sin() * 127.0 + 128.0;

    (red as u8, green as u8, blue as u8)
}

/// Puts a lambda function code to AWS S3.
pub fn put_object_to_s3(bucket: &str, key: &str, obj_path: &str) -> Result<(), Error> {
    rainbow_println("[UPLOAD] ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒... ... ... ...");
    // compress lambda function code to bootstrap.zip
    let fname = Path::new(obj_path).parent().unwrap().join("bootstrap.zip");
    let w = std::fs::File::create(&fname)?;
    let mut zip = zip::ZipWriter::new(w);
    let options = zip::write::FileOptions::default()
        .compression_method(zip::CompressionMethod::Bzip2)
        .unix_permissions(777);
    zip.start_file("bootstrap", options)?;
    zip.write_all(&fs::read(&obj_path)?)?;
    zip.finish()?;

    if !fname.exists() {
        rainbow_println(&format!(
            "[ERROR]: failed to rename the binary {} to {:?}!",
            obj_path, fname
        ));
        rainbow_println("[EXIT]: ..............");
        std::process::exit(-1);
    }

    // uploads bootstrap.zip to AWS S3
    if let Ok(bytes) = fs::read(&fname) {
        let put_obj_req = PutObjectRequest {
            bucket: String::from(bucket),
            key: String::from(key),
            body: Some(bytes.into()),
            ..Default::default()
        };

        let client = S3Client::new(Region::default());
        block_on(client.put_object(put_obj_req))?;
        rainbow_println("[OK] Upload Succeed.");
    } else {
        rainbow_println(&format!("[ERROR]: failed to read {:?}!", fname));
        rainbow_println("[EXIT]: ..............");
        std::process::exit(-1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rainbow_print() {
        let text = include_str!("./squirtle.txt");
        rainbow_println(text);
    }
}
