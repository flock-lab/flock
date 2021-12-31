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

//! Flock CLI reads/writes objects from/to AWS S3.

use crate::rainbow::rainbow_println;
use anyhow::{bail, Result};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use ini::Ini;
use lazy_static::lazy_static;
use rusoto_core::Region;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::{S3Client, S3};
use std::fs;
use std::io::Write;
use std::path::Path;

lazy_static! {
    /// Global settings.
    pub static ref FLOCK_CONF: Ini = Ini::load_from_str(include_str!("../../flock/src/config.toml")).unwrap();
    pub static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["flock"]["s3_bucket"].to_string();
}

pub fn command(matches: &ArgMatches) -> Result<()> {
    futures::executor::block_on(put_function_object(
        &FLOCK_S3_BUCKET,
        matches
            .value_of("s3 key")
            .expect("No function s3 key provided"),
        matches
            .value_of("code path")
            .expect("No function code path provided"),
    ))?;
    Ok(())
}

pub fn command_args() -> App<'static, 'static> {
    SubCommand::with_name("upload")
        .about("Uploads a function code to AWS S3")
        .setting(AppSettings::DisableVersion)
        .arg(
            Arg::with_name("code path")
                .short("p")
                .long("path")
                .value_name("FILE")
                .help("Sets the path to the function code")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("s3 key")
                .short("k")
                .long("key")
                .value_name("S3_KEY")
                .help("Sets the S3 key to upload the function code to")
                .takes_value(true),
        )
}

/// Puts a lambda function code to AWS S3.
///
/// # Arguments
/// * `bucket` - The S3 bucket to put the code in.
/// * `key` - The S3 key to put the code in.
/// * `code_path` - The path to the code to put.
pub async fn put_function_object(bucket: &str, key: &str, code_path: &str) -> Result<()> {
    rainbow_println("============================================================");
    rainbow_println("                Upload function code to S3                  ");
    rainbow_println("============================================================");
    rainbow_println("\n\nPackaging code and uploading to S3...");

    if !std::path::Path::new(code_path).exists() {
        bail!("The function code ({}) doesn't exist.", code_path);
    }

    // Package the lambda function code into a zip file.
    let fname = Path::new(code_path).parent().unwrap().join("bootstrap.zip");
    let zip_file = std::fs::File::create(&fname)?;
    let mut zip_writer = zip::ZipWriter::new(zip_file);
    zip_writer.start_file(
        "bootstrap",
        zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Bzip2)
            .unix_permissions(777),
    )?;
    zip_writer.write_all(&fs::read(&code_path)?)?;
    zip_writer.finish()?;

    if !fname.exists() {
        bail!("Failed to rename the binary {} to {:?}!", code_path, fname);
    }

    // Put the zip file to S3.
    let request = PutObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body: Some(fs::read(&fname)?.into()),
        ..Default::default()
    };

    S3Client::new(Region::default()).put_object(request).await?;
    rainbow_println("[OK] Upload Succeed.");

    Ok(())
}
