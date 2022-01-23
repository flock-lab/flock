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

use anyhow::{anyhow, bail, Context as _, Ok, Result};
use benchmarks::rainbow_println;
use clap::{App, AppSettings, Arg, ArgMatches};
use flock::aws::s3;
use ini::Ini;
use lazy_static::lazy_static;
use log::warn;
use rusoto_core::Region;
use rusoto_s3::PutObjectRequest;
use rusoto_s3::{S3Client, S3};
use std::fs;
use std::io::Write;
use std::path::Path;

lazy_static! {
    /// Global settings.
    pub static ref FLOCK_CONF: Ini = Ini::load_from_str(include_str!("../../flock/src/configs/flock.toml")).unwrap();
    pub static ref FLOCK_S3_BUCKET: String = FLOCK_CONF["s3"]["bucket"].to_string();
}

pub fn command(matches: &ArgMatches) -> Result<()> {
    let (command, matches) = match matches.subcommand() {
        Some((command, matches)) => (command, matches),
        None => unreachable!(),
    };

    match command {
        "put" => futures::executor::block_on(put_function_object(matches)),
        "list" => futures::executor::block_on(list_buckets(matches)),
        "delete" => futures::executor::block_on(delete_buckets(matches)),
        _ => {
            warn!("{} command is not implemented", command);
            Ok(())
        }
    }
    .with_context(|| anyhow!("{} command failed", command))?;

    Ok(())
}

pub fn command_args() -> App<'static> {
    App::new("s3")
        .about("The AWS S3 Tool for Flock")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(put_args())
        .subcommand(list_args())
        .subcommand(delete_args())
}

fn delete_args() -> App<'static> {
    App::new("delete").about("Deletes AWS S3 Buckets").arg(
        Arg::new("delete buckets with substring pattern")
            .short('p')
            .long("pattern")
            .help("Sets the pattern to delete buckets with")
            .takes_value(true)
            .default_value("flock-"),
    )
}

fn list_args() -> App<'static> {
    App::new("list").about("List AWS S3 Buckets").arg(
        Arg::new("list buckets with substring pattern")
            .short('p')
            .long("pattern")
            .help("Sets the pattern to list buckets with")
            .takes_value(true)
            .default_value("flock-"),
    )
}

fn put_args() -> App<'static> {
    App::new("put")
        .about("Puts a function code to AWS S3")
        .arg(
            Arg::new("code path")
                .short('p')
                .long("path")
                .value_name("FILE")
                .help("Sets the path to the function code")
                .takes_value(true),
        )
        .arg(
            Arg::new("s3 key")
                .short('k')
                .long("key")
                .value_name("S3_KEY")
                .help("Sets the S3 key to upload the function code to")
                .takes_value(true),
        )
}

/// Puts a lambda function code to AWS S3.
pub async fn put_function_object(matches: &ArgMatches) -> Result<()> {
    let bucket = &FLOCK_S3_BUCKET;
    let key = matches
        .value_of("s3 key")
        .expect("No function's s3 key provided");
    let code_path = matches
        .value_of("code path")
        .expect("No function's code path provided");

    if !std::path::Path::new(code_path).exists() {
        bail!("The function code ({}) doesn't exist.", code_path);
    }

    rainbow_println("============================================================");
    rainbow_println("                Upload function code to S3                  ");
    rainbow_println("============================================================");
    rainbow_println("\n\nPackaging code and uploading to S3...");

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

/// Lists S3 buckets.
async fn list_buckets(matches: &ArgMatches) -> Result<()> {
    if matches.is_present("list buckets with substring pattern") {
        if s3::get_matched_buckets(
            matches
                .value_of("list buckets with substring pattern")
                .as_ref()
                .unwrap(),
        )
        .await?
        .into_iter()
        .map(|bucket| {
            rainbow_println(bucket);
        })
        .count()
            == 0
        {
            rainbow_println("No matched buckets found.");
        }
    } else if s3::list_buckets()
        .await?
        .into_iter()
        .map(|bucket| {
            rainbow_println(bucket);
        })
        .count()
        == 0
    {
        rainbow_println("No bucket found.");
    }

    Ok(())
}

/// Deletes S3 buckets.
async fn delete_buckets(matches: &ArgMatches) -> Result<()> {
    if matches.is_present("delete buckets with substring pattern") {
        let prefix = matches
            .value_of("delete buckets with substring pattern")
            .expect("No prefix provided");
        let buckets = s3::get_matched_buckets(prefix).await?;
        if buckets.is_empty() {
            rainbow_println("No matched buckets found.");
        } else {
            let mut count = 0;
            for bucket in buckets {
                s3::delete_bucket(&bucket).await?;
                count += 1;
            }
            rainbow_println(format!("Deleted {} buckets.", count));
        }
    } else {
        let buckets = s3::list_buckets().await?;
        if buckets.is_empty() {
            rainbow_println("No buckets found.");
        } else {
            let mut count = 0;
            for bucket in buckets {
                s3::delete_bucket(&bucket).await?;
                count += 1;
            }
            rainbow_println(format!("Deleted {} buckets.", count));
        }
    }

    Ok(())
}
