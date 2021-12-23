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

use anyhow::{bail, Result};
use itertools::Itertools;
use log::info;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::time::Duration;
use std::time::SystemTime;
use structopt::StructOpt;

#[derive(Default, Clone, Debug, StructOpt)]
struct CloudWatchLogOpt {
    // Input file path.
    #[structopt(long, short, default_value = "agg_latency/log.txt")]
    path:    String,
    // Output format.
    #[structopt(short = "f", long = "format", default_value = "csv")]
    _format: String,
    // Output file path.
    #[structopt(short = "o", long = "output", default_value = "agg_latency/log.csv")]
    _output: String,
    // Start time.
    #[structopt(short = "s", long = "start", default_value = "")]
    start:   String,
    // End time.
    #[structopt(short = "e", long = "end", default_value = "")]
    end:     String,
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn compute_time(log_lines: &[String]) -> Result<Duration> {
    // The last two lines are the actual computation time
    let compute_lines = log_lines
        .to_owned()
        .split_off(log_lines.len() - 2)
        .iter()
        .map(|line| {
            let t = line.split('\t').next().unwrap();
            t[0..t.rfind('-').unwrap()].to_string()
        })
        .collect::<Vec<String>>();
    assert!(compute_lines.len() == 2);

    let start_time: SystemTime = compute_lines[0]
        .parse::<humantime::Timestamp>()
        .unwrap()
        .into();
    let end_time: SystemTime = compute_lines[1]
        .parse::<humantime::Timestamp>()
        .unwrap()
        .into();
    let difference = end_time.duration_since(start_time)?;

    info!("{:#?}", compute_lines);
    info!("Compute time: {} milliseconds", difference.as_millis());

    Ok(difference)
}

fn invoke_intervals(log_lines: &[String]) -> Result<Vec<Duration>> {
    // remove the first line and last line
    let agg_window_lines = log_lines.to_owned()[1..log_lines.len() - 1].to_vec();
    assert!(agg_window_lines.len() % 2 == 0);

    let mut intervals: Vec<(SystemTime, SystemTime)> = vec![];
    for (prev, next) in agg_window_lines.iter().tuples() {
        let p = prev.split('\t').next().unwrap();
        let n = next.split('\t').next().unwrap();
        intervals.push((
            p[0..p.rfind('-').unwrap()]
                .parse::<humantime::Timestamp>()
                .unwrap()
                .into(),
            n[0..n.rfind('-').unwrap()]
                .parse::<humantime::Timestamp>()
                .unwrap()
                .into(),
        ));
    }

    let durations = intervals
        .iter()
        .map(|(start, end)| Ok(end.duration_since(*start)?))
        .collect::<Result<Vec<Duration>>>();

    info!("Invocation Intervals: {:?}", durations);
    durations
}

fn total_time(log_lines: &[String]) -> Result<Duration> {
    let prev = log_lines.first().unwrap();
    let next = log_lines.last().unwrap();

    let p = prev.split('\t').next().unwrap();
    let n = next.split('\t').next().unwrap();

    let start_time: SystemTime = p[0..p.rfind('-').unwrap()]
        .parse::<humantime::Timestamp>()
        .unwrap()
        .into();
    let end_time: SystemTime = n[0..n.rfind('-').unwrap()]
        .parse::<humantime::Timestamp>()
        .unwrap()
        .into();
    let difference = end_time.duration_since(start_time)?;

    info!("Total time: {} milliseconds", difference.as_millis());
    Ok(difference)
}

fn main() -> Result<()> {
    env_logger::init();

    let opt = CloudWatchLogOpt::from_args();

    if !opt.start.is_empty() && !opt.end.is_empty() {
        info!("Caculating the time interval");
        let start_time: SystemTime = opt.start[0..opt.start.rfind('-').unwrap()]
            .to_string()
            .parse::<humantime::Timestamp>()
            .unwrap()
            .into();
        let end_time: SystemTime = opt.end[0..opt.end.rfind('-').unwrap()]
            .to_string()
            .parse::<humantime::Timestamp>()
            .unwrap()
            .into();
        let difference = end_time.duration_since(start_time)?;
        info!(
            "The time interval is {} milliseconds",
            difference.as_millis()
        );
    } else {
        let input = opt.path;

        if !Path::new(&input).exists() {
            bail!("The log file ({}) doesn't exist.", input);
        }

        // filter lines that contain "START" and "END"
        let mut log_lines = Vec::new();
        if let Ok(lines) = read_lines(input) {
            for line in lines.flatten() {
                if line.contains("START") || line.contains("END") {
                    log_lines.push(line);
                }
            }
        }

        compute_time(&log_lines)?;
        invoke_intervals(&log_lines)?;
        total_time(&log_lines)?;
    }

    Ok(())
}
