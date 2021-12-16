<img src="docs/img/flock.png" width=60%>

## Flock: A Practical Serverless Streaming SQL Query Engine

[![CI](https://github.com/flock-lab/flock/workflows/CI/badge.svg?branch=code&event=pull_request)](https://github.com/flock-lab/flock/actions)
[![codecov](https://codecov.io/gh/flock-lab/flock/branch/master/graph/badge.svg?token=1FOM4DJUZJ)](https://codecov.io/gh/flock-lab/flock)
<a href="https://cla-assistant.io/flock-lab/flock"><img src="https://cla-assistant.io/readme/badge/flock-lab/flock" alt="CLA assistant" /></a>
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)


The generic lambda function code is built in advance and uploaded to AWS S3.

| Service  | Cloud Platform | S3 Bucket      | S3 Key           |    S3 URL                        |   [YSB Bench](https://github.com/yahoo/streaming-benchmarks)    |   [NEXMark Bench](https://beam.apache.org/documentation/sdks/java/testing/nexmark/) |
| :---: | :---------------: | :----------------: | :--------------------------------: | :------------------: | :-------------------: | :-------------------: |
| **Flock** | AWS Lambda | flock-lab   |   flock        |  s3://flock-lab/flock     | ✅ | ✅ |


## Build From Source Code

You can enable the features `simd` (to use SIMD instructions) and/or `mimalloc` or `snmalloc` (to use either the mimalloc or snmalloc allocator) as features by passing them in as --features:

```shell
$ cargo +nightly build --target x86_64-unknown-linux-gnu --release \
        --features "arrow/simd datafusion/simd snmalloc"
```

## Upgrade Cloud Function Services

flock-cli is an interactive command-line query tool for Flock.

```shell
$ cd target/x86_64-unknown-linux-gnu/release/
$ ./flock-cli --help
```

<details>
<summary>
<strong>Output</strong>
</summary>

```shell
Flock 0.2.0
UMD Database Group
Command Line Interactive Contoller for Flock

USAGE:
    flock-cli [OPTIONS] [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --config <FILE>    Sets a custom config file

SUBCOMMANDS:
    fsql       The terminal-based front-end to Flock
    help       Prints this message or the help of the given subcommand(s)
    nexmark    The NEXMark Benchmark Tool
    upload     Uploads a function code to AWS S3
```
</details>
</br>


## Nexmark Benchmark

All the following Nexmark queries share the same lambda function code.

| Query    | Name     | Summary  | Flock |
| -------- | -------- | -------- | -------- |
| q0 | Pass Through | Measures the monitoring overhead including the source generator. | ✅ |
| q1 | Currency Conversion | Convert each bid value from dollars to euros. | ✅ |
| q2 | Selection | Find bids with specific auction ids and show their bid price. | ✅ |
| q3 | Local Item Suggestion | Who is selling in OR, ID or CA in category 10, and for what auction ids?  | ✅ |
| q4 | Average Price for a Category | Select the average of the wining bid prices for all auctions in each category. | ✅ |
| q5 | Hot Items | Which auctions have seen the most bids in the last period? | ✅ |
| q6 | Average Selling Price by Seller | What is the average selling price per seller for their last 10 closed auctions. | ✅ |
| q7 | Highest Bid | Select the bids with the highest bid price in the last period. | ✅ |
| q8 | Monitor New Users | Select people who have entered the system and created auctions in the last period. | ✅ |
| q9 | Winning Bids | Find the winning bid for each auction. | ✅ |
| q13 | Bounded Side Input Join | Joins a stream to a bounded side input, modeling basic stream enrichment. | ✅ |

We provide a script (`nexmark.sh`) to build, deploy and run the benchmark.

```shell
$ ./nexmark.sh -help

Nexmark Benchmark Script for Flock

Syntax: nexmark [-g|-h|-c|-r [-q <query_id>] [-s <number_of_seconds>] [-e <events_per_second>] [-p <number_of_parallel_streams>]]
options:
g     Print the GPL license notification.
h     Print this Help.
c     Compile and deploy the benchmark.
r     Run the benchmark.
q     NexMark Query Number [0-9]. Default: 5
p     Number of NexMark Generators. Default: 1
s     Seconds to run the benchmark. Default: 10
e     Number of events per second. Default: 1000
```

To build and deploy the benchmark, run:

```shell
$ ./nexmark.sh -c
```

For example, to run the query 5 for 10 seconds with 1,000 events per second, and 1 generator, you can run:

```shell
$ ./nexmark.sh -r -q 5 -s 10 -e 1000 -p 1
```

<details>
<summary>
<strong>Client Output</strong>
</summary>

```bash
============================================================
 Running the benchmark
============================================================
Nexmark Query Number: 5
Nexmark Generators: 1
Nexmark Events Per Second: 1000
Nexmark Seconds to Run: 10
============================================================

[OK] Nexmark Benchmark Starting

[1] Warming up the lambda functions

[2021-12-15T15:20:48Z INFO  nexmark_bench] Running benchmarks with the following options: NexmarkBenchmarkOpt { query_number: 5, debug: true, generators: 1, seconds: 10, events_per_second: 1000 }
[2021-12-15T15:20:48Z INFO  nexmark_bench] Creating lambda function: nexmark_datasource
[2021-12-15T15:20:49Z INFO  nexmark_bench] Creating lambda function group: Group(("q5-00", 8))
[2021-12-15T15:20:49Z INFO  nexmark_bench] Creating function member: q5-00-00
[2021-12-15T15:20:49Z INFO  nexmark_bench] Creating function member: q5-00-01
[2021-12-15T15:20:50Z INFO  nexmark_bench] Creating function member: q5-00-02
[2021-12-15T15:20:50Z INFO  nexmark_bench] Creating function member: q5-00-03
[2021-12-15T15:20:51Z INFO  nexmark_bench] Creating function member: q5-00-04
[2021-12-15T15:20:51Z INFO  nexmark_bench] Creating function member: q5-00-05
[2021-12-15T15:20:52Z INFO  nexmark_bench] Creating function member: q5-00-06
[2021-12-15T15:20:52Z INFO  nexmark_bench] Creating function member: q5-00-07
[2021-12-15T15:20:52Z INFO  nexmark_bench] [OK] Invoke function: nexmark_datasource 0
[2021-12-15T15:20:52Z INFO  nexmark_bench] [OK] Received status from function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
[2021-12-15T15:20:52Z INFO  driver::logwatch::tail] Sending log request FilterLogEventsRequest { end_time: None, filter_pattern: None, limit: Some(100), log_group_name: "/aws/lambda/nexmark_datasource", log_stream_name_prefix: None, log_stream_names: None, next_token: None, start_time: Some(1639581352971) }
[2021-12-15T15:20:55Z INFO  driver::logwatch::tail] [OK] Got response from AWS CloudWatch Logs.
[2021-12-15T15:20:55Z INFO  nexmark_bench] Got a lastlog response
[2021-12-15T15:20:55Z INFO  nexmark_bench] Waiting 5s before requesting logs again...
[2021-12-15T15:21:00Z INFO  driver::logwatch::tail] Sending log request FilterLogEventsRequest { end_time: None, filter_pattern: None, limit: Some(100), log_group_name: "/aws/lambda/nexmark_datasource", log_stream_name_prefix: None, log_stream_names: None, next_token: None, start_time: Some(1639581352971) }
[2021-12-15T15:21:06Z INFO  driver::logwatch::tail] [OK] Got response from AWS CloudWatch Logs.
2021-12-15 10:20:54 START RequestId: f65ef2a9-1fec-4e19-8793-9e10c5966506 Version: $LATEST
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  runtime::datasource::nexmark::nexmark] Generating events for 10s over 1 partitions.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::source] Nexmark Benchmark: Query 5
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::source] NEXMarkSource { config: Config { args: {"threads": "1", "seconds": "10", "events-per-second": "1000"} }, window: HoppingWindow((10, 5)) }
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::source] [OK] Generate nexmark events.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 0: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 1: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 2: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 3: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 4: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 5: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 6: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 7: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 8: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::utils] Epoch 9: 20 persons, 60 auctions, 920 bids.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::window] [OK] Send 10 NexMark events from a window (epoch: 0-10) to function: q5-00-04.
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::window] [OK] Event 0 - function payload bytes: 21068
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::window] [OK] Event 1 - function payload bytes: 22409
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:54 [2021-12-15T15:20:54Z INFO  nexmark_lambda::window] [OK] Event 2 - function payload bytes: 21588
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Event 3 - function payload bytes: 21987
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Event 4 - function payload bytes: 22354
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Event 5 - function payload bytes: 22175
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Event 6 - function payload bytes: 21757
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Event 7 - function payload bytes: 21426
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Event 8 - function payload bytes: 21796
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Event 9 - function payload bytes: 21819
2021-12-15 10:20:55 [2021-12-15T15:20:55Z INFO  nexmark_lambda::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-15 10:20:55 END RequestId: f65ef2a9-1fec-4e19-8793-9e10c5966506
2021-12-15 10:20:55 REPORT RequestId: f65ef2a9-1fec-4e19-8793-9e10c5966506      Duration: 1231.79 ms    Billed Duration: 1245 ms        Memory Size: 128 MB     Max Memory Used: 23 MB  Init Duration: 12.72 ms
[2021-12-15T15:21:06Z INFO  nexmark_bench] Got a Token response
```
</details>
</br>




<details>
<summary>
<strong>Function Output</strong>
</summary>

```bash
TART RequestId: 78a68707-3f3d-4244-a51a-584f9432709d Version: $LATEST
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 0, seq_len: 10 }
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: 78a68707-3f3d-4244-a51a-584f9432709d
REPORT RequestId: 78a68707-3f3d-4244-a51a-584f9432709d	Duration: 38.83 ms	Billed Duration: 66 ms	Memory Size: 128 MB	Max Memory Used: 17 MB	Init Duration: 26.32 ms
START RequestId: 23dae113-ccf3-449f-944f-116bb925daaf Version: $LATEST
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 5, seq_len: 10 }
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: 23dae113-ccf3-449f-944f-116bb925daaf
REPORT RequestId: 23dae113-ccf3-449f-944f-116bb925daaf	Duration: 1.71 ms	Billed Duration: 2 ms	Memory Size: 128 MB	Max Memory Used: 17 MB
START RequestId: e5e51594-5819-494c-a6d3-c9c9ed9ab865 Version: $LATEST
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 6, seq_len: 10 }
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: e5e51594-5819-494c-a6d3-c9c9ed9ab865
REPORT RequestId: e5e51594-5819-494c-a6d3-c9c9ed9ab865	Duration: 1.30 ms	Billed Duration: 2 ms	Memory Size: 128 MB	Max Memory Used: 18 MB
START RequestId: def2fc0b-61da-49f8-80b4-9e49f5f4a091 Version: $LATEST
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 7, seq_len: 10 }
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: def2fc0b-61da-49f8-80b4-9e49f5f4a091
REPORT RequestId: def2fc0b-61da-49f8-80b4-9e49f5f4a091	Duration: 6.89 ms	Billed Duration: 7 ms	Memory Size: 128 MB	Max Memory Used: 18 MB
START RequestId: a18c2e75-d1a4-4595-aa84-4cde90eecad4 Version: $LATEST
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 8, seq_len: 10 }
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: a18c2e75-d1a4-4595-aa84-4cde90eecad4
REPORT RequestId: a18c2e75-d1a4-4595-aa84-4cde90eecad4	Duration: 1.16 ms	Billed Duration: 2 ms	Memory Size: 128 MB	Max Memory Used: 18 MB
START RequestId: 01168950-7558-4af6-9e8c-8f71c4542149 Version: $LATEST
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 9, seq_len: 10 }
[2021-12-15T15:20:56Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: 01168950-7558-4af6-9e8c-8f71c4542149
REPORT RequestId: 01168950-7558-4af6-9e8c-8f71c4542149	Duration: 8.22 ms	Billed Duration: 9 ms	Memory Size: 128 MB	Max Memory Used: 18 MB
START RequestId: c28193bb-b818-4cf4-863c-2e9d34dd2398 Version: $LATEST
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 1, seq_len: 10 }
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: c28193bb-b818-4cf4-863c-2e9d34dd2398
REPORT RequestId: c28193bb-b818-4cf4-863c-2e9d34dd2398	Duration: 1.18 ms	Billed Duration: 2 ms	Memory Size: 128 MB	Max Memory Used: 18 MB
START RequestId: 46e54b2e-91da-4609-9f54-f152d38681c7 Version: $LATEST
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 2, seq_len: 10 }
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: 46e54b2e-91da-4609-9f54-f152d38681c7
REPORT RequestId: 46e54b2e-91da-4609-9f54-f152d38681c7	Duration: 1.15 ms	Billed Duration: 2 ms	Memory Size: 128 MB	Max Memory Used: 18 MB
START RequestId: 2b1c1fe0-9556-4849-8251-39ede796f0f0 Version: $LATEST
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 3, seq_len: 10 }
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Window data collection has not been completed.
END RequestId: 2b1c1fe0-9556-4849-8251-39ede796f0f0
REPORT RequestId: 2b1c1fe0-9556-4849-8251-39ede796f0f0	Duration: 1.08 ms	Billed Duration: 2 ms	Memory Size: 128 MB	Max Memory Used: 18 MB
START RequestId: 78c64a1a-b312-4099-b596-541c078b04b7 Version: $LATEST
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Receiving a data packet: Uuid { tid: "q5-1639581654", seq_num: 4, seq_len: 10 }
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor] Received all data packets for the window: "q5-1639581654"
[2021-12-15T15:20:58Z INFO  nexmark_lambda::actor]
+---------+-----+
| auction | num |
+---------+-----+
| 1500    | 841 |
+---------+-----+
```
</details>
</br>



## License

Copyright (c) 2020-present UMD Database Group.
The library, examples, and all source code are released under [AGPL-3.0 License](LICENSE).
