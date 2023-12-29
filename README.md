## Flock: A Low-Cost Streaming Query Engine on FaaS Platforms

[![CI](https://github.com/flock-lab/flock/workflows/CI/badge.svg?branch=code&event=pull_request)](https://github.com/flock-lab/flock/actions)
[![codecov](https://codecov.io/gh/flock-lab/flock/branch/master/graph/badge.svg?token=1FOM4DJUZJ)](https://codecov.io/gh/flock-lab/flock)
<a href="https://cla-assistant.io/flock-lab/flock"><img src="https://cla-assistant.io/readme/badge/flock-lab/flock" alt="CLA assistant" /></a>
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)

Flock is a cloud-native streaming query engine that leverages the on-demand elasticity of Function-as-a-Service (FaaS) platforms to perform real-time data analytics. Traditional server-centric deployments often suffer from resource under- or over-provisioning, leading to resource wastage or performance degradation. Flock addresses these issues by providing more fine-grained elasticity that can dynamically match the per-query basis with continuous scaling, and its billing methods are more fine-grained with millisecond granularity, making it a low-cost solution for stream processing. Our approach, payload invocation, eliminates the need for external storage services and eliminates the requirement for a query coordinator in the data architecture. Our evaluation shows that Flock significantly outperforms state-of-the-art systems in terms of cost, especially on ARM processors, making it a promising solution for real-time data analytics on FaaS platforms.



The generic lambda function code is built in advance and uploaded to AWS S3.

| FaaS Service | AWS Lambda | GCP Functions | Azure Functions | Architectures | SIMD | [YSB](https://github.com/yahoo/streaming-benchmarks) | [NEXMark](https://beam.apache.org/documentation/sdks/java/testing/nexmark/) |
| :----------: | :--------: | :-----------: | :-------------: | :-----------: | :--: | :--------------------------------------------------: | :-------------------------------------------------------------------------: |
|  **Flock**   |  🏅🏅🏅🏅  |    👉 TBD     |     👉 TBD      | **Arm**, x86  |  ✅  |                          ✅                          |                                     ✅                                      |

## [Arxiv Paper](https://arxiv.org/abs/2312.16735)

```
@misc{gang2023flock,
      title={Flock: A Low-Cost Streaming Query Engine on FaaS Platforms}, 
      author={Gang Liao and Amol Deshpande and Daniel J. Abadi},
      year={2023},
      eprint={2312.16735},
      archivePrefix={arXiv},
      primaryClass={cs.DB}
}
```

## Build From Source Code

You can enable the features `simd` (to use SIMD instructions) and/or `mimalloc` or `snmalloc` (to use either the mimalloc or snmalloc allocator) as features by passing them in as --features:

To build and deploy Flock to AWS Lambda in one step, you can use the following command:

```ignore
$ ./configure -c -a x86_64
```

<details>
<summary>
<strong>Output</strong>
</summary>

```ignore
============================================================
 Compiling and Deploying Benchmarks
============================================================

Building x86_64-unknown-linux-gnu

[1/3] Compiling Flock Lambda Function...
[2/3] Compiling Flock CLI...
[3/3] Deploying Flock Lambda Function...

============================================================
                Upload function code to S3
============================================================

Packaging code and uploading to S3...
[OK] Upload Succeed.

============================================================
```

</details>
</br>

If you prefer to use the `cargo` command to build and deploy Flock, you can use the following commands:

<details>
<summary>
<strong>Commands</strong>
</summary>

1.  Build Flock for x86_64

    ```ignore
    $ cargo +nightly build --target x86_64-unknown-linux-gnu --release --features "simd mimalloc"
    ```

2.  Deploy Flock binary to AWS S3

    ```ignore
    $ cd ./target/x86_64-unknown-linux-gnu/release
    $ ./flock-cli s3 put --path ./flock --key flock_x86_64
    ```

    </details>
    </br>

## Nexmark Benchmark

### Schemas

These are multiple queries over a three entities model representing on online auction system:

- **Person** represents a person submitting an item for auction and/or making a bid on an auction.
- **Auction** represents an item under auction.
- **Bid** represents a bid for an item under auction.

### Queries

| Query                                                                                              | Name                            | Summary                                                                                                             | Flock |
| -------------------------------------------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ----- |
| [q0](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q0.sql)   | Pass Through                    | Measures the monitoring overhead including the source generator.                                                    | ✅    |
| [q1](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q1.sql)   | Currency Conversion             | Convert each bid value from dollars to euros.                                                                       | ✅    |
| [q2](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q2.sql)   | Selection                       | Find bids with specific auction ids and show their bid price.                                                       | ✅    |
| [q3](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q3.sql)   | Local Item Suggestion           | Who is selling in OR, ID or CA in category 10, and for what auction ids?                                            | ✅    |
| [q4](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q4.sql)   | Average Price for a Category    | Select the average of the wining bid prices for all auctions in each category.                                      | ✅    |
| [q5](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q5.sql)   | Hot Items                       | Which auctions have seen the most bids in the last period?                                                          | ✅    |
| [q6](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q6.sql)   | Average Selling Price by Seller | What is the average selling price per seller for their last 10 closed auctions.                                     | ✅    |
| [q7](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q7.sql)   | Highest Bid                     | Select the bids with the highest bid price in the last period.                                                      | ✅    |
| [q8](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q8.sql)   | Monitor New Users               | Select people who have entered the system and created auctions in the last period.                                  | ✅    |
| [q9](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q9.sql)   | Winning Bids                    | Find the winning bid for each auction.                                                                              | ✅    |
| [q10](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q10.sql) | Log to File System              | Log all events to AWS S3, SQS, and DynamoDB. Illustrates windows streaming data into partitioned file system.       | ✅    |
| [q11](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q11.sql) | User Sessions                   | How many bids did a user make in each session they were active? Illustrates session windows.                        | ✅    |
| [q12](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q12.sql) | Processing Time Windows         | How many bids does a user make within a fixed processing time limit? Illustrates working in processing time window. | ✅    |
| [q13](https://github.com/flock-lab/flock/blob/master/flock/src/datasource/nexmark/queries/q13.sql) | Bounded Side Input Join         | Joins a stream to a bounded side input, modeling basic stream enrichment.                                           | ✅    |

> Note: q1 ~ q8 are from original NEXMark queries, q0 and q9 ~ q13 are from Apache Beam.

### Run the benchmark

`flock-cli` is a command line interface to Flock. It can be used to run queries or benchmarks against Flock. For example, to run the query 5 on x86_64 for 10 seconds with 1,000 events per second, and 1 generator, you can run:

```ignore
$ ./flock-cli nexmark run -q 5 -s 10 -e 1000 -g 1 --arch x86_64
```

<details>
<summary>
<strong>Client Output</strong>
</summary>

``````bash
 /`
/:y/` `
`shdhso.
 -yhddh+.
  .yhhhy+-
   .syyhs+/.
    `+shhs++:.
     `:syyyo++/.
       .+ssys+++/-`          `.----.`
        ./oyyyo+++/:.`     `-/+++/-..`
          -/osyso++++/:.` -/++/-`
           .-/osssoo++++/:++++`
           `.-/++osooo++++++++-
              `-:/+oooo++++++o/
                `-:/+o++++++oo-                                `````             `
                 `.-//++++++o/   `:++:::://   .:++:`        .:///////.       .://///+-   ./++:` .++/.
                 ``..:+++++o+`     os`   -+     ss        `/+-`//. `-+/`   `+s:`   `o:    `so  `:+-
                     :+++++/`      os`  --      ss        /o`  `+o`  `++   +s:      ``    `so .+:`
                   `:+++++:        os:::o/      ss        o/   /+++`  :s   ss.            `ss/so`
                 .:++++:.`         os`  --      ss     `  /o``+/``o/:`++   +s:      `     `so .oo.
             `.:/++++/.            os`          ss    :+   /+:-`  .-:+/`   `+s:`    o/    `so  `+s:
          .-----:/++-            `:++:-       .:++::::+/    .:++//++:.       ./++///+-   .:o+:`  :o/:
          `.-:::-/:`                                            ``
        `--.``-/:`
            .:-`


Flock: A Practical Serverless Streaming SQL Query Engine (https://github.com/flock-lab/flock)

Copyright (c) 2020-present, UMD Data System Group.

▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒

This program is free software: you can use, redistribute, and/or modify it under the terms of the GNU Affero
General Public License, version 3 or later ("AGPL"), as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see
<http://www.gnu.org/licenses/>.

▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒

================================================================
                    Running the benchmark
================================================================
[info] Running the NEXMark benchmark with the following options:

NexmarkBenchmarkOpt {
    query_number: 5,
    generators: 1,
    seconds: 10,
    events_per_second: 1000,
    data_sink_type: "blackhole",
    async_type: false,
    memory_size: 128,
    architecture: "x86_64",
}

[info] Creating lambda function: flock_datasource
[info] Creating lambda function group: Group(("q5-00", 8))
[info] Creating function member: q5-00-00
[info] Creating function member: q5-00-01
[info] Creating function member: q5-00-02
[info] Creating function member: q5-00-03
[info] Creating function member: q5-00-04
[info] Creating function member: q5-00-05
[info] Creating function member: q5-00-06
[info] Creating function member: q5-00-07
[info] [OK] Invoking NEXMark source function: flock_datasource by generator 0
[info] [OK] Received status from function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
[info] Waiting for the current invocations to be logged.
[2021-12-16T19:09:35Z INFO  driver::logwatch::tail] Sending log request FilterLogEventsRequest { end_time: None, filter_pattern: None, limit: Some(100), log_group_name: "/aws/lambda/flock_datasource", log_stream_name_prefix: None, log_stream_names: None, next_token: None, start_time: Some(1639681715344) }
[2021-12-16T19:09:38Z INFO  driver::logwatch::tail] [OK] Got response from AWS CloudWatch Logs.
2021-12-16 14:09:18 START RequestId: fd8ae2ad-b64a-4e02-88c8-c43f00974022 Version: $LATEST
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  runtime::datasource::nexmark::nexmark] Generating events for 10s over 1 partitions.
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  flock::nexmark::source] Nexmark Benchmark: Query 5
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  flock::nexmark::source] NEXMarkSource { config: Config { args: {"threads": "1", "events-per-second": "1000", "seconds": "10"} }, window: Hopping((10, 5)) }
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  flock::nexmark::source] [OK] Generate nexmark events.
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  runtime::datasource::nexmark::nexmark] Epoch 0: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  runtime::datasource::nexmark::nexmark] Epoch 1: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  runtime::datasource::nexmark::nexmark] Epoch 2: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  runtime::datasource::nexmark::nexmark] Epoch 3: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  runtime::datasource::nexmark::nexmark] Epoch 4: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:18 [2021-12-16T19:09:18Z INFO  runtime::datasource::nexmark::nexmark] Epoch 5: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  runtime::datasource::nexmark::nexmark] Epoch 6: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  runtime::datasource::nexmark::nexmark] Epoch 7: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  runtime::datasource::nexmark::nexmark] Epoch 8: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  runtime::datasource::nexmark::nexmark] Epoch 9: 20 persons, 60 auctions, 920 bids.
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Send 10 NexMark events from a window (epoch: 0-10) to function: q5-00-04.
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 0 - function payload bytes: 21078
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 1 - function payload bytes: 22419
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 2 - function payload bytes: 21598
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 3 - function payload bytes: 21997
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 4 - function payload bytes: 22364
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 5 - function payload bytes: 22185
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 6 - function payload bytes: 21767
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 7 - function payload bytes: 21436
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 8 - function payload bytes: 21806
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Event 9 - function payload bytes: 21829
2021-12-16 14:09:19 [2021-12-16T19:09:19Z INFO  flock::window] [OK] Received status from async lambda function. InvocationResponse { executed_version: None, function_error: None, log_result: None, payload: Some(b""), status_code: Some(202) }
2021-12-16 14:09:19 END RequestId: fd8ae2ad-b64a-4e02-88c8-c43f00974022
2021-12-16 14:09:19 REPORT RequestId: fd8ae2ad-b64a-4e02-88c8-c43f00974022      Duration: 1177.85 ms    Billed Duration: 1189 ms        Memory Size: 128 MB     Max Memory Used: 22 MB  Init Duration: 10.88 ms
[2021-12-16T19:09:38Z INFO  driver::deploy::common] Got a Token response
-------------------------------------------------------------

[OK] Nexmark Benchmark Complete
``````

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

## Advanced Usage

`flock-cli` has a number of advanced features that can be used to control and customize the behavior of Flock.

For example, to delete all functions, you can use the `flock-cli lambda -D` command. Or use the `flock-cli lambda -d <function pattern>` command to delete specific functions. To list all functions, use the `flock-cli lambda -L` command.

To see the help for the `nexmark run` command, issue the command: `flock-cli nexmark run -h`

```ignore
Runs the NEXMark Benchmark

USAGE:
    flock-cli nexmark run [OPTIONS]

OPTIONS:
    -a, --async-type
            Runs the NEXMark benchmark with async function invocations

    -e, --events-per-second <events per second>
            Runs the NEXMark benchmark with a number of events per second [default: 1000]

    -g, --generators <data generators>
            Runs the NEXMark benchmark with a number of data generators [default: 1]

    -h, --help
            Print help information

        --log-level <log-level>
            Log level [default: info] [possible values: error, warn, info, debug, trace, off]

    -m, --memory-size <memory size>
            Sets the memory size (MB) for the worker function [default: 128]

    -q, --query <query number>
            Sets the NEXMark benchmark query number [default: 3] [possible values: 0, 1, 2, 3, 4, 5,
            6, 7, 8, 9, 10, 11, 12, 13]

    -r, --arch <architecture>
            Sets the architecture for the worker function [default: x86_64] [possible values:
            x86_64, arm64]

    -s, --seconds <duration>
            Runs the NEXMark benchmark for a number of seconds [default: 20]

        --silent
            Suppress all output

    -t, --data-sink-type <data sink type>
            Runs the NEXMark benchmark with a data sink type [default: blackhole] [possible values:
            sqs, s3, dynamodb, efs, blackhole]

        --trace
            Log ultra-verbose (trace level) information
```

## License

Copyright (c) 2020-present UMD Database Group.
The library, examples, and all source code are released under [AGPL-3.0 License](LICENSE).
