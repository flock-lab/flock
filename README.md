<img src="docs/squirtle.png" width=20%>

## Squirtle: Serverless SQL Query Engine for Real-Time and Batch Analytics

[![CI](https://github.com/DSLAM-UMD/Squirtle/workflows/CI/badge.svg?branch=code&event=pull_request)](https://github.com/DSLAM-UMD/Squirtle/actions)
[![codecov](https://codecov.io/gh/DSLAM-UMD/Squirtle/branch/master/graph/badge.svg?token=1FOM4DJUZJ)](https://codecov.io/gh/DSLAM-UMD/Squirtle)
<a href="https://cla-assistant.io/DSLAM-UMD/Squirtle"><img src="https://cla-assistant.io/readme/badge/DSLAM-UMD/Squirtle" alt="CLA assistant" /></a>
[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)


The generic lambda function code is built in advance and uploaded to AWS S3.

| Lambda Function Code    | S3 Bucket      | S3 Key           |    S3 URL                        |
| ----------------------- | ---------------| ---------------- | -------------------------------- |
| [Nexmark Benchmark](https://beam.apache.org/documentation/sdks/java/testing/nexmark/)      | umd-squirtle   |   nexmark        |  https://umd-squirtle.s3.amazonaws.com/nexmark      |
| [TPC-H](http://tpc.org/tpch/)                    | umd-squirtle  |   tpch           |  `coming soon`


## Build From Source Code

You can enable the features `simd` (to use SIMD instructions) and/or `mimalloc` or `snmalloc` (to use either the mimalloc or snmalloc allocator) as features by passing them in as --features:

```shell
$ cargo build --target x86_64-unknown-linux-gnu --release --features "arrow/simd datafusion/simd  mimalloc"
```

## Upgrade Cloud Function Services

squirtle-cli is an interactive command-line query tool for Squirtle.

```shell
$ cd target/x86_64-unknown-linux-gnu/release/
$ ./squirtle-cli --help
```

<details>
<summary>
<strong>Output</strong>
</summary>

```shell
Squirtle 0.1.0
Command Line Interactive Contoller for Squirtle

USAGE:
    squirtle-cli [FLAGS] [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -v               Sets the level of verbosity
    -V, --version    Prints version information

OPTIONS:
    -u, --upload <FILE>    Upload lambda execution code to S3.
    -k, --key <STRING>     AWS S3 key for this function code.
```
</details>
</br>

For example, you can use `squirtle-cli` in response to the uploading, updating, or deleting of the cloud functions in AWS S3.

```shell
# for example, upgrade the NexMark Benchmark for streaming processing
./squirtle-cli -u nexmark_lambda -k nexmark
```

<details>
<summary>
<strong>Output</strong>
</summary>

```bash

                                      ```
                                `-/shdddddhyo/-`
                              `/hdmmmmmmmmmmmmdh+.
                             :ymmmmmmmmmmmmmmmmmmy-
                            /symmmmmmmmmmds/smmmhys-
                            +s`mmmmmmmmmmym` :mmyyyo
                            :  hmmmmmmmmd   .:hmyyy+s/
                           `+//mmmmmmmmmm+::/sdyyyy+ds+:.
                           -ydmmhmmdmmmmmmmmmdyhosooNsooo/`
                        /syyyyosyhhyhhhhhhhhyhdyhmdsdm+/+++-
              ``....``  :yoymmmmhysyhhhhyyss+ydddmmmdym++oo+:
          -+yhdmmmmmmhyosyhmmmmmmmyydhhhhhhhhshdhhdmmmsMoooo+.
       .odmmy+:-.``.-/ohmdsodmmmmmyyhhhysssyhhsoyyyyhhoMN/oo+/
     `odmh/``:///`      .ommsohddhsNNNNdyNNNNNNdysoooosMh+ooo+
    `hmy-   .mmmms`       `ymy.`-osmNNNNsNNNNNNNNyhdhomMoooooo   ./osso:.
    ymo`       +mmo`        sms` -hyhNNNhdNNNNNNydNNNymMooooo/ -ydmmmmmmd+`
   -ho`        :mmmo`       `mm-  :hhyhhhohhhhhyyNNNNNsmNo+o:o+dmmmmmmmmmms`
   -+         /mmmmm+`       hm+   -omNNNmsmNNNNmhhmdhy+hmo-:smmmmmhhyydmmm/
    :.       ommy-ymm/       dm/   .syydNNNydNNNNNhydmmhoym/+hmmmmydmmmommy/
    -:     `smms` `hmm:.:`  /mh.  -ymmmdyyhhsoyhdyhmmmmmysosyyhmmmodmmmddyo`
     ::`  `ymm+`   `dmmmmo`:md:   :dmmmmmhoo+oyys/mmmmmmyy+yyyyhdmyoyhhys/`
      :+-`/++:      .s+:``+mh-    `smmmmmmdyo.--:+mmmmmmyy/://++++//----
       .oo:``         `-ohh+`      /hmmmmmmms`   :hmmmmhys:
         `/oso+:::/+oyhho:`        -+yhyso+/-    :hdmmhyys:
             .-:/++/:-`                          `--+s:---`



       ███████╗ ██████╗ ██╗   ██╗██╗██████╗ ████████╗██╗     ███████╗
       ██╔════╝██╔═══██╗██║   ██║██║██╔══██╗╚══██╔══╝██║     ██╔════╝
       ███████╗██║   ██║██║   ██║██║██████╔╝   ██║   ██║     █████╗
       ╚════██║██║▄▄ ██║██║   ██║██║██╔══██╗   ██║   ██║     ██╔══╝
       ███████║╚██████╔╝╚██████╔╝██║██║  ██║   ██║   ███████╗███████╗
       ╚══════╝ ╚══▀▀═╝  ╚═════╝ ╚═╝╚═╝  ╚═╝   ╚═╝   ╚══════╝╚══════╝


Squirtle: Serverless SQL Query Engine for Real-Time and Batch Analytics (https://github.com/DSLAM-UMD/Squirtle)

Copyright (c) 2020-2021, UMD Data System Group. All rights reserved.

▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒

============================================
         Upload function code to S3  
============================================


[UPLOAD] ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒... ... ... ...
[OK] Upload Succeed.
```

</details>
</br>

## Nexmark Benchmark

All the following Nexmark queries share the same lambda function code.

| Query    | Name     | Summary  | Squirtle |
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

## License

Copyright 2020 UMD Database Group. All Rights Reserved.
The library, examples, and all source code are released under [Apache License 2.0](LICENSE).
