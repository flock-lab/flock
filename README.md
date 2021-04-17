<img src="docs/squirtle.png" width=20%>

## Squirtle: Serverless SQL Query Engine for Real-Time and Batch Analytics

[![CI](https://github.com/DSLAM-UMD/Squirtle/workflows/CI/badge.svg?branch=code&event=pull_request)](https://github.com/DSLAM-UMD/Squirtle/actions)
[![codecov](https://codecov.io/gh/DSLAM-UMD/Squirtle/branch/master/graph/badge.svg?token=1FOM4DJUZJ)](https://codecov.io/gh/DSLAM-UMD/Squirtle)
[![DOI](https://zenodo.org/badge/295168255.svg)](https://zenodo.org/badge/latestdoi/295168255)
<a href="https://cla-assistant.io/DSLAM-UMD/Squirtle"><img src="https://cla-assistant.io/readme/badge/DSLAM-UMD/Squirtle" alt="CLA assistant" /></a>
[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

## Nexmark Benchmark

The generic lambda function code is built in advance and uploaded to AWS S3.

| Lambda Function Code    | S3 Bucket  | S3 Key |
| ----------------------- | ---------- | ------ |
| Nexmark Benchamark      |    ***     |   ***  |  


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
