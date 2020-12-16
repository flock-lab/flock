# -*- coding: utf-8 -*-
# Copyright (c) 2020 UMD Database Group. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from boto import kinesis
import time
import json
import argparse

parser = argparse.ArgumentParser(description='Get stream name.')
parser.add_argument("-n", "--name", type=str, required=True)
args = parser.parse_args()
print("listening:", args.name)

kinesis = kinesis.connect_to_region("us-east-1")
shard_id = 'shardId-000000000000'  #we only have one shard!
shard_it = kinesis.get_shard_iterator(args.name, shard_id,
                                      "LATEST")["ShardIterator"]
while 1:
    out = kinesis.get_records(shard_it, limit=1)
    if len(out["Records"]) != 0 :
        rec = json.loads(out["Records"][0]["Data"])
        print(rec)
        
        # if args.name=="joinResults":
        #     if len(rec["results"]) != 0 :
        #         print("Record num:", len(rec["results"]))
        #         print(rec["results"])
        # else:
        #     print(rec)

    shard_it = out["NextShardIterator"]
    time.sleep(0.2)
