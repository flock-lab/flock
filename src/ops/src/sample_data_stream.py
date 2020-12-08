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

import json
import boto3
import random

kinesis = boto3.client('kinesis')


def getStream1Data():
    data = {}
    data['attr_1'] = random.randint(90, 92)
    data['attr_2'] = random.randint(90, 120)
    data['attr_3'] = random.randint(90, 120)
    data['attr_4'] = random.randint(90, 120)
    return data


def getStream2Data():
    data = {}
    data['attr_1'] = random.randint(90, 92)
    data['attr_5'] = random.randint(90, 120)
    data['attr_6'] = random.randint(90, 120)
    data['attr_7'] = random.randint(90, 120)

    return data


# while True:
stream1 = 0
stream2 = 0
for i in range(50):
    rnd = random.random()
    if (rnd < 0.5):
        data = json.dumps(getStream1Data())
        print(data)
        kinesis.put_record(StreamName="stream1",
                           Data=data,
                           PartitionKey="partitionkey")
        stream1 += 1
    else:
        data = json.dumps(getStream2Data())
        print(data)
        kinesis.put_record(StreamName="stream2",
                           Data=data,
                           PartitionKey="partitionkey")
        stream2 += 1

# for i in range(10):
#     rnd = random.random()
#     data = json.dumps(getStream1Data())
#     print(data)
#     kinesis.put_record(
#         StreamName="stream1",
#         Data=data,
#         PartitionKey="partitionkey")
#     stream1 += 1
# print(stream1, stream2)

# for i in range(10):
#     rnd = random.random()
#     data = json.dumps(getStream1Data())
#     print(data)
#     kinesis.put_record(
#         StreamName="stream1",
#         Data=data,
#         PartitionKey="partitionkey")
#     stream1 += 1

print(stream1, stream2)
