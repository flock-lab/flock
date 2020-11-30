#!/bin/bash
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



#This script runs Kinesis Data Analytics Flink Benchmarking Utility

export TZ='America/Chicago'
echo 'Running Kinesis Data Generator Application' @ "$(date)"
java -jar /home/ec2-user/kda-flink-benchmarking-utility/amazon-kinesis-data-analytics-flink-benchmarking-utility-0.1.jar \
	/home/ec2-user/kda-flink-benchmarking-utility/benchmarking_specs.json >> /home/ec2-user/kda-flink-benchmarking-utility/logs_new/kdg_log_"$(date '+%Y-%m-%d-%H-%M-%S')".log
