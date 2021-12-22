#!/bin/bash
# Copyright (c) 2020-present, UMD Database Group.
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

source scripts/rainbow.sh

############################################################
# Help                                                     #
############################################################
Help() {
  # Display Help
  echo $(echogreen "A Benchmark Script for Flock")
  echo
  echo "Syntax: flock_bench [-g|-h|-c|-r|-a [-b <bench_type>] [-d <data_sink_type>] [-q <query_id>] [-s <number_of_seconds>] [-e <events_per_second>] [-p <number_of_parallel_streams>] [-m <memory_size>]]"
  echo "options:"
  echo "g     Print the GPL license notification."
  echo "h     Print this Help."
  echo "c     Compile and deploy the benchmark."
  echo "r     Run the benchmark. Default: false"
  echo "a     Async invoke the function. Default: sync invoke"
  echo "b     The type of the benchmark [nexmark, ysb]. Default: 'nexmark'"
  echo "d     The type of the data sink [empty: 0, s3: 1, dynamodb: 2, sqs: 3]. Default: 0"
  echo "q     NexMark Query Number [0-9]. Ignored if '-b' is not 'nexmark'. Default: 5"
  echo "p     Number of Data Generators. Default: 1"
  echo "s     Seconds to run the benchmark. Default: 10"
  echo "e     Number of events per second. Default: 1000"
  echo "m     The cloud function's memory size. Default: 128"
  echo
}

############################################################
# License                                                  #
############################################################
AGPLV3() {
  # Display GPL license
  echo "A Benchmark Script for Flock"
  echo
  echo "Copyright (c) 2020-present, UMD Database Group."
  echo
  echo "This program is free software: you can redistribute it and/or modify"
  echo "it under the terms of the GNU Affero General Public License, version 3"
  echo "or later, as published by the Free Software Foundation."
  echo
  echo "This program is distributed in the hope that it will be useful, but"
  echo "WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY"
  echo "or FITNESS FOR A PARTICULAR PURPOSE."
  echo
  echo "You should have received a copy of the GNU Affero General Public License"
  echo "along with this program. If not, see <https://www.gnu.org/licenses/>."
  echo
}

############################################################
# Compile and Deploy Benchmarks                            #
############################################################
Build_and_Deploy() {
  # Compile and Deploy
  echo $(echogreen "============================================================")
  echo $(echogreen "         Compiling and Deploying Benchmarks                 ")
  echo $(echogreen "============================================================")
  echo
  echo $(echogreen "[1] Compiling Flock's Generic Lambda Function...")
  cd src/function
  cargo +nightly build --target x86_64-unknown-linux-gnu --release --features "arrow/simd datafusion/simd mimalloc"
  echo
  echo $(echogreen "[2] Compiling the Benchmark Client ...")
  cd ../../bench
  cargo +nightly build --target x86_64-unknown-linux-gnu --release --features "arrow/simd datafusion/simd mimalloc"
  echo
  echo $(echogreen "[3] Compiling Flock CLI...")
  cd ../src/bin/cli
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  echo
  echo $(echogreen "[4] Deploying Flock's Generic Lambda Function...")
  cd ../../../target/x86_64-unknown-linux-gnu/release
  ./flock-cli upload -p flock -k flock
  cd ../../..
  echo $(echoblue "-------------------------------------------------------------")
  echo
  echo $(echogreen "[OK] Flock Completed Deployment.")
  echo
}

############################################################
# Process the input options. Add options as needed.        #
############################################################
run="false"
async="false"
bench="nexmark"
memory_size="128"
data_sink=0
generators=1
events_per_second=1000
seconds=10
query=5
flock=$(<src/bin/cli/src/flock)
# Get the options
while getopts "hgcraq:b:d:p:s:e:m:" option; do
  case $option in
  h) # display Help
    Help
    exit
    ;;
  g) # display GPL license
    AGPLV3
    exit
    ;;
  c) # compile and deploy the benchmark
    Build_and_Deploy
    exit
    ;;
  r) # run the benchmark
    run="true"
    ;;
  a) # async invoke the function
    async="true"
    ;;
  b) # benchmark type
    bench=$OPTARG
    ;;
  d) # data sink type
    data_sink=$OPTARG
    ;;
  q) # set the query number
    query=$OPTARG
    ;;
  p) # set the number of generators
    generators=$OPTARG
    ;;
  s) # set the number of seconds to run the benchmark
    seconds=$OPTARG
    ;;
  e) # set the number of events per second
    events_per_second=$OPTARG
    ;;
  m) # set the memory size
    memory_size=$OPTARG
    ;;
  \?) # Invalid option
    echo $(echored "Error: Invalid option")
    echo
    Help
    exit
    ;;
  esac
done

# Set the upper limit of the number of seconds to run the benchmark
# to save cloud budget.
if [ $seconds -gt 60 ]; then
  echo $(echopurple "Warning: '-s $seconds' seconds is too long, set to 60 seconds automatically.")
  echo
  seconds=60
fi

if [ $query -gt 9 ]; then
  echo $(echored "Error: Query number must be between 0 and 9.")
  exit
fi

if [ $bench != "nexmark" ] && [ $bench != "ysb" ]; then
  echo $(echored "Error: Benchmark type must be either 'nexmark' or 'ysb'.")
  exit
fi

if [ $data_sink -gt 3 ] || [ $data_sink -lt 0 ]; then
  echo $(echored "Error: Data sink type must be between 0 and 3.")
  exit
fi


if [ $memory_size -gt 10240 ]; then
  echo $(echored "Error: The cloud function's memory size must be less than 10GB.")
  exit
fi

if [ "$run" = "true" ]; then
  echo "$flock"
  echo
  echo $(echogreen "============================================================")
  echo $(echogreen "                  Running the benchmark                     ")
  echo $(echogreen "============================================================")
  echo "Benchmark Type: ${bench^^}"
  echo "Query Number: $query (ignored for YSB)"
  echo "Generators: $generators"
  echo "Events Per Second: $events_per_second"
  echo "Seconds to Run: $seconds"
  echo "Data Sink: $data_sink (ignored for YSB)"
  echo "Async Invoke: $async"
  echo "Function Memory Size: $memory_size"
  echo $(echogreen "============================================================")
  echo
  echo $(echogreen "[OK] Benchmark Starting")
  echo

  if [ $bench = "ysb" ]; then
    benchmark="ysb_bench"
    query_args=""
    sink_args=""
  else
    benchmark="nexmark_bench"
    query_args="-q $query"
    sink_args="-d $data_sink"
  fi

  if [ $async = "true" ]; then
    async_args="--async"
  else
    async_args=""
  fi

  # run the benchmark
  echo $(echogreen "[1] Running the benchmark")
  echo
  RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/$benchmark \
    $query_args $sink_args $async_args -g $generators -s $seconds -m $memory_size --events_per_second $events_per_second --debug
  echo $(echoblue "-------------------------------------------------------------")
  echo
  echo $(echogreen "[OK] Nexmark Benchmark Complete")
elif [ "$run" = "false" ]; then
  echo
  echo $(echored "Error: incomplete command line arguments, please use '-h' for help.")
  echo
fi
