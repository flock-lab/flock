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
  echo "Syntax: nexmark [-g|-h|-c|-r [-b <bench_type>] [-q <query_id>] [-s <number_of_seconds>] [-e <events_per_second>] [-p <number_of_parallel_streams>]]"
  echo "options:"
  echo "g     Print the GPL license notification."
  echo "h     Print this Help."
  echo "c     Compile and deploy the benchmark."
  echo "r     Run the benchmark. Default: false"
  echo "b     The type of the benchmark [nexmark, ysb]. Default: 'nexmark'"
  echo "q     NexMark Query Number [0-9]. Ignored if '-b' is not 'nexmark'. Default: 5"
  echo "p     Number of Data Generators. Default: 1"
  echo "s     Seconds to run the benchmark. Default: 10"
  echo "e     Number of events per second. Default: 1000"
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
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  echo
  echo $(echogreen "[2] Compiling the Benchmark Client ...")
  cd ../../bench
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
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
bench="nexmark"
generators=1
events_per_second=1000
seconds=10
query=5
flock=$(<src/bin/cli/src/flock)
# Get the options
while getopts "hgcrq:b:p:s:e:" option; do
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
  b) # benchmark type
    bench=$OPTARG
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
  echo $(echogreen "============================================================")
  echo
  echo $(echogreen "[OK] Benchmark Starting")
  echo

  if [ $bench = "ysb" ]; then
    benchmark="ysb_bench"
    query_args=""
  else
    benchmark="nexmark_bench"
    query_args="-q $query"
  fi

  # dry run to warm up the lambda functions.
  echo $(echogreen "[1] Warming up the lambda functions")
  echo
  RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/$benchmark \
    $query_args -g $generators -s $seconds --events_per_second $events_per_second --debug
  echo $(echoblue "-------------------------------------------------------------")
  echo
  sleep 2

  # run the benchmark
  echo $(echogreen "[2] Running the benchmark")
  echo
  RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/$benchmark \
    $query_args -g $generators -s $seconds --events_per_second $events_per_second --debug
  echo $(echoblue "-------------------------------------------------------------")
  echo
  echo $(echogreen "[OK] Nexmark Benchmark Complete")
elif [ "$run" = "false" ]; then
  echo
  echo $(echored "Error: incomplete command line arguments, please use '-h' for help.")
  echo
fi
