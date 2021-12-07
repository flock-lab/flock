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

############################################################
# Help                                                     #
############################################################
Help() {
  # Display Help
  echo "Nexmark Benchmark Script for Flock"
  echo
  echo "Syntax: fn_bech [-g|h|c|r] [-q <query_id>] [-s <number_of_seconds>] [-e <events_per_second>] [-p <number_of_parallel_streams>]"
  echo "options:"
  echo "g     Print the GPL license notification."
  echo "h     Print this Help."
  echo "c     Compile and deploy the benchmark."
  echo "r     Run the benchmark."
  echo "q     NexMark Query Number [0-9]."
  echo "p     Number of NexMark Generators."
  echo "s     Seconds to run the benchmark."
  echo "e     Number of events per second."
  echo
}

############################################################
# License                                                  #
############################################################
AGPLV3() {
  # Display GPL license
  echo "Nexmark Benchmark Script for Flock"
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
# Compile and Deploy Nexmark Bench                         #
############################################################
Nexmark() {
  # Compile and Deploy
  echo "Nexmark Benchmark Script"
  cd src/function
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  cd ../../bench
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  cd ../target/x86_64-unknown-linux-gnu/release
  ./flock-cli -u nexmark_lambda -k nexmark
  cd ../../..
  echo "Nexmark Benchmark Script Complete"
}

############################################################
# Process the input options. Add options as needed.        #
############################################################
run="false"
generators=1
events_per_second=1000
seconds=10
query=5
# Get the options
while getopts "hgcrq:w:s:e:" option; do
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
    Nexmark
    exit
    ;;
  r) # run the benchmark
    run="true"
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
    echo "Error: Invalid option"
    echo
    Help
    exit
    ;;
  esac
done

if [ "$run" = "true" ]; then
  echo "============================================================"
  echo "                  Running the benchmark                     "
  echo "============================================================"
  echo "Nexmark Query Number: $query"
  echo "Nexmark Generators: $generators"
  echo "Nexmark Events Per Second: $events_per_second"
  echo "Nexmark Seconds to Run: $seconds"
  echo "============================================================"
  echo
  echo "Nexmark Benchmark Starting"
  echo

  # delete the old nexmark_datasource function to make sure we have a clean slate
  aws lambda delete-function --function-name nexmark_datasource

  # dry run to warm up the lambda functions.
  echo "[1] Warming up the lambda functions"
  echo
  RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/nexmark_bench \
    -q $query -g $generators -s $seconds --events_per_second $events_per_second --debug
  echo "-------------------------------------------------------------"
  echo
  sleep 5

  # run the benchmark
  echo "[2] Running the benchmark"
  echo
  RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/nexmark_bench \
    -q $query -g $generators -s $seconds --events_per_second $events_per_second --debug
  echo "-------------------------------------------------------------"
  echo
  echo "Nexmark Benchmark Complete"
elif [ "$run" = "false" ]; then
  echo "Error: Incomplete Command Line Arguments"
  echo
  Help
fi
