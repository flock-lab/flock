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
  echo $(echogreen "Nexmark Benchmark Script for Flock")
  echo
  echo "Syntax: nexmark [-g|-h|-c|-r [-q <query_id>] [-s <number_of_seconds>] [-e <events_per_second>] [-p <number_of_parallel_streams>]]"
  echo "options:"
  echo "g     Print the GPL license notification."
  echo "h     Print this Help."
  echo "c     Compile and deploy the benchmark."
  echo "r     Run the benchmark."
  echo "q     NexMark Query Number [0-9]. Default: 5"
  echo "p     Number of NexMark Generators. Default: 1"
  echo "s     Seconds to run the benchmark. Default: 10"
  echo "e     Number of events per second. Default: 1000"
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
  echo $(echogreen "============================================================")
  echo $(echogreen "         Compiling and Deploying Nexmark Benchmark          ")
  echo $(echogreen "============================================================")
  echo
  echo $(echogreen "[1] Compiling Nexmark Benchmark Lambda Function...")
  cd src/function
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  echo
  echo $(echogreen "[2] Compiling Nexmark Benchmark Client ...")
  cd ../../bench
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  echo
  echo $(echogreen "[3] Compiling Flock CLI...")
  cd src/bin/cli
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  echo
  echo $(echogreen "[4] Deploying Nexmark Benchmark Lambda Function...")
  cd ../target/x86_64-unknown-linux-gnu/release
  ./flock-cli -u nexmark_lambda -k nexmark
  cd ../../..
  echo $(echoblue "-------------------------------------------------------------")
  echo
  echo $(echogreen "[OK] Nexmark Benchmark Script Complete")
  echo
}

############################################################
# Process the input options. Add options as needed.        #
############################################################
run="false"
generators=1
events_per_second=1000
seconds=10
query=5
flock=$(<src/bin/cli/src/flock.txt)
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

if [ "$run" = "true" ]; then
  echo "$flock"
  echo
  echo $(echogreen "============================================================")
  echo $(echogreen "                  Running the benchmark                     ")
  echo $(echogreen "============================================================")
  echo "Nexmark Query Number: $query"
  echo "Nexmark Generators: $generators"
  echo "Nexmark Events Per Second: $events_per_second"
  echo "Nexmark Seconds to Run: $seconds"
  echo $(echogreen "============================================================")
  echo
  echo $(echogreen "[OK] Nexmark Benchmark Starting")
  echo

  # dry run to warm up the lambda functions.
  echo $(echogreen "[1] Warming up the lambda functions")
  echo
  RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/nexmark_bench \
    -q $query -g $generators -s $seconds --events_per_second $events_per_second --debug
  echo $(echoblue "-------------------------------------------------------------")
  echo
  sleep 5

  # run the benchmark
  echo $(echogreen "[2] Running the benchmark")
  echo
  RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/nexmark_bench \
    -q $query -g $generators -s $seconds --events_per_second $events_per_second --debug
  echo $(echoblue "-------------------------------------------------------------")
  echo
  echo $(echogreen "[OK] Nexmark Benchmark Complete")
elif [ "$run" = "false" ]; then
  echo
  echo $(echored "Error: incomplete command line arguments, please use '-h' for help.")
  echo
fi
