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
  echo "Syntax: flock_bench [-g|-h|-c|-r]"
  echo "options:"
  echo "g     Print the GPL license notification."
  echo "h     Print this Help."
  echo "c     Compile and deploy the benchmark."
  echo "r     Run the benchmark."
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
  echo $(echogreen "[1] Compiling Flock Lambda Function...")
  cd flock-function
  cargo +nightly build --target x86_64-unknown-linux-gnu --release --features "datafusion/simd mimalloc"
  echo
  echo $(echogreen "[2] Compiling Flock CLI...")
  cd ../flock-cli
  cargo +nightly build --target x86_64-unknown-linux-gnu --release
  echo
  echo $(echogreen "[3] Deploying Flock Lambda Function...")
  echo
  cd ../target/x86_64-unknown-linux-gnu/release
  ./flock-cli upload -p flock -k flock
  cd ../../..
  echo
  echo $(echogreen "[OK] Flock Completed Deployment.")
  echo
}

############################################################
# Run Benchmarks                                           #
############################################################
Run() {
  echo $(echogreen "============================================================")
  echo $(echogreen "                   Running the benchmarks                   ")
  echo $(echogreen "============================================================")
  echo
  echo $(echored "[Error] If you want to run the benchmark, please use \"flock-cli [nexmark|ysb] run\".")
  echo
  echo $(echoblue "$ ./target/x86_64-unknown-linux-gnu/release/flock-cli nexmark run -h")
  echo
  ./target/x86_64-unknown-linux-gnu/release/flock-cli nexmark run -h
  echo
}

############################################################
# Process the input options. Add options as needed.        #
############################################################

# Get the options
while getopts "hgcr" option; do
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
    Run
    exit
    ;;
  \?) # Invalid option
    echo $(echored "Error: Invalid option")
    echo
    Help
    exit
    ;;
  esac
done

echo
echo $(echored "Error: incomplete command line arguments, please use '-h' for help.")
echo
