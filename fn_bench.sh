#!/bin/bash
# Copyright (c) 2021 UMD Database Group. All Rights Reserved.
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
Help()
{
   # Display Help
   echo "Nexmark Benchmark Script for Flock"
   echo
   echo "Syntax: fn_bech [-g|h|c]"
   echo "options:"
   echo "g     Print the GPL license notification."
   echo "h     Print this Help."
   echo "c     Compile and deploy the benchmark."
   echo
}

############################################################
# License                                                  #
############################################################
AGPLV3()
{
  # Display GPL license
  echo "Nexmark Benchmark Script for Flock"
  echo
  echo "Copyright (c) 2020-Present UMD Database Group."
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
Nexmark()
{
  # Compile and Deploy
  echo "Nexmark Benchmark Script"
  cd src/function; cargo +nightly build --target x86_64-unknown-linux-gnu --release
  cd ../../bench; cargo +nightly build --target x86_64-unknown-linux-gnu --release
  cd ../target/x86_64-unknown-linux-gnu/release; ./flock-cli -u nexmark_lambda -k nexmark
  cd ../../..
  echo "Nexmark Benchmark Script Complete"
}

############################################################
# Process the input options. Add options as needed.        #
############################################################
# Get the options
while getopts ":hgc" option; do
  case $option in
    h) # display Help
        Help
        exit;;
    g) # display GPL license
        AGPLV3
        exit;;
    c) # compile and deploy the benchmark
        Nexmark
        exit;;
    \?) # Invalid option
        echo "Error: Invalid option"
        exit;;
  esac
done

# cd ./target/x86_64-unknown-linux-gnu/release/
# RUST_LOG=info ./nexmark_bench  -q 3  -g 1 -s 3 --events_per_second 3000 --debug
