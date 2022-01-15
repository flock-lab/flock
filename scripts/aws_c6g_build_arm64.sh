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

#!/usr/bin/env sh

set -e

# Please use `c6g.4xlarge` to compile arm64 release binaries.

# Install dependencies.
sudo yum update -y
sudo yum install -y git gcc-c++ cmake
sudo yum install -y openssl-devel llvm-devel

# Install Rust and its aarch64 toolchains.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
rustup target add --toolchain nightly aarch64-unknown-linux-gnu
rustup target add --toolchain nightly x86_64-unknown-linux-gnu

# rustup target add --toolchain nightly aarch64-unknown-linux-musl
# git clone git://git.musl-libc.org/musl
# cd musl && ./configure && sudo make install
# export PATH="/usr/local/musl/bin:$PATH"

# Clone the repository.
git clone https://github.com/flock-lab/flock && cd flock/flock-function

# Build and deploy the Flock generic functions.
#
# Large-System Extensions (LSE)
#
# The Graviton2 processor in C6g[d], C6gn, M6g[d], R6g[d], T4g, X2gd, Im4gn, Is4gen, and G5g
# instances has support for the Armv8.2 instruction set. Armv8.2 specification includes the
# large-system extensions (LSE) introduced in Armv8.1. LSE provides low-cost atomic operations:
# LSE improves system throughput for CPU-to-CPU communication, locks, and mutexes. The
# improvement can be up to an order of magnitude when using LSE instead of load/store exclusives.
# LSE can be enabled in Rust and we've seen cases on larger machines where performance is
# improved by over 3x by setting the RUSTFLAGS environment variable and rebuilding your project.
#
# If you're running only on Graviton2 or newer hardware you can also enable other instructions
# by setting the cpu target as well: `-Ctarget-cpu=neoverse-n1`
# cargo +nightly build --target aarch64-unknown-linux-musl --release  --features "simd mimalloc"
export RUSTFLAGS="-Ctarget-feature=+lse -Ctarget-cpu=neoverse-n1"
cargo +nightly build --target aarch64-unknown-linux-gnu --release --features "simd mimalloc"
