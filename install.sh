#!/usr/bin/env bash

set -ex

cargo build --release
cp -f ./target/release/i3status-rs ~/.local/bin/cargo/bin/
