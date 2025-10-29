#!/usr/bin/env bash

set -ex

cargo zigbuild --release
cp -f ./target/release/i3status-rs ~/.local/bin/cargo/bin/
