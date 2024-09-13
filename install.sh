#!/usr/bin/env bash

set -ex

cargo zigbuild --profile release-small --locked
cp target/release-small/i3status-rs ~/.local/bin/cargo/bin/i3status-rs
