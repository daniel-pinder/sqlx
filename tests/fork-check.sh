#!/usr/bin/env bash
#
# billing-7t7r.1: the fork's socket patch must compile on EVERY runtime arm,
# not only the one the narrowed suite happens to run on.
#
# Why this exists. The fork replaces upstream's `Async<TcpStream>` socket with
# the tracked wrappers (rt_tokio::socket / rt_async_io::socket), because every
# transport now reports `last_transport_progress` and friends. One upstream
# signature survived that change: `connect_tcp_async_io` still returned
# `impl Socket`, which compiles only if the raw `async_io::Async<TcpStream>`
# implements `Socket` — the blanket impl upstream had and the fork does not.
# The narrowed suite (`_rt-tokio` only) never compiled that arm, so the break
# existed only under `--all-features`; upstream v0.9.0 compiles the same
# command (30 doctests pass), so this is a fork defect, not an upstream one.
#
# Every check is its own command so a failure names the arm:
#
#   1. all features together — the combination CI's doc lane enables;
#   2. each runtime feature ALONE — a cfg block that is only valid when the
#      other arm is also present is exactly the class of bug above;
#   3. the all-features DOC test — `cargo check` does not instantiate
#      doctests, and this is the invocation that failed.
#
# Toolchain note: `--all-features` reaches aws-lc-sys (cmake + a C compiler)
# and bindgen (libclang at runtime). The fleet runner image carries cmake and
# libclang1-19; a failure naming those is an image problem, not a code one.
#
# This script covers the async-io arm by COMPILATION. Its runtime coverage is
# tests/socket-timeout-async-io.sh: the witnesses are plain `#[test]` cases
# around scripted loopback peers driven by `test_block_on`, which resolves to
# `async_io::block_on` whenever `_rt-async-io` is enabled (see sqlx-core's
# src/rt/mod.rs), so the same cases run through the tracked async-io wrapper.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "fork-check: sqlx-core --all-features"
cargo check --locked -p sqlx-core --all-features

for feature in _rt-tokio _rt-async-io _rt-async-std _rt-smol _rt-async-global-executor; do
    echo "fork-check: sqlx-core --features $feature"
    cargo check --locked -p sqlx-core --features "$feature"
done

echo "fork-check: sqlx-core --all-features --doc"
cargo test --locked -p sqlx-core --all-features --doc
