#!/usr/bin/env bash
#
# The repository heavy gate script (the rust-service `heavy-test-command`,
# declared by the ci-config entry projects/sqlx-fibretel/options.yml).
#
# The patch's own suite: sqlx-core under the features its pool tests need.
# The fork's pool patch (the bounded on-release ping and the bounded close()
# join, bead billing-1e0j) lives in sqlx-core's pool, and its three tests
# drive in-memory SQLite — no database server, no network, no fixtures.
#
# THE UNIT SUITE'S FEATURES ARE NARROWED DELIBERATELY (this script used to run
# --all-features for it too). `_rt-tokio` is the runtime the pool and socket
# tests run on; the SQLite driver arrives through sqlx-core's dev-dependency
# on the facade, which bundles SQLite (cc only); `any` and `sqlx-toml` are the
# two features that three of the crate's unit tests gate on, so the suite
# stays whole at 10 unit + 30 doctests, all green locally.
#
# WHAT THE NARROWING MUST NOT DO is hide a broken runtime arm. billing-7t7r.1
# was exactly that: the fork's tracked socket left `connect_tcp_async_io`
# returning `impl Socket`, which no longer holds for the raw async-io stream,
# and `--all-features` failed to compile while the `_rt-tokio` suite stayed
# green. tests/fork-check.sh compiles BOTH runtime arms (together and each
# alone) and runs the all-features doctest; it runs here so the arm the suite
# does not exercise is still compiled by every heavy gate. The async-io arm is
# additionally RUN by tests/socket-timeout-async-io.sh (billing-7t7r.2), which
# replays the key socket_timeout witnesses through its tracked wrapper.
#
# `--all-features` reaches aws-lc-sys (cmake + a C compiler) and bindgen
# (libclang at runtime). The fleet runner image installs cmake and
# libclang1-19; a failure naming either is an image problem, not a code one.
#
# `--locked` is load-bearing: the lockfile is tracked in this fork and part
# of the reviewed bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

tests/fork-check.sh

# The async-io arm is not only compiled, it is RUN: the key socket_timeout
# witnesses execute through rt_async_io's tracked wrapper on an async-io
# reactor (billing-7t7r.2). See the script header for the arm-selection
# mechanism and the red proof recorded on the bead.
tests/socket-timeout-async-io.sh

cargo test --locked -p sqlx-core --features _rt-tokio,any,sqlx-toml

# Socket timeout witnesses use scripted loopback peers, not a database. The
# postgres dev-dependency enables Tokio and rustls/ring (no cmake/libclang).
# The existing bind_iter doctest imports chrono.
cargo test --locked -p sqlx-postgres --features chrono
