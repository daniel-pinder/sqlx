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
# FEATURES ARE NARROWED DELIBERATELY (this script used to run
# --all-features). The fleet runner image (ci-runner-image/Dockerfile) has
# no cmake and no libclang, and --all-features reaches aws-lc-sys (a cmake
# build) and libsqlite3-sys's unbundled build (libclang, and no system
# libsqlite3 on the image either). `_rt-tokio` is the runtime the tests run
# on; the SQLite driver arrives through sqlx-core's dev-dependency on the
# facade, which bundles SQLite (cc only); `any` and `sqlx-toml` are the two
# features that three of the crate's unit tests gate on, so the suite stays
# whole at 10 unit + 30 doctests, all green locally.
#
# `--locked` is load-bearing: the lockfile is tracked in this fork and part
# of the reviewed bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo test --locked -p sqlx-core --features _rt-tokio,any,sqlx-toml

# Socket timeout witnesses use scripted loopback peers, not a database. The
# postgres dev-dependency enables Tokio and rustls/ring (no cmake/libclang).
# The existing bind_iter doctest imports chrono.
cargo test --locked -p sqlx-postgres --features chrono
