#!/usr/bin/env bash
#
# The repository cheap gate script (the rust-service `cheap-command`,
# declared by the ci-config entry projects/sqlx-fibretel/options.yml).
#
# SUBJECT: the seven RELEASED crates — sqlx-core, sqlx-mysql,
# sqlx-postgres, sqlx-sqlite, sqlx-macros-core, sqlx-macros and the `sqlx`
# facade. The workspace's other members (`sqlx-cli`, `sqlx-test`, the
# examples) are repo furniture, not release targets.
#
# TWO SCOPE DECISIONS, BOTH THE FORK-CI RULING'S (Daniel, 2026-10-11;
# recorded here because this file is what has to carry them):
#
#   1. THE LINT GATE IS sqlx-core's. Clippy runs with `-D warnings` over
#      sqlx-core ONLY — the crate the FibreTel patch lives in, and the only
#      bytes this fork may change. The other six released crates are FROZEN
#      at the certified rev (a8416412, publish-once), so the fork cannot fix
#      a lint in them, and newer clippy rows grow lints that fire on code
#      nobody here may edit. This script used to lint six crates; measured at
#      1.97.1 that scope fails on clippy's `useless_borrows_in_formatting` at
#      four frozen sites (sqlx-postgres src/connection/sasl.rs:120;
#      sqlx-macros-core src/query/output.rs:277/307/326), while the same
#      command at the old 1.94 pin is green. The delta is the toolchain, and
#      the answer is scope, not a waiver.
#
#      `--no-deps` is what makes that scope REAL, and it is measured, not
#      assumed: trailing `--` flags and clippy's wrapper reach workspace-
#      member DEPENDENCIES too, so a bare `-p sqlx-core` invocation still
#      denies warnings in the frozen dev-dependency graph it builds
#      (sqlx-core's dev-dependency on the facade unifies sqlx-sqlite,
#      sqlx-postgres and sqlx-macros-core into this build). Measured at
#      1.97.1: the command WITHOUT `--no-deps` fails on those frozen bytes
#      (40 `time::format_description` deprecations in
#      sqlx-sqlite/src/types/time.rs, the sasl.rs and output.rs sites above),
#      and a two-crate micro-probe pinned the mechanism both ways — with
#      `--no-deps` a dependency's deprecation is a warning and its clippy
#      lint does not fire, while the selected crate's own lint is still
#      denied. With it, the gate is exactly the ruling's: sqlx-core's bytes,
#      all targets, `-D warnings -D unsafe_code`, and NO `-A deprecated` —
#      the time deprecations live in frozen sqlx-sqlite, which this
#      invocation no longer denies (measured green without the allowance).
#
#   2. THE OTHER SIX CRATES MUST BUILD, NOT LINT: `cargo check` with NO
#      `-D warnings`, over today's feature sets. What the fork owes them is
#      that they compile against the toolchain it builds on.
#
# FEATURE SETS ARE NARROWED ON PURPOSE (this script used to lint
# --all-features): the fleet runner image (ci-runner-image/Dockerfile) has
# build-essential, pkg-config and libssl-dev but NO cmake and NO libclang,
# and --all-features reaches aws-lc-sys (a cmake build),
# libsqlite3-sys/buildtime_bindgen (libclang) and native-tls/openssl-sys.
# The sets below are what the patch's tests need; measured on the dependency
# graphs, they pull none of aws-lc, bindgen or a TLS backend.
#
# Measured green (rc=0) at 1.97.1, the fleet toolchain the repository pins.
#
# `--locked` is load-bearing: the lockfile is tracked in this fork and part
# of the reviewed bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check

# (1) THE LINT GATE — sqlx-core, all targets, `-D warnings -D unsafe_code`.
# `_rt-tokio` is the runtime the patch's pool tests take; the drivers'
# test-shaped feature set arrives through sqlx-core's dev-dependency on the
# facade, unified into this same build. `--no-deps` keeps the gate ON
# sqlx-core's own bytes: without it the trailing `-D warnings` (and clippy's
# wrapper) also reach the workspace-member dependencies this build compiles,
# whose frozen source carries lints the fork must not fix — see decision 1
# in the header.
cargo clippy --locked --all-targets -p sqlx-core --features _rt-tokio --no-deps \
    -- -D warnings -D unsafe_code

# (2) THE BUILD GATE — the other five released crates, all targets, at the
# feature set the six-crate invocation used to carry. `macros` and `migrate`
# are named explicitly on sqlx-macros-core: they are two of the features the
# crate's own dev-dependency requests, and without them (measured) the
# frozen crate's `resolve_path` helper is dead code.
cargo check --locked --all-targets \
    -p sqlx-mysql -p sqlx-postgres -p sqlx-sqlite \
    -p sqlx-macros -p sqlx-macros-core \
    --features "sqlx-core/_rt-tokio,sqlx-macros-core/macros,sqlx-macros-core/migrate"

# (3) THE FACADE at its lib target only: its tests/ and benches/ are
# upstream's per-driver integration suites, shipped inside the published
# .crate and therefore frozen; under a multi-driver feature union they do
# not even type-check (upstream builds them per-driver; measured 17 units
# with E0282/E0308). The lib is what consumers use, at a consumer-shaped
# feature union.
cargo check --locked -p sqlx --lib \
    --features "runtime-tokio,any,mysql,postgres,sqlite,migrate,macros,json,sqlx-toml,chrono,time,uuid,bigdecimal,rust_decimal,bit-vec,ipnet,ipnetwork,mac_address"
