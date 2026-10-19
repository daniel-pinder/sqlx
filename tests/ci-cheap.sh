#!/usr/bin/env bash
#
# The repository cheap gate script (the rust-service `cheap-command`,
# declared by the ci-config entry projects/sqlx-fibretel/options.yml).
#
# WHAT THIS SCRIPT IS FOR NOW. The archetype lane owns the fork's gate
# (Daniel's 2026-10-11 fork-CI ruling, implemented by ci-templates !373 +
# ci-config !170): with `fork-patched-crates: sqlx-core sqlx-postgres`
# the lane runs `cargo fmt --all --check`, clippy on both patched crates
# with `-D warnings -D unsafe_code`, and `cargo check --workspace --all-targets
# --locked` — lint what we own, compile what we vendor. This script no longer
# repeats any of that. What it keeps is the one dimension the lane cannot
# express:
#
# THE FEATURE-SCOPED LINT. The lane's clippy runs at default features — the
# template refuses a features input (eg0u.2: no conditional, no argv
# injection) — but the pool and socket timeout witnesses run under Tokio.
# The feature-scoped lint of both patched crates lives here, where a feature
# set can be named. The postgres test graph also enables rustls/ring for the
# silent SSL negotiation witness.
#
# `--no-deps` is measured, not assumed: trailing `--` flags and clippy's
# wrapper reach workspace-member DEPENDENCIES, so a bare `-p sqlx-core`
# invocation still denies warnings in the frozen dev-dependency graph it
# builds (sqlx-core's dev-dependency on the facade unifies sqlx-sqlite with
# its deprecated time API, sqlx-postgres and sqlx-macros-core). Measured at
# 1.97.1 with a two-crate micro-probe in both directions: with `--no-deps` a
# dependency's deprecation is a warning and its clippy lint does not fire,
# while the selected crate's own lint is still denied. No `-A` allowance is
# carried: the time deprecations live in frozen sqlx-sqlite, which this
# invocation does not deny.
#
# The union-feature builds this script used to run (the other five released
# crates at the six-crate feature set, and the facade at a consumer-shaped
# union) were dropped with the 2026-10-11 lane handover: the lane's
# workspace check builds every member at default features, and the heavy
# lane's tests/ci-verify.sh compiles the patch-test graph (`cargo test -p
# sqlx-core --features _rt-tokio,any,sqlx-toml`) with the drivers unified in
# through the same dev-dependency. Feature sets stay deliberately narrow:
# the runner image has no cmake and no libclang, and `_rt-tokio` pulls
# neither aws-lc-sys nor bindgen.
#
# Measured green (rc=0) at 1.97.1, the fleet toolchain the repository pins.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo clippy --locked --all-targets -p sqlx-core -p sqlx-postgres --features _rt-tokio --no-deps \
    -- -D warnings -D unsafe_code
