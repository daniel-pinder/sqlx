#!/usr/bin/env bash
#
# The repository cheap gate script (the rust-service `cheap-command`,
# declared by the ci-config entry projects/sqlx-fibretel/options.yml).
#
# SUBJECT: the seven RELEASED crates — the publish set the fork exists to
# ship: sqlx-core, sqlx-mysql, sqlx-postgres, sqlx-sqlite, sqlx-macros-core,
# sqlx-macros and the `sqlx` facade. The workspace's other members
# (`sqlx-cli`, `sqlx-test`, the examples) are repo furniture, not release
# targets, and stay outside the gate.
#
# THE SPLIT AGAINST THE ARCHETYPE'S OWN LANES, WRITTEN DOWN. The rust-service
# lanes already run `cargo fmt`, the workspace clippy (default features,
# `-D warnings -D unsafe_code` — templates/rust-cheap.yml) and the workspace
# nextest run; none of that is repeated here. What this script adds:
#
#   * the ALL-FEATURES clippy over the seven crates. The lane's own clippy
#     item cannot take features (templates/rust-integration.yml records the
#     gap at the clippy step), and the fork's patch sits in sqlx-core, whose
#     default feature set is the narrowest surface of the crate.
#   * `cargo fmt --all -- --check` under the pinned toolchain, so the tree's
#     formatting is the toolchain's own and a rebase diff stays reviewable.
#
# TWO SCOPING DECISIONS, BOTH MEASURED AGAINST THE FROZEN BYTES:
#
#   1. The `sqlx` FACADE is linted at its lib target only. Its tests/ and
#      benches/ are upstream's per-driver integration suites, shipped inside
#      the published .crate and therefore frozen: under --all-features they
#      do not even type-check (upstream builds them per-driver; measured
#      17 units with E0282/E0308 and clippy's default-deny lints — the
#      `any` suite assumes exactly one driver). Linting the facade's lib is
#      the part of its surface the fork can gate.
#   2. ONE RECORDED WAIVER in both invocations: `-A deprecated`. Under
#      --all-features the union enables sqlx-sqlite's `time` type
#      integration, whose sqlx-sqlite/src/types/time.rs uses time 0.3.55's
#      deprecated format_description API — 40 deprecation errors, measured.
#      The published bytes stay byte-identical to the certified rev
#      (a8416412, bead billing-1e0j publish-once), so the fork cannot carry
#      a lint fix for it, and upstream's own CI never runs this combination.
#      Every other warning stays denied.
#
# `--locked` is load-bearing: the lockfile is tracked in this fork and part
# of the reviewed bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check

cargo clippy --locked \
    -p sqlx-core -p sqlx-mysql -p sqlx-postgres -p sqlx-sqlite \
    -p sqlx-macros-core -p sqlx-macros \
    --all-targets --all-features \
    -- -D warnings -D unsafe_code -A deprecated

cargo clippy --locked -p sqlx --all-features \
    -- -D warnings -D unsafe_code -A deprecated
