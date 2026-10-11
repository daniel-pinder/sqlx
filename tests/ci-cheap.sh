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
# THE SPLIT AGAINST THE ARCHETYPE'S OWN LANES, WRITTEN DOWN. The rust-service
# lanes already run `cargo fmt`, the workspace clippy (default features,
# `-D warnings -D unsafe_code` — templates/rust-cheap.yml) and the workspace
# nextest run; none of that is repeated here. What this script adds:
#
#   * `cargo fmt --all -- --check` under the pinned toolchain, so the tree's
#     formatting is the toolchain's own and a rebase diff stays reviewable.
#   * the feature-scoped clippy below, wider than the lane's default set.
#
# FEATURES ARE NARROWED DELIBERATELY (this script used to lint
# --all-features). The fleet runner image (ci-runner-image/Dockerfile) has
# build-essential, pkg-config and libssl-dev but NO cmake and NO libclang,
# and --all-features reaches aws-lc-sys (a cmake build),
# libsqlite3-sys/buildtime_bindgen (libclang) and native-tls/openssl-sys.
# The sets below are what the patch's tests need; measured on the
# dependency graphs, they pull none of aws-lc, bindgen or a TLS backend.
#
# TWO SCOPING DECISIONS, BOTH MEASURED AGAINST THE FROZEN BYTES:
#
#   1. The `sqlx` FACADE is linted at its lib target only. Its tests/ and
#      benches/ are upstream's per-driver integration suites, shipped inside
#      the published .crate and therefore frozen: under a multi-driver
#      feature set they do not even type-check (upstream builds them
#      per-driver; measured 17 units with E0282/E0308 and clippy's
#      default-deny lints — the `any` suite assumes exactly one driver).
#   2. ONE RECORDED WAIVER in both invocations: `-A deprecated`. The `time`
#      type integration (enabled by sqlx-core's dev-dependencies and by the
#      facade's `time` feature) compiles sqlx-sqlite/src/types/time.rs,
#      which uses time 0.3.55's deprecated format_description API — 40
#      deprecation errors, measured. The published bytes stay byte-identical
#      to the certified rev (a8416412, bead billing-1e0j publish-once), so
#      the fork cannot carry a lint fix for it, and upstream's own CI never
#      runs this combination. Every other warning stays denied.
#
# `--locked` is load-bearing: the lockfile is tracked in this fork and part
# of the reviewed bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check

# The six release crates, all targets. `sqlx-core/_rt-tokio` is the runtime
# the pool tests take; the drivers' test-shaped feature set (postgres,
# sqlite-bundled, mysql, migrate, macros, time, uuid) arrives through
# sqlx-core's dev-dependency on the facade, unified into this same build.
# `macros` and `migrate` are named explicitly on sqlx-macros-core: they are
# two of the features the crate's own dev-dependency requests, and without
# them (measured) the frozen crate's `resolve_path` helper is dead code and
# `-D warnings` fails; with them it is live, and the query/migrate modules
# are linted too.
cargo clippy --locked --all-targets \
    -p sqlx-core -p sqlx-mysql -p sqlx-postgres -p sqlx-sqlite \
    -p sqlx-macros -p sqlx-macros-core \
    --features "sqlx-core/_rt-tokio,sqlx-macros-core/macros,sqlx-macros-core/migrate" \
    -- -D warnings -D unsafe_code -A deprecated

# The facade at its lib target only (see scoping decision 1), at a
# consumer-shaped feature union; this invocation also lints every member's
# lib through the workspace-member clippy wrapper.
cargo clippy --locked -p sqlx \
    --features "runtime-tokio,any,mysql,postgres,sqlite,migrate,macros,json,sqlx-toml,chrono,time,uuid,bigdecimal,rust_decimal,bit-vec,ipnet,ipnetwork,mac_address" \
    -- -D warnings -D unsafe_code -A deprecated
