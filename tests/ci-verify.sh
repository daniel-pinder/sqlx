#!/usr/bin/env bash
#
# The repository heavy gate script (the rust-service `heavy-test-command`,
# declared by the ci-config entry projects/sqlx-fibretel/options.yml).
#
# The patch's own suite: sqlx-core under ALL features. The fork's one patch
# (the bounded on-release ping and bounded close() join, bead billing-1e0j)
# lives in sqlx-core's pool, and its tests drive in-memory SQLite — no
# database server, no network, no fixtures. All features rather than the
# default set so the pool tests compile against every dependency surface
# sqlx-core ships; the archetype's own lanes run the workspace suite with
# default features.
#
# `--locked` is load-bearing: the lockfile is part of the reviewed bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo test --locked -p sqlx-core --all-features
