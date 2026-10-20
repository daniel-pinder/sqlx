#!/usr/bin/env bash
#
# billing-7t7r.2: run the KEY socket_timeout witnesses on an async-io reactor.
#
# Why this exists. tests/fork-check.sh proves the async-io arm COMPILES; it
# cannot prove the tracked wrapper still RECORDS progress. The witnesses in
# sqlx-postgres/tests/socket_timeout.rs are plain `#[test]` cases around
# scripted loopback peers, driven by `test_block_on`, and sqlx-core's
# src/rt/mod.rs resolves that to `async_io::block_on` whenever `_rt-async-io`
# is enabled — its cfg order puts async-io before tokio. The dev-dependency on
# the facade always enables Tokio too, but under `async_io::block_on` no Tokio
# runtime is current, so `connect_tcp` also takes the async-io arm
# (`rt_tokio::available()` is false). Enabling `sqlx/runtime-smol` here
# therefore runs the SAME witnesses through
# `rt_async_io::socket::TrackedTcpStream`.
#
# How the arm itself is proved. The bead records the mutation: deleting the
# `if read > 0` stamp in sqlx-core/src/rt/rt_async_io/socket.rs makes the
# transport-progress witness below FAIL. A run that silently regressed to the
# Tokio arm would keep passing under that mutation, so the red proof is the
# arm's evidence — this script is the harness that carries it.
#
# Selection is exact-name and must be non-empty: a renamed or dropped witness
# fails this gate instead of silently running nothing.
set -euo pipefail
cd "$(dirname "$0")/.."

witnesses=(
    # Transport progress on the plaintext arm: each partial read restarts the
    # inactivity bound although the whole message outlives it.
    partial_reads_reset_the_inactivity_timeout
    # Exact bound: silence after a delivered byte fires ONE bound after that
    # byte, never after a freshly granted window.
    tls_progress::silence_after_progress_times_out_within_one_bound
    # Transport progress on the TLS arm: an idle wait opened after the last
    # byte still fires one bound from THAT byte.
    tls_progress::a_delayed_idle_wait_is_not_granted_a_fresh_bound
    # OUTGOING waits count outbound progress only, on both arms (billing-7t7r
    # P2, grok): a blocked COPY send is not extended by bytes the peer
    # streams back...
    inbound_bytes_do_not_extend_a_blocked_plaintext_flush
    # ...and a blocked TLS outbound wait is not extended by inbound
    # ciphertext, even though the session reads it while the window is full.
    tls_progress::inbound_ciphertext_does_not_extend_a_blocked_outbound_flush
)

for witness in "${witnesses[@]}"; do
    echo "async-io witness: $witness"
    if ! output=$(cargo test --locked -p sqlx-postgres \
        --features chrono,sqlx/runtime-smol --test socket_timeout \
        -- --exact "$witness" 2>&1); then
        printf '%s\n' "$output"
        echo "async-io witness: $witness FAILED" >&2
        exit 1
    fi
    printf '%s\n' "$output"
    if ! grep -q 'test result: ok\. 1 passed' <<<"$output"; then
        echo "async-io witness: $witness did not run exactly one passing test" >&2
        exit 1
    fi
done

echo "async-io witness: all ${#witnesses[@]} witnesses passed on the async-io reactor"
