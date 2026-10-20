# sqlx-fibretel

A **bridge fork** of [sqlx](https://github.com/launchbadge/sqlx) carrying
three patches, until upstream ships them.

- **Base:** upstream `v0.9.0` — `75bc0487eb661da811bb7a3c5d158f1bd463fef4`.
- **Pool bounds:** upstreamable as-is, they bound three
  waits in `sqlx-core`'s pool that a wedged backend could otherwise park
  forever:
  - the on-release ping — `RELEASE_PING_TIMEOUT` (5 s, matching the
    existing `CLOSE_ON_DROP_TIMEOUT`); on expiry the connection is dropped,
    never gracefully closed (a peer that ACKs and stops reading would park
    the graceful close on the same undrainable buffer);
  - the `PoolInner::close()` join — `CLOSE_JOIN_TIMEOUT` (5 s); on expiry
    the join is abandoned and the idle drain proceeds on one shared
    deadline, with the abandoned-join accounting saturating so
    `Pool::size()` cannot wrap;
  - the idle-connection liveness ping before a checkout — `ACQUIRE_PING_TIMEOUT`
    (5 s); on expiry the connection is discarded without an awaited close
    (the same synchronous-drop rule as the on-release ping), and the size
    guard is handed back so the acquire opens a fresh connection in the
    freed slot.
- **Socket bounds:** `PgConnectOptions::socket_timeout` optionally bounds
  each socket read, write, flush and shutdown wait, plus a total deadline for
  connection setup. Expiry returns `Io(TimedOut)` carrying `sqlx::error::SocketTimedOut` and permanently poisons
  the connection, so the pool discards it. The default is `None` (unchanged).
  The bound follows TRANSPORT progress in the wait's own direction. Every
  transport — plain TCP or UDS as well as TLS — records two instants: the last
  byte it moved in either direction, and the last byte it moved OUTBOUND. A
  receive wait is anchored to the former (`last + bound`, recomputed each
  iteration), so a record or message arriving in fragments is not a stall;
  an outgoing wait (write, flush, shutdown) is anchored to the latter only,
  on every backend, so a peer that keeps talking while never draining our
  send buffer still trips the bound one interval after our last byte was
  accepted. Only a whole bound with no progress in the wait's direction
  poisons. Idle LISTEN waits arm it on the first byte of the message: a
  connection with no byte at all waits indefinitely, while a message that has
  started arriving must complete within the bound, and its deadline stays that
  byte's own `last + bound` even when the wait starts late. A TLS session also
  tracks record FRAMING over the ciphertext it consumes (never decrypting it),
  so a record prefix pulled by an earlier read still arms the next idle wait.
  Only RECEIVE activity arms a read wait; a write may re-anchor a deadline but
  cannot start a message. COPY IN's user data source remains unbounded.
  Two exactness limits, both deliberate:
  - An unfinished TLS record can be shown unfinished, not shown to carry
    application data — TLS 1.3 ciphertext may decode to handshake or alert
    traffic — so the conservative predicate is "incomplete incoming record".
  - "Last transport byte" is the last successful userspace read/write, with
    executor scheduling tolerance, not the physical network arrival time.
  This does not add cancellation safety for queries dropped by external deadlines.
  DB-free witnesses live in `sqlx-postgres/tests/socket_timeout.rs` and the buffered
  socket's unit tests; both patched crates are in `.fibretel/fork.toml`'s scope.
- **Why a fork:** none of the waits is boundable by configuration (measured
  in FibreTel's `billing-1e0j` diagnosis, extended by its `messaging-v6m`
  follow-up), and with every permit wedged, `acquire` fails `PoolTimedOut`
  for the life of the process.

## How FibreTel consumes it

The seven patched `sqlx-*` crates are published to FibreTel's private
registry at exactly `=0.9.0`, and consumers point `[patch.crates-io]` at
that registry — no git source in the Rust build path. The version is
published **once**, from the reviewed bytes: any later fix-up needs a new
upstream-compatible version, and a yank is the only retraction.

**Publish order** (each `cargo publish -p <crate> --registry fibretel`
verifies against the index, so a crate must be visible there before its
dependents): `sqlx-core`; then `sqlx-mysql`, `sqlx-postgres`,
`sqlx-sqlite`; then `sqlx-macros-core`, `sqlx-macros`, and the `sqlx`
facade.

## The bridge ends when upstream carries the fix

Drop the patch, drop the registry pin, and follow upstream. The upstream
PR (title and body drafted alongside this fork) is opened against
`launchbadge/sqlx`.
