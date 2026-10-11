# sqlx-fibretel

A **bridge fork** of [sqlx](https://github.com/launchbadge/sqlx) carrying
one patch, until upstream ships it.

- **Base:** upstream `v0.9.0` — `75bc0487eb661da811bb7a3c5d158f1bd463fef4`.
- **The patch:** one commit on top, upstreamable as-is. It bounds two
  waits in `sqlx-core`'s pool that a wedged backend could otherwise park
  forever:
  - the on-release ping — `RELEASE_PING_TIMEOUT` (5 s, matching the
    existing `CLOSE_ON_DROP_TIMEOUT`); on expiry the connection is dropped,
    never gracefully closed (a peer that ACKs and stops reading would park
    the graceful close on the same undrainable buffer);
  - the `PoolInner::close()` join — `CLOSE_JOIN_TIMEOUT` (5 s); on expiry
    the join is abandoned and the idle drain proceeds on one shared
    deadline, with the abandoned-join accounting saturating so
    `Pool::size()` cannot wrap.
- **Why a fork:** neither wait is boundable by configuration (measured in
  FibreTel's `billing-1e0j` diagnosis), and with every permit wedged,
  `acquire` fails `PoolTimedOut` for the life of the process.

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
