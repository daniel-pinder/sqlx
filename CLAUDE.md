# sqlx-fibretel — agent notes

A **bridge fork** of upstream sqlx: base `v0.9.0`
(`75bc0487eb661da811bb7a3c5d158f1bd463fef4`), one patch commit on top,
upstreamable as-is. See `README.md` for what the patch does.

## Rules

- **The patch commit stays byte-identical to the reviewed source.** The
  reviewed tree is the `a8416412` rev in the working fork
  (`~/fibretel/sqlx`, branch `fibretel/pool-bounds`); any rebase must prove
  `git diff <reviewed-rev> HEAD` is empty.
- **Upstream-shaped only.** No FibreTel business logic, no consumer-
  specific code, nothing that would not belong in a PR to
  `launchbadge/sqlx`. Repo furniture (this file, `README.md`,
  `AGENTS.md`) is documented as separate from the patch.
- **Publication is publish-once.** FibreTel consumes the seven `sqlx-*`
  crates from its private registry at exactly `=0.9.0`, from these bytes;
  a later fix-up needs a new upstream-compatible version.
- **Tests:** `cargo test -p sqlx-core` — the three pool-bound tests drive
  in-memory SQLite and need no database server.
- **The upstream PR** is opened by Daniel, not by an agent.

`AGENTS.md` is a symlink to this file (Codex reads `AGENTS.md`, Claude and
Grok read `CLAUDE.md`).
