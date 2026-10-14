<!-- PR body for basecamp/once-campfire-go. Numbers: matched shared-harness run
     2026-10-14 against the latest published Rust revision f9dca47. -->

# Performance engine: every route beats the latest published Rust revision

This branch adds a measured performance engine on top of the port — owned
HTTP/1.1 loop, direct SQLite read/write lanes, generation-checked
whole-response cache, batched write path with bounded WAL, cable wake path,
compiled renderer — with the same schema, storage keys, password hashes and
Rails-signed sessions. Every fast path has a rollback switch.

## Results — vs Rust (matched run)

Shared verification harness (`basecamp/once-campfire-verification`, revision
`ec02deb`), Rust at its latest published revision `f9dca47` (the response-cache
revision), both runtime databases on tmpfs, c=16, 3 alternating rounds,
8-second samples, every response contract-checked, zero errors:

| HTTP workload (requests/sec) | Rust `f9dca47` | This branch | vs Rust |
|---|---:|---:|---:|
| Room page | 109,586 | **149,403** | **1.36×** |
| Messages page | 106,497 | **192,759** | **1.81×** |
| Sidebar | 124,369 | **166,116** | **1.34×** |
| Search | 125,870 | **197,400** | **1.57×** |
| Post a message | 8,235 | **15,817** | **1.92×** |

The local Rust image reproduces the published Rust figures within ~2–4%
(published: 106,494 / 102,697 / 120,294 / 121,378 / 8,037), so the comparison
is against a validated baseline. Raw receipts:
`/tmp/opencode/matched-rust-go/summary.json` (also recorded in
[`plans/validation.md`](plans/validation.md)).

### vs the C port (native, same pinning)

| Route | C | This branch | vs C |
|---|---:|---:|---:|
| Room page | 156,170 | 146,722 | 0.94× |
| Messages page | 161,041 | **188,938** | **1.17×** |
| Sidebar | 172,087 | 165,135 | 0.96× |
| Search | 171,762 | **198,252** | **1.15×** |
| Post a message | 7,449 | **17,994** | **2.42×** |

Ahead of C on messages, search and post; room and sidebar are near parity.

## How

- **Owned HTTP/1.1 loop** (`internal/fastserve`, `CAMPFIRE_SERVER_LOOP=on`,
  default): vectorized request parsing, canonical-header fast path, precomposed
  response heads, one writev per response, deadline calls eliminated where the
  socket state already matches.
- **Whole-response cache** (`CAMPFIRE_RESPONSE_CACHE_MB`, 64 MiB default) for
  room, messages and search: completed responses stored as their exact wire
  bytes; hits re-check the session and the observed SQLite generation before
  serving; direct replay through the precomposed seam removes the header-map
  and body-copy work. Sidebar keeps its own whole-page cache.
- **Direct SQLite lanes** (`internal/fastdb`, CGO): read lane with fused
  single-transition version and session reads; write lane with a batched
  after-commit phase (commits per message drop from ~1.0 to ~0.01 at batch
  scale), group commit, and checkpoints off the writer.
- **Bounded WAL** (`CAMPFIRE_CHECKPOINT_PAGES`, 10,000 frames): a C WAL hook
  tracks frames; the writer runs PASSIVE + RESTART between jobs, so a busy post
  workload holds the WAL at ~40 MB instead of growing unbounded (measured
  4.6 GB in 10 s without it). Refused restarts are counted, backed off and
  never misreported.
- **Cable wake path** (`CAMPFIRE_CABLE_FAST=on`, default): subscriber index,
  exact-payload frame cache, per-client ring with a condition-variable writer,
  batched vectored writes; the unread bump uses an active-subscription index
  and one bulk authorization, with removed members skipped and revoked
  sessions stopped.
- **Runtime**: PGO build (`cmd/campfire/default.pgo`, `-pgo=auto`, falls back
  cleanly when absent) and `CAMPFIRE_GOGC=200` as the shipped default
  (rollback `CAMPFIRE_GOGC=100`); measured GC CPU per request halves, total
  CPU per request −4.9% on the read workload for +13 MiB peak RSS.

## Correctness

- Full `-race` suite (`bin/check`), browser workflows, and the screen parity
  inventory: **178/178 applicable cells match the reference** on pixels and
  accessibility (192 cells, 0 errors).
- Go↔Rust interoperability: Rust-issued sessions accepted by Go and vice
  versa; Rust renders and searches messages Go wrote (FTS).
- Every cache observes external SQLite commits (`PRAGMA data_version` on a
  pinned reader) and re-checks authorization where it serves user data;
  admission happens only under the pre-read generation. Foreign-writer
  revocation regressions exist for the search cache and the cable memo.
- The write path keeps primary-commit-before-callback semantics, per-job
  savepoint isolation, cancellation survival and a shutdown drain; WAL/NORMAL
  durability is unchanged.

## Measurement notes and limitations

- The first run of this comparison accidentally used Rust's pre-response-cache
  revision (`743aa84`, whose numbers match the *old* published table within
  2–3%); this body reports the corrected matched run against `f9dca47`.
- Auxiliary rows (avatar, static CSS, `/up`) and the cable fan-out were not
  re-measured in the matched session; the earlier measurements of those routes
  are recorded in `plans/validation.md` and are not part of the headline claim.
- Known deliberate differences from Rust (strict DOM/network specs, cache
  coherency scope, formatting) are listed under "Known differences" in the
  README.
- `github.com/sebishogun/simdhttp` and `github.com/sebishogun/simd` are
  MIT-licensed modules by this PR's author used only by the owned loop.
