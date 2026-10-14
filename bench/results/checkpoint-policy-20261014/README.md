# WAL checkpoint policy measurement (2026-10-14, Lever 4)

Development measurement on branch `opt-p3-checkpoint` (worktree
`once-campfire-go-opt9`, from merged train `36bee91`). Native binary, a copy of
the harness seed (`once-campfire-rust/parity/.seed/default`), the harness's own
load generator (`bench/loadgen`) driving `post_message` against the native
server on CPUs 8–11 with the load generator on 12–15. Storage on btrfs (the
harness's own filesystem); the host was busy throughout (1-minute load average
12–20 on 32 CPUs), so runs are reported as repeated interleaved pairs rather
than a single number.

`CAMPFIRE_CHECKPOINT_TELEMETRY=1` was set for both arms; the counters it logs
are always kept (one store per checkpoint) and cost nothing on the request
path.

## Telemetry design

Per PASSIVE checkpoint the checkpointer records, in atomics (no lock on either
side, no per-request work):

- the WAL frame count at the checkpoint (`PRAGMA wal_checkpoint(PASSIVE)`'s
  second column) and its maximum;
- the frames this checkpoint moved (the change in the third column, the
  backfill count — a WAL restart resets it) and the total moved;
- the backlog a reader is still pinning (`frames - backfilled`) and its
  maximum;
- the checkpoint duration, total and maximum;
- the time since the previous checkpoint;
- the `-wal` file size (the WAL's on-disk high-water) and its maximum;
- the busy count (a checkpoint SQLite could not finish).

The write lane separately counts WAL restarts. `CAMPFIRE_CHECKPOINT_TELEMETRY`
prints one line per checkpoint, one per restart and a summary at shutdown; an
in-process snapshot is asserted by the tests.

## What the numbers say

**The fixed one-second interval is not the cost.** Across every run the
checkpointer kept the database backfilled: backlog was 0–15 frames (of ~0.8–1.3
million in the WAL), and the checkpointer's total work over a ten-second window
was 67–451 ms of wall time (a few percent of one core, on the checkpointer's own
goroutine). So changing *when* the interval checkpoints does not buy anything on
btrfs.

**The WAL file is unbounded, and that is the real finding.** With only the
timed PASSIVE checkpoints the `-wal` file grows monotonically for the whole run:
**4.5–5.2 GB in ten seconds** at 8.4 K posts/s (2.8–3.2 GB at 3.2 K posts/s with
one poster). PASSIVE backfills every frame but never resets the WAL, and
SQLite's own automatic restart (`walRestartLog`) needs a window with no readers
in the WAL read slots — with the read pool active on every request, that window
never comes. Over a long-running server this grows without bound (hundreds of
MB/s) until the disk fills.

## A/B: timed PASSIVE only (`CAMPFIRE_CHECKPOINT_PAGES=0`) vs growth ceiling (default 10000 frames)

Interleaved pairs, ten seconds each, c=16, medians of three reps. Raw lines:
[conc16.txt](conc16.txt), [conc1.txt](conc1.txt).

| Arm | req/s | p50 | p99 | WAL max | writer restarts / 10 s | checkpointer total |
|---|---:|---:|---:|---:|---:|---:|
| timed PASSIVE only | 8426 | 1.92 ms | 4.94 ms | **4629 MB** | 0 | 298 ms |
| growth ceiling (10000) | **8711** | **1.71 ms** | 9.76 ms | **39.9 MB** | 143 | 123 ms |

Single poster (c=1, two reps): 3168–3232 → 3760–3796 req/s, p50 0.27 → 0.23 ms,
p99 0.67 → 0.48 ms, WAL 2.8–2.9 GB → 39.4 MB.

So the growth ceiling bounds the WAL by ~115× at no measured throughput cost —
the c=16 and c=1 medians both move slightly *in its favour*, within this host's
noise — and it pays about 5 ms of p99 at c=16, where the writer's restart
(PASSIVE then RESTART, waiting for readers) briefly holds the lane. The
checkpointer's own work also drops, because it backfills the same frames in
smaller batches.

## Decision

Change the policy. The timed interval stays as the fallback (the task's "no win
by delaying writeback beyond the timed interval": the ceiling only fires
*earlier*), and the growth ceiling is the new primary schedule. The rollback
switch is `CAMPFIRE_CHECKPOINT_PAGES=0`, which restores the pre-change
behaviour exactly (no restarts, unbounded WAL). The ceiling value follows the
Rust reference's `WAL_LIMIT_PAGES` (10000); 40000 frames (165 MB) was also
measured and showed no advantage beyond a larger bound, so the tighter
reference value is kept.

## Limitations

- One host, one seed, one workload shape (posts). Read-only workloads never
  grow the WAL, so the ceiling never fires there.
- The host was busy; req/s varies by ±5% between adjacent runs, so the
  throughput comparison is directional. The WAL bound is not: it is structural
  (the file stops growing at the ceiling) and reproducible.
- The in-process test asserts the bound with the same mechanism
  (`TestWALStaysBoundedUnderSustainedWrites`); the native runs confirm it on
  btrfs with the real server.
