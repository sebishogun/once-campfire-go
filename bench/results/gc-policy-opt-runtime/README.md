# ENGINE-47: `CAMPFIRE_GOGC` 100 vs 200 (one binary, read workload)

**Decision: the shipped image sets `CAMPFIRE_GOGC=200` (Dockerfile `ENV`); `CAMPFIRE_GOGC=100`
rolls it back. No `CAMPFIRE_GOMEMLIMIT` is shipped.**

Reproduce with `bench/gc-policy`; the generated tables are in [`report.md`](report.md), the
per-repetition samples, decoded body hashes and log volumes in [`raw.json`](raw.json), the host
and binary identity in [`metadata.json`](metadata.json), and the GC and startup lines of each
server run in `go*.log`.

```sh
bin/build
bench/gc-policy --values 100 100@null 200 200:256MiB --reps 18 --seconds 2 --warmup 1 \
  --out bench/results/gc-policy-opt-runtime
```

## What was measured

The same binary in every arm; only `CAMPFIRE_GOGC` (and, in one arm, `CAMPFIRE_GOMEMLIMIT`)
differs. Revision `36bee91` (`c37c9c490497`), Go 1.27.1, `AMD RYZEN AI MAX+ 395`, 16 physical
cores. The server is pinned to CPUs 8-11 and the load generator to 12-15 (the shared harness's
pinning), `GOMAXPROCS=4`, against a disposable SQLite backup of
`once-campfire-verification/fixtures/default`. Reads are room_show, messages_page, sidebar and
search at concurrency 16 through the public front listener, gzip requested, 1 s warmup and 2 s
samples per route. Eighteen repetitions per arm, the arm order rotating cyclically so every arm
occupies every slot. `GODEBUG=gctrace=1` is set in every arm and the per-request access log is
left on, so the workload is the shipped composition.

`100@null` is a second, identically configured `GOGC=100` arm: the host's noise floor for the
same statistic. It is the yardstick, not a result.

The decoded body of every route is fetched once per run, normalized only for the listener origin
and the room page's live cursor, and is byte-identical in all 72 runs.

## Numbers (medians over 18 runs; paired ratios are within-repetition)

| Arm | CPU µs/req | GC CPU µs/req | GC cycles | GC share | RSS peak MiB | RSS final MiB |
|---|---:|---:|---:|---:|---:|---:|
| `100` | 38.14 | 2.910 | 1944 | 5% | 71.4 [67.9–76.3] | 67.0 |
| `null` (`GOGC=100`) | 36.60 | 2.644 | 2389 | 5% | 70.8 [66.7–75.3] | 65.5 |
| `200` | 35.62 | 1.559 | 1024 | 3% | 87.2 [77.3–92.1] | 76.8 |
| `200+256MiB` | 36.00 | 1.494 | 1084 | 3% | 86.6 [77.7–88.9] | 76.6 |

| Arm | CPU µs/req ratio vs `100`, median [min–max] | reps <1 | GC CPU µs/req ratio, median | reps <1 |
|---|---:|---:|---:|---:|
| `null` | 0.996 [0.835–1.109] | 9/18 | 0.985 | 12/18 |
| `200` | 0.931 [0.725–1.114] | 15/18 | 0.535 | 17/18 |
| `200+256MiB` | 0.948 [0.699–1.108] | 15/18 | 0.488 | 18/18 |

## Reading it

* **GC work halves, unambiguously.** GC CPU per request falls 2.91 → 1.56 µs (median ratio 0.535,
  17/18 repetitions), GC cycles 1944 → 1024, and the runtime's own GC share 5% → 3%. The
  noise-floor arm sits at 0.985 with 12/18 — that metric does not move with the GC target set to
  the same value.
* **Total CPU per request falls ~5–7%**, from 38.14 to 35.62 µs (median ratio 0.931, 15/18
  repetitions) — less than the 46% drop in GC CPU because GC was only ~8% of total CPU. The
  noise-floor arm sits at 0.996 with 9/18, so the effect is about twice the median offset of
  identical arms. A second, independent 18-repetition run of the same design (three arms:
  `100`, `100@null`, `200`) put the `200`-vs-`100` CPU ratio at 0.952 (16/18) and
  `null`-vs-`100` at 1.002 (8/18).
* **Throughput is not the evidence.** The paired req/s ratios for `200` are positive on three of
  four routes (median 1.04–1.08, 12–14/18), but the identically configured `null` arm is also
  positive on three routes (median 1.00–1.07, 10–14/18) and individual repetitions swing from
  0.09× to 42× when the host stalls the pinned cores. The host was shared and busy (load average
  9–15 at the start of a run, and other work on CPUs 8-15 throughout), so the throughput columns
  cannot separate a 5% effect from the noise. CPU per request is the metric that can, and it
  agrees with the mechanism.
* **The memory cost is real and bounded.** Peak RSS rises 71.4 → 87.2 MiB (+22%) and final RSS
  67.0 → 76.8 MiB, tracking the larger heap goal. That is the price of the CPU win; it is within
  any sane bound for this workload and far below any plausible container limit.
* **`CAMPFIRE_GOMEMLIMIT=256MiB` changes nothing here** (`200+256MiB` vs `200`: CPU 0.948 vs
  0.931, GC CPU 1.494 vs 1.559 µs, peak RSS 86.6 vs 87.2 MiB, all inside the arm-to-arm spread).
  It is non-binding at this peak, so shipping it would add a knob with no measured benefit and a
  real risk of collector thrash in memory-heavy workloads that this read benchmark does not
  exercise. It is left unset; the knob remains available.

## Uncertainty

* The host is shared and busy; only CPU per request and the GC counters are separable from the
  noise at this sample size. A quiet host would resolve the throughput sign directly.
* The measured workload is warm-cache reads with the per-request access log on, matching the
  shared harness. Uploads, video/image processing, Cable fan-out at high client counts and cold
  caches are not represented, and a higher GOGC raises peak memory most where allocation bursts
  are largest.
* The profile's bootstrap is the usual one: the PGO profile (`cmd/campfire/default.pgo`) was
  collected from the binary built before it existed, with `CAMPFIRE_GOGC=200` in effect. It does
  not change the GC comparison, which is one binary against itself.
