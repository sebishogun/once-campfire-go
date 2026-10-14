# GOGC arms: one binary, read workload

`36bee91926258b819955a419fba3d7546b39e6c8` (`c37c9c490497`), Go 1.27.1, omarchy. Public front listener, gzip, 16 concurrent clients on 12-15, server pinned to 8-11, 1s warmup and 2s samples per route, 18 repetitions per arm, order rotating cyclically each repetition so every arm occupies every slot. Decoded bodies are byte-identical across every run (normalized only for the listener origin and the room page's live cursor). GODEBUG=gctrace=1 in every arm; host load at start: 9.19 13.58 15.02 6/4158 857655.

Arms: `100` (GOGC=100), `null` (GOGC=100), `200` (GOGC=200), `200+256MiB` (GOGC=200, limit=256MiB). An arm with the same GOGC as the baseline and a different label is the host noise floor.

| Route | Arm | req/s, median [min–max] | CPU µs/req | GC CPU µs/req | GC cycles | GC share % | RSS peak MiB, median [min–max] |
|---|---:|---:|---:|---:|---:|---:|---:|
| room_show | 100 | 118,750 [16,224–130,947] | 38.14 | 2.910 | 1944 | 5.0 | 71.4 [67.9–76.3] |
| room_show | null | 126,207 [45,969–133,402] | 36.60 | 2.644 | 2389 | 5.0 | 70.8 [66.7–75.3] |
| room_show | 200 | 115,210 [56,071–140,134] | 35.62 | 1.559 | 1024 | 3.0 | 87.2 [77.3–92.1] |
| room_show | 200+256MiB | 135,793 [66,546–143,891] | 36.00 | 1.494 | 1084 | 3.0 | 86.6 [77.7–88.9] |
| messages_page | 100 | 126,474 [2,093–148,502] | 38.14 | 2.910 | 1944 | 5.0 | 71.4 [67.9–76.3] |
| messages_page | null | 140,793 [47,988–150,484] | 36.60 | 2.644 | 2389 | 5.0 | 70.8 [66.7–75.3] |
| messages_page | 200 | 134,344 [63,755–156,606] | 35.62 | 1.559 | 1024 | 3.0 | 87.2 [77.3–92.1] |
| messages_page | 200+256MiB | 145,470 [64,921–157,770] | 36.00 | 1.494 | 1084 | 3.0 | 86.6 [77.7–88.9] |
| sidebar | 100 | 132,710 [16,121–148,518] | 38.14 | 2.910 | 1944 | 5.0 | 71.4 [67.9–76.3] |
| sidebar | null | 141,518 [61,891–151,710] | 36.60 | 2.644 | 2389 | 5.0 | 70.8 [66.7–75.3] |
| sidebar | 200 | 141,842 [60,829–158,066] | 35.62 | 1.559 | 1024 | 3.0 | 87.2 [77.3–92.1] |
| sidebar | 200+256MiB | 139,290 [19,120–158,222] | 36.00 | 1.494 | 1084 | 3.0 | 86.6 [77.7–88.9] |
| search | 100 | 147,670 [41,881–155,510] | 38.14 | 2.910 | 1944 | 5.0 | 71.4 [67.9–76.3] |
| search | null | 145,720 [55,459–153,535] | 36.60 | 2.644 | 2389 | 5.0 | 70.8 [66.7–75.3] |
| search | 200 | 150,455 [53,267–162,379] | 35.62 | 1.559 | 1024 | 3.0 | 87.2 [77.3–92.1] |
| search | 200+256MiB | 136,198 [6,501–161,109] | 36.00 | 1.494 | 1084 | 3.0 | 86.6 [77.7–88.9] |

## Paired ratios against the baseline (same repetition)

Ratios to the `100` arm measured in the same repetition, and the number of repetitions that favour the arm. Throughput ratio above 1 (or CPU µs/req ratio below 1) with every repetition agreeing is a clear sign; mixed signs mean the host noise is larger than the effect.

| Route | Arm | req/s ratio, median [min–max] | reps >1 | CPU µs/req ratio, median [min–max] | reps <1 |
|---|---:|---:|---:|---:|---:|
| room_show | null | 1.067 [0.382–3.735] | 14/18 | 0.996 [0.835–1.109] | 9/18 |
| room_show | 200 | 1.080 [0.485–4.617] | 12/18 | 0.931 [0.725–1.114] | 15/18 |
| room_show | 200+256MiB | 1.164 [0.556–7.477] | 17/18 | 0.948 [0.699–1.108] | 15/18 |
| messages_page | null | 1.069 [0.382–30.851] | 11/18 | 0.996 [0.835–1.109] | 9/18 |
| messages_page | 200 | 1.066 [0.562–35.354] | 12/18 | 0.931 [0.725–1.114] | 15/18 |
| messages_page | 200+256MiB | 1.167 [0.830–42.668] | 14/18 | 0.948 [0.699–1.108] | 15/18 |
| sidebar | null | 1.035 [0.703–4.245] | 13/18 | 0.996 [0.835–1.109] | 9/18 |
| sidebar | 200 | 1.059 [0.554–6.618] | 14/18 | 0.931 [0.725–1.114] | 15/18 |
| sidebar | 200+256MiB | 1.067 [0.630–5.289] | 13/18 | 0.948 [0.699–1.108] | 15/18 |
| search | null | 1.002 [0.574–2.303] | 10/18 | 0.996 [0.835–1.109] | 9/18 |
| search | 200 | 1.042 [0.504–2.435] | 14/18 | 0.931 [0.725–1.114] | 15/18 |
| search | 200+256MiB | 0.999 [0.093–2.366] | 9/18 | 0.948 [0.699–1.108] | 15/18 |

Per-repetition samples, the GC and startup lines of each server log (with the effective `gc policy` line), decoded body hashes, binary hash, seed hash and host load are in `raw.json`, `metadata.json` and `go*.log`; the per-request access log is left on (harness fidelity) and only its line and byte counts are kept. This is a workstation measurement on a shared host; medians, spread and paired signs are the evidence, not single samples.

