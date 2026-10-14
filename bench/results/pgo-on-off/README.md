# ENGINE-47 scope check: PGO on vs off

The GC decision was made on one binary, so it says nothing about whether profile-guided
optimization itself helps. This directory measures that separately with the same harness, the
same aligned-window accounting and the same request windows.

```sh
go build -tags sqlite_fts5 -trimpath -pgo=off  -o .cache/pgo/campfire-nopgo ./cmd/campfire
bin/build                                              # ./campfire, -pgo=auto, the committed profile
bench/gc-policy --binary nopgo=.cache/pgo/campfire-nopgo \
  --values go/200@pgo go/200@pgo-null nopgo/200@nopgo \
  --reps 12 --warmup 2 --seconds 4 --out bench/results/pgo-on-off
```

| Arm | Binary | sha256 | CPU µs/req | GC CPU µs/req | RSS peak MiB |
|---|---|---|---:|---:|---:|
| `pgo` | `./campfire`, `-pgo=auto` | `f3f063bb…` | 23.06 | 0.904 | 86.4 |
| `pgo-null` | same binary again | `f3f063bb…` | 23.70 | 0.931 | 87.7 |
| `nopgo` | `-pgo=off` | `d46650a9…` | 23.22 | 0.858 | 82.5 |

Pooled over all four routes and 12 repetitions (48 pairs per arm), against the `pgo` arm:

| Arm | CPU µs/req ratio, median [min–max] | pairs <1 | req/s ratio, median | pairs >1 |
|---|---:|---:|---:|---:|
| `pgo-null` | 1.007 [0.821–1.138] | 18/48 | 0.996 | 18/48 |
| `nopgo` | 0.995 [0.837–1.195] | 29/48 | 1.011 | 32/48 |

**Result: no detectable difference.** The no-PGO binary's CPU per request ratio is 0.995 — the
same as the identically configured noise-floor arm's 1.007 — with signs split 29/48 and 18/48
against a fair 24/48. Throughput is likewise split (32/48 and 18/48). A 1–2% PGO effect would be
invisible at this sample size on this host; nothing larger is present on this workload.

So the shipped build's PGO input is a *mechanism* that is verified to engage (`go version -m`
reports `build -pgo=default.pgo`) and to leave the decoded response bytes unchanged, not a
measured speedup. The GC default was therefore decided independently of PGO, on a PGO build of
the same source.

Generated tables are in [`report.md`](report.md); per-window samples and per-run totals in
[`raw.json`](raw.json); binary hashes and host identity in [`metadata.json`](metadata.json).
Host load at the start of the run: 6.2 (1 min), shared host, `GODEBUG=gctrace=1` in every arm,
per-request access log left on.

## Provenance of the committed profile

`cmd/campfire/default.pgo.provenance.json` identifies the binary that produced the profile from
that binary's own `go version -m` output (`binary.vcs_revision`, `binary.vcs_modified`,
`binary.build_settings`, `binary.go_version_m`) rather than from the checkout, resolves and hashes
the PGO input the binary was itself built with (`binary_pgo_input`), records the effective runtime
settings from the server's startup log (`effective.gc_percent`, `effective.mem_limit`,
`effective.cpus_allowed`) alongside the requested ones, and pins a content-addressed source
identity for every file that goes into the build (`go_sources_sha256`, 787 files) plus the
checkout's revision and diff hash.

The identification was checked by profiling a binary built at another revision while the checkout
was elsewhere: `bin/pgo-profile --binary /tmp/campfire-pgo …` (a build from `36bee91`, checkout at
`ce7c42f`) reported `binary.vcs_revision = 36bee91…`, `binary.vcs_modified = true` and
`binary.build_settings["-pgo"] = "default.pgo"` — the binary's stamp, not the checkout's — with
`binary_pgo_input.sha256` resolving to the profile that binary had been built with.

One limitation, by construction: the manifest pins the profile file it just wrote (which matches
the committed tree) but cannot pin its own output file, so the pinned hash of
`cmd/campfire/default.pgo.provenance.json` is the previous file's, or absent on the first run.
Everything else in `go_sources_sha256` matches the tree it was collected from.

