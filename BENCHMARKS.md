# Performance evidence

This guide explains what immulog benchmarks measure, how to run them, and how
to interpret the recorded results. Results are host-specific evidence, not
portable throughput guarantees or release thresholds unless explicitly stated.

## Contents

- [Purpose and scope](#purpose-and-scope)
- [Test types and measurement model](#test-types-and-measurement-model)
- [Run the benchmarks](#run-the-benchmarks)
  - [Go microbenchmarks](#go-microbenchmarks)
  - [Soak workloads](#soak-workloads)
  - [Environment and artifacts](#environment-and-artifacts)
- [Recorded results](#recorded-results)
  - [Microbenchmark qualification — 2026-09-26](#microbenchmark-qualification--2026-09-26)
  - [Sustained soak — 2026-09-25](#sustained-soak--2026-09-25)
  - [Microbenchmarks — 2026-09-20](#microbenchmarks--2026-09-20)
- [Comparison and qualification policy](#comparison-and-qualification-policy)

## Purpose and scope

Use benchmarks to detect performance changes and characterize a particular
workload on a particular host. Microbenchmarks isolate append and fetch paths;
soaks exercise the integrated store over time, including consumer progress,
reopen cycles, resource use, and independent correctness oracles. A passing
run only supports claims about its recorded configuration.

The repository currently has two performance test families:

| Family | What it measures | Use it for | It does not establish |
|---|---|---|---|
| `perf/benchmarks` | Durable append and fetch paths with Go benchmark timing, allocations, and throughput | Short, repeatable comparisons of storage/API paths | Whole-system endurance or production capacity |
| `perf/soak` | Integrated append/consumer workload, lag, resource trends, reopen behavior, and oracle verification | Long-run correctness, liveness, and stability under a selected offered load | A universal capacity figure or device-independent guarantee |

The microbenchmarks use normal filesystem-backed operations; they are not
in-memory queue measurements. `mixed` soaks deliberately exercise cancellation,
overload, retention, and membership churn, so their throughput is stress
behavior, not a capacity result. `sustained` disables deliberate overload and
short-deadline cancellation so acknowledged rate and consumer lag can be
interpreted as local throughput evidence.

## Test types and measurement model

`perf/benchmarks` covers:

- `BenchmarkDirectAppendBatch`: batched `Partition.AppendBatch` with different
  record counts and payload sizes.
- `BenchmarkIngressAppend`, `BenchmarkIngressAppendParallel`, and
  `BenchmarkIngressBatchLinger`: ingress append behavior across producer
  counts, payload sizes, and upstream timed acquisition windows.
- `BenchmarkFetch` and `BenchmarkFetchParallel`: segment reads, rebuildable
  tail-cache hits, and concurrent readers.

The append cases include the normal durable write and synchronization path.
Positive `BatchLinger` values affect upstream batch acquisition; they do not
mean immulog performs one fsync for multiple formed batches. Interpret `syncOps`
alongside throughput rather than assuming group-commit savings.

`perf/soak/TestMixedWorkloadSoak` runs append producers against retained and
stable topic fixtures, exercises consumers and commits, and verifies progress
with per-partition oracles. The `mixed` profile adds resilience stress; the
`sustained` profile removes deliberate overload and short-deadline cancellation.
Use `--churn-interval 0` when measuring without deliberate consumer replacement.

Important metric semantics:

- `offered`, `acknowledged`, `unknown`, and `known_rejected` describe append
  outcomes; `cancelled` and `overload_calls` are additional, potentially
  overlapping observations.
- Soak rates use the active `measurement_nanos` interval. `total_nanos` includes
  drain, verification, and cleanup; report the denominator when comparing
  rates.
- `acknowledged_bytes` and `delivered_payload_bytes` count record values only,
  excluding keys and disk framing.
- Soak latency percentiles are upper bounds of configured buckets, not exact
  order statistics. A legacy `0` in a latency field means the open-ended final
  bucket.
- Heap and RSS maxima are periodic samples, so short-lived peaks can be missed.
  RSS includes resident process pages outside the Go heap. Process CPU values
  are Linux clock ticks and process I/O is cumulative `/proc` data, not
  device-wide utilization.
- Soak observer/oracle checks can be skipped when their state is busy; report
  skipped counts and dropped samples with the results.

## Run the benchmarks

### Go microbenchmarks

Use the runner for reproducible profiles. It runs repeated samples, captures
configuration and host metadata, preserves raw output, and returns failure for
a nonzero benchmark or process status.

```sh
perf/benchmarks/run.sh --profile smoke
perf/benchmarks/run.sh --profile standard
perf/benchmarks/run.sh --profile qualification
```

| Profile | Intended use | Time/sample | Samples/process | Independent runs |
|---|---|---:|---:|---:|
| `smoke` | Fast local validation | 1s | 3 | 1 |
| `standard` | Development comparison | 5s | 5 | 1 |
| `qualification` | Repeated comparison evidence | 10s | 10 | 3 |

`qualification` requires a clean worktree unless `--allow-dirty` is supplied;
avoid that override for recorded evidence. Narrow a run to append or fetch
benchmarks, or compare concurrency levels:

```sh
perf/benchmarks/run.sh --profile standard --suite append
perf/benchmarks/run.sh --profile standard --suite fetch --cpu 1,2,4,8
```

Add `--analyze` to write aggregate medians, P10–P90 ranges, coefficient of
variation, throughput, allocations, and variance warnings. Use
`--analyze-only --run-dir PATH` to analyze existing artifacts. Add
`--append-to BENCHMARKS.md` only when intentionally recording a result; the
runner rejects duplicate commit/date entries. Use the direct `go test` command
when developing a benchmark or debugging the runner:

```sh
go test ./perf/benchmarks -run '^$' \
  -bench 'BenchmarkIngressAppendParallel|BenchmarkDirectAppendBatch' \
  -benchmem -benchtime=1s -count=1
```

Append benchmarks should validate the final durable end and report
`producer-records/s` and `producer-bytes/s`; fetch benchmarks report
`consumer-records/s` and `consumer-bytes/s`. Use `b.SetBytes` for Go's standard
bandwidth metric. Keep setup, reopen, retention, and verification outside
steady-state throughput measurements.

### Soak workloads

Use a dedicated data directory and preserve the runner's artifacts. Do not
point a soak at an application data directory. The runner automatically
estimates disk and open-file requirements for long runs; keep those preflights
enabled for sustained and qualification tests.

Quick mixed-profile correctness smoke (resource preflights are intentionally
disabled only for this short local run):

```sh
perf/soak/run.sh \
  --run-dir "$HOME/immulog-soak/mixed-smoke" \
  --profile mixed \
  --duration 20s \
  --timeout 90s \
  --seed 0x5eed5eed \
  --minimum-free-bytes 0 \
  --minimum-open-files 0 \
  --analyze
```

Short sustained-throughput smoke:

```sh
perf/soak/run.sh \
  --run-dir "$HOME/immulog-soak/sustained-smoke" \
  --profile sustained \
  --warmup 30s \
  --duration 20s \
  --timeout 90s \
  --minimum-free-bytes 0 \
  --minimum-open-files 0 \
  --analyze
```

For a rate sweep, use separate directories per offered rate; for repeated
qualification runs, let the runner create isolated runs rather than writing a
shell loop:

```sh
perf/soak/run.sh \
  --run-dir "$HOME/immulog-soak/rate-sweep" \
  --profile sustained \
  --warmup 5m \
  --duration 30m \
  --timeout 40m \
  --seed 0x5eed5eed \
  --rate-sweep 500,1000,1500 \
  --churn-interval 0 \
  --analyze

perf/soak/run.sh \
  --run-dir "$HOME/immulog-soak/qualification" \
  --profile sustained \
  --warmup 5m \
  --duration 4h \
  --timeout 4h30m \
  --seed 0x5eed5eed \
  --producer-rate 1000 \
  --churn-interval 0 \
  --runs 3 \
  --analyze
```

Choose a rate whose sampled backlog slope is not positive and whose delivery
and commit lag remain bounded. The warmup path currently invokes one soak
cycle; with the default 10-minute reopen interval, a requested warmup longer
than that cycle is truncated. Keep warmup shorter than the reopen interval or
set the interval accordingly, and verify actual elapsed warmup from timestamps
rather than trusting `warmup_nanos` alone. A 5-minute warmup above is within the
default interval. For staged consumer-liveness and soak-validation guidance,
see [Soak validation](docs/soak-validation.md).

Use `--producer-rate` for a monotonic aggregate offered-record schedule; zero
or omission selects unlimited producer mode. Use `--sample-interval` and
`--sample-limit` to bound periodic samples. `--analyze` writes a
measurement-aware summary. Analyze a saved report with:

```sh
go run ./perf/analyze --input metrics.json --format markdown
```

`--runs 3` writes `runs.tsv`; rate sweeps write `rates.tsv`. Run
`perf/soak/run.sh --help` for all options.

The runner records the commit, host and workload configuration, resource
preflights, initial/final disk state, test logs, checkpoints, and schema-v2
`metrics.json`. Keep `metrics.json`, `summary.md`, `runs.tsv` or `rates.tsv`,
logs, checkpoints, and environment captures together as the evidence artifact.
The manual workflow in `.github/workflows/performance.yml` can also run the
microbenchmark suite and an optional fixed-seed mixed soak smoke.

### Environment and artifacts

For a meaningful comparison, record date/time and tested commit; CPU model and
logical CPU count; RAM when captured; OS/kernel/architecture; Go version,
`GOOS`, `GOARCH`, `GOAMD64`, and `GOMAXPROCS`; filesystem/device; test command,
duration, sample count, payload/batching, partitions, and operating limits.
Do not infer values missing from an artifact.

A minimal Linux capture is:

```sh
go version
go env GOOS GOARCH GOAMD64
grep -m1 'model name' /proc/cpuinfo
nproc
free -h
uname -a
git status --short
git rev-parse HEAD
ulimit -Sn
ulimit -Hn
df -T /mnt/immulog-benchmarks
```

The soak runner estimates four-hour needs at approximately 24 GiB free space
and 65,536 open files. Long runs can consume multiple GiB and thousands of
files; use a dedicated Linux host or reserved volume without competing load.
Do not disable resource preflights for sustained or qualification runs. A
24-hour soak requires a specifically sized directory and planned resource
limits; it is not a routine local smoke.

## Recorded results

The current evidence sets below are retained here. Older result records were
removed because the benchmark and soak frameworks evolved; they should not be
compared directly with these measurements.

<!-- microbenchmark-evidence:commit=a6955eaf4bb9b38193aba82e3c30429b1b9bbb11,date=2026-09-26 -->

### Microbenchmark qualification — 2026-09-26

Measured from commit `a6955eaf4bb9b38193aba82e3c30429b1b9bbb11` with profile
`qualification`, suite `all`, `10s` per sample, 10 samples per process run,
and 3 independent process runs.

Host: Intel(R) Core(TM) i7-10510U CPU @ 1.80GHz; Go: go1.26.2
linux/amd64; filesystem: ext4.

Values are medians across all captured samples; P10–P90 shows ns/op
variability.

| Benchmark | Samples | Median ns/op | P10–P90 ns/op | CV | Median records/s | Median MB/s | Median B/op | Median allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `BenchmarkDirectAppendBatch/records-1/payload-1024` | 30 | 1469898 | 1458265–1496597 | 3.8% | 680 | 0.70 | 249578 | 8 |
| `BenchmarkDirectAppendBatch/records-1/payload-256` | 30 | 1436918 | 1428521–1482601 | 11.0% | 696 | 0.18 | 249139 | 8 |
| `BenchmarkDirectAppendBatch/records-1/payload-4096` | 30 | 1557966 | 1550032–1640147 | 8.2% | 642 | 2.63 | 254510 | 8 |
| `BenchmarkDirectAppendBatch/records-256/payload-1024` | 30 | 3828418 | 3803540–6085378 | 24.3% | 66868 | 68.47 | 2155948 | 537 |
| `BenchmarkDirectAppendBatch/records-256/payload-256` | 30 | 2158341 | 2140677–3283308 | 32.0% | 118610 | 30.37 | 713256 | 536 |
| `BenchmarkDirectAppendBatch/records-256/payload-4096` | 30 | 9246450 | 8966999–10767911 | 7.1% | 27686 | 113.41 | 9388317 | 540 |
| `BenchmarkDirectAppendBatch/records-64/payload-1024` | 30 | 2068410 | 2031771–2471099 | 10.6% | 30942 | 31.68 | 555450 | 147 |
| `BenchmarkDirectAppendBatch/records-64/payload-256` | 30 | 1659942 | 1635543–1804618 | 10.2% | 38556 | 9.87 | 263006 | 147 |
| `BenchmarkDirectAppendBatch/records-64/payload-4096` | 30 | 3771481 | 3722867–5936747 | 19.8% | 16970 | 69.50 | 2234004 | 149 |
| `BenchmarkDirectAppendBatch/records-8/payload-1024` | 30 | 1608803 | 1598639–1781359 | 9.7% | 4972 | 5.09 | 284946 | 27 |
| `BenchmarkDirectAppendBatch/records-8/payload-256` | 30 | 1524234 | 1516785–1553321 | 8.6% | 5248 | 1.34 | 256290 | 27 |
| `BenchmarkDirectAppendBatch/records-8/payload-4096` | 30 | 1752740 | 1745821–1877810 | 18.3% | 4564 | 18.70 | 333690 | 29 |
| `BenchmarkFetch/segment` | 30 | 75488 | 69075–82564 | 9.1% | 1695682 | 434.10 | 193886 | 258 |
| `BenchmarkFetch/tail` | 30 | 25292 | 23045–30490 | 11.9% | 5060806 | 1295.57 | 49152 | 129 |
| `BenchmarkFetchParallel` | 30 | 93340 | 84440–103118 | 8.0% | 1371328 | 351.06 | 193889 | 258 |
| `BenchmarkIngressAppend` | 30 | 1475554 | 1449119–2103793 | 48.1% | 678 | 0.17 | 248847 | 18 |
| `BenchmarkIngressAppendParallel/producers-1/payload-1024` | 30 | 1507532 | 1500526–1840324 | 8.3% | 663 | 0.68 | 252256 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-256` | 30 | 1484716 | 1473018–1814713 | 9.8% | 674 | 0.17 | 250397 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-4096` | 30 | 1605428 | 1595026–1936826 | 12.8% | 623 | 2.55 | 256964 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-64` | 30 | 1479028 | 1464122–1677297 | 7.1% | 676 | 0.04 | 249102 | 19 |
| `BenchmarkIngressAppendParallel/producers-16/payload-1024` | 30 | 200307 | 197896–203461 | 1.8% | 4992 | 5.11 | 38112 | 12 |
| `BenchmarkIngressAppendParallel/producers-16/payload-256` | 30 | 194310 | 191244–196113 | 3.8% | 5146 | 1.32 | 33213 | 12 |
| `BenchmarkIngressAppendParallel/producers-16/payload-4096` | 30 | 221230 | 219853–247690 | 9.4% | 4520 | 18.52 | 41348 | 12 |
| `BenchmarkIngressAppendParallel/producers-16/payload-64` | 30 | 187316 | 183409–189145 | 3.7% | 5338 | 0.34 | 32447 | 12 |
| `BenchmarkIngressAppendParallel/producers-2/payload-1024` | 30 | 1515632 | 1499925–1788416 | 8.9% | 660 | 0.68 | 249906 | 19 |
| `BenchmarkIngressAppendParallel/producers-2/payload-256` | 30 | 1496285 | 1476482–1773699 | 28.4% | 668 | 0.17 | 250899 | 19 |
| `BenchmarkIngressAppendParallel/producers-2/payload-4096` | 30 | 1579868 | 1559214–1740160 | 6.5% | 633 | 2.59 | 260177 | 19 |
| `BenchmarkIngressAppendParallel/producers-2/payload-64` | 30 | 1463646 | 1433446–1731575 | 8.0% | 683 | 0.04 | 255653 | 19 |
| `BenchmarkIngressAppendParallel/producers-32/payload-1024` | 30 | 102532 | 101296–106814 | 6.3% | 9753 | 9.98 | 17882 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-256` | 30 | 98716 | 97470–100957 | 6.5% | 10130 | 2.59 | 18304 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-4096` | 30 | 133388 | 129732–181146 | 15.1% | 7497 | 30.70 | 39681 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-64` | 30 | 95028 | 94166–96321 | 4.0% | 10524 | 0.68 | 16934 | 11 |
| `BenchmarkIngressAppendParallel/producers-4/payload-1024` | 30 | 770107 | 763627–780886 | 2.2% | 1298 | 1.33 | 124006 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-256` | 30 | 744782 | 736683–748281 | 2.1% | 1342 | 0.34 | 123773 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-4096` | 30 | 821448 | 815082–843337 | 2.2% | 1218 | 4.99 | 137984 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-64` | 30 | 736246 | 731133–742983 | 0.7% | 1358 | 0.09 | 123749 | 16 |
| `BenchmarkIngressAppendParallel/producers-64/payload-1024` | 30 | 56582 | 55879–77486 | 15.2% | 17673 | 18.10 | 12346 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-256` | 30 | 51143 | 50491–55466 | 12.0% | 19553 | 5.00 | 9954 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-4096` | 30 | 89432 | 87788–104979 | 12.4% | 11182 | 45.80 | 39146 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-64` | 30 | 48646 | 48224–51092 | 4.2% | 20556 | 1.32 | 9339 | 11 |
| `BenchmarkIngressAppendParallel/producers-8/payload-1024` | 30 | 395438 | 389076–399602 | 2.2% | 2528 | 2.59 | 67820 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-256` | 30 | 377019 | 370629–382758 | 3.1% | 2652 | 0.68 | 63645 | 13 |
| `BenchmarkIngressAppendParallel/producers-8/payload-4096` | 30 | 403221 | 400230–443961 | 4.5% | 2480 | 10.16 | 62570 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-64` | 30 | 370356 | 366462–375984 | 1.9% | 2700 | 0.17 | 63492 | 13 |
| `BenchmarkIngressBatchLinger/producers-32/linger-0s` | 30 | 103082 | 101979–111510 | 13.4% | 9701 | 9.93 | 17820 | 12 |
| `BenchmarkIngressBatchLinger/producers-32/linger-100µs` | 30 | 94352 | 93125–102701 | 17.3% | 10598 | 10.85 | 12970 | 11 |
| `BenchmarkIngressBatchLinger/producers-32/linger-1ms` | 30 | 97243 | 96232–97750 | 8.6% | 10284 | 10.53 | 12744 | 11 |
| `BenchmarkIngressBatchLinger/producers-32/linger-5ms` | 30 | 258446 | 236800–278026 | 10.3% | 3870 | 3.96 | 12304 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-0s` | 30 | 56881 | 56142–74859 | 15.2% | 17580 | 18.01 | 12340 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-100µs` | 30 | 50860 | 48280–55050 | 12.1% | 19662 | 20.13 | 11436 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-1ms` | 30 | 55080 | 53887–61696 | 12.7% | 18156 | 18.59 | 10869 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-5ms` | 30 | 121676 | 120307–127978 | 5.6% | 8218 | 8.42 | 10813 | 11 |
| `BenchmarkIngressBatchLinger/producers-8/linger-0s` | 30 | 393550 | 389926–399035 | 8.4% | 2541 | 2.60 | 67979 | 14 |
| `BenchmarkIngressBatchLinger/producers-8/linger-100µs` | 30 | 355205 | 346607–358971 | 1.5% | 2815 | 2.88 | 27058 | 13 |
| `BenchmarkIngressBatchLinger/producers-8/linger-1ms` | 30 | 359339 | 350110–364988 | 7.4% | 2783 | 2.84 | 25724 | 13 |
| `BenchmarkIngressBatchLinger/producers-8/linger-5ms` | 30 | 1008902 | 937365–1041529 | 4.3% | 991 | 1.02 | 14731 | 13 |

Variance notes:

- `BenchmarkDirectAppendBatch/records-1/payload-256`: 11.0% CV.
- `BenchmarkDirectAppendBatch/records-256/payload-1024`: 24.3% CV.
- `BenchmarkDirectAppendBatch/records-256/payload-256`: 32.0% CV.
- `BenchmarkDirectAppendBatch/records-64/payload-1024`: 10.6% CV.
- `BenchmarkDirectAppendBatch/records-64/payload-256`: 10.2% CV.
- `BenchmarkDirectAppendBatch/records-64/payload-4096`: 19.8% CV.
- `BenchmarkDirectAppendBatch/records-8/payload-4096`: 18.3% CV.
- `BenchmarkFetch/tail`: 11.9% CV.
- `BenchmarkIngressAppend`: 48.1% CV.
- `BenchmarkIngressAppendParallel/producers-1/payload-4096`: 12.8% CV.
- `BenchmarkIngressAppendParallel/producers-2/payload-256`: 28.4% CV.
- `BenchmarkIngressAppendParallel/producers-32/payload-4096`: 15.1% CV.
- `BenchmarkIngressAppendParallel/producers-64/payload-1024`: 15.2% CV.
- `BenchmarkIngressAppendParallel/producers-64/payload-256`: 12.0% CV.
- `BenchmarkIngressAppendParallel/producers-64/payload-4096`: 12.4% CV.
- `BenchmarkIngressBatchLinger/producers-32/linger-0s`: 13.4% CV.
- `BenchmarkIngressBatchLinger/producers-32/linger-100µs`: 17.3% CV.
- `BenchmarkIngressBatchLinger/producers-32/linger-5ms`: 10.3% CV.
- `BenchmarkIngressBatchLinger/producers-64/linger-0s`: 15.2% CV.
- `BenchmarkIngressBatchLinger/producers-64/linger-100µs`: 12.1% CV.
- `BenchmarkIngressBatchLinger/producers-64/linger-1ms`: 12.7% CV.

The raw run artifacts and machine-readable analysis are preserved in the
benchmark run directory. This is host-specific evidence, not a portable
throughput guarantee.

### Sustained soak — 2026-09-25

| Environment | Value |
|---|---|
| Run interval | 2026-09-25 06:01:46–18:31:58 (+05:30) |
| Commit / branch | `74eae1fe33bfd3ed3331a0d5c8655c571a19d16f` / `main` |
| Processor | Intel Core i7-10510U @ 1.80GHz |
| CPU count | 8 logical CPUs; physical core count not captured in run artifacts |
| Memory | 15 GiB reported on the same host after the run; total RAM not captured with the run |
| OS / kernel | Linux `7.0.0-31-generic` |
| Architecture | `amd64`, `GOAMD64=v1` |
| Go | `1.26.2` |
| `GOMAXPROCS` / worktree status | Not captured |

Configuration: three isolated sustained-profile runs, four-hour duration,
30-minute configured warmup, 10-minute reopen interval, seed `0x5eed5eed`,
1,000 records/s aggregate producer-rate target, and `--churn-interval 0`.
Each exited with status 0; the analyzer marked all three metrics reports valid.

| Run | Wall interval (+05:30) | Active measurement | Offered records/s | Acknowledged records/s | Backlog start → end (slope/s) | Delivery/commit lag avg/max | Max open files | Max HeapAlloc | Max RSS |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 06:01:46–10:11:49 | 3h44m48s | 947.58 | 947.57 | 0 → 0 (0.00) | 5/814; 6/814 | 531 | 1,155,647,448 B | 1,581,441,024 B |
| 2 | 10:11:49–14:21:54 | 3h40m41s | 945.36 | 945.35 | 0 → 0 (0.00) | 6/808; 6/808 | 530 | 1,149,460,656 B | 1,462,484,088 B |
| 3 | 14:21:54–18:31:58 | 3h41m31s | 949.99 | 949.98 | 0 → 0 (0.00) | 6/934; 6/934 | 531 | 1,140,586,048 B | 1,475,399,680 B |

Lag values are records: average/max delivery followed by average/max commit.
Each run had zero assignment losses, zero dropped samples, and zero overload
calls. Stable-topic oracles verified through their next offset with no
retention skips; retained-topic oracles passed with the expected retention
accounting. Unknown append outcomes were 111, 104, and 123; known rejections
were 2, 1, and 5, respectively.

The three measured acknowledgement rates vary by less than 0.5%, with no
sampled stable-consumer backlog growth and a flat open-file high-water mark.
The producer schedule realized approximately 945–950 offers/s, below its
1,000 records/s target, so this is evidence of stable operation near 950
records/s—not a qualification at a realized 1,000 records/s or a capacity
ceiling. Measurement-only rates exclude reopen-cycle drain, verification, and
cleanup time. Each run occupied about 4h10m wall time: approximately 10m of
actual warmup followed by the configured four-hour test interval.

**Warmup caveat:** although the configuration requests 30 minutes, the warmup
path invokes `runSoakCycle` only once, and that cycle returns at the 10-minute
reopen interval. The recorded wall intervals are consistent with about 10
minutes of actual warmup per run; `warmup_nanos` records the configured 30
minutes, not elapsed warmup. Treat these as evidence with approximately 10
minutes of warmup, not as satisfying the documented 30-minute qualification
setting. The runner did not capture a worktree diff, so the recorded commit
alone does not prove the worktree was clean. These are repeatable local
stability results for this offered load, not a portable throughput guarantee
or a maximum-capacity result.

<!-- microbenchmark-evidence:commit=ec3c3c9058b733c3018360c2a05cbca54e2d56a2,date=2026-09-20 -->

### Microbenchmarks — 2026-09-20

Configuration: `qualification` profile, `all` suite, 10 seconds per sample,
10 samples per process run, and 3 independent runs (30 samples per benchmark).

| Environment | Value |
|---|---|
| Run date | 2026-09-20; exact start/end times not captured |
| Commit | `ec3c3c9058b733c3018360c2a05cbca54e2d56a2` |
| Processor | Intel Core i7-10510U @ 1.80GHz |
| CPU count / memory | Not captured |
| OS / kernel | Linux; kernel version not captured |
| Architecture | `amd64` |
| Go | `1.26.2` |
| `GOMAXPROCS` / artifact directory | Not captured |

The detailed matrix is preserved below; medians and P10–P90 ranges are in
ns/op.

<details>
<summary>Full microbenchmark results, grouped by test type</summary>

#### Durable append batches

| Benchmark | Median ns/op | P10–P90 ns/op | CV | Records/s | MB/s | B/op | Allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| `BenchmarkDirectAppendBatch/records-1/payload-1024` | 2494053 | 2394313–2702143 | 5.3% | 401 | 0.41 | 150213 | 8 |
| `BenchmarkDirectAppendBatch/records-1/payload-256` | 2440842 | 2106755–2662601 | 8.1% | 410 | 0.11 | 148162 | 8 |
| `BenchmarkDirectAppendBatch/records-1/payload-4096` | 2720244 | 2602843–2876461 | 4.4% | 368 | 1.50 | 154176 | 8 |
| `BenchmarkDirectAppendBatch/records-8/payload-1024` | 2774228 | 2664853–2933735 | 3.4% | 2884 | 2.96 | 190794 | 27 |
| `BenchmarkDirectAppendBatch/records-8/payload-256` | 2694334 | 2506570–2809474 | 4.7% | 2970 | 0.76 | 155203 | 27 |
| `BenchmarkDirectAppendBatch/records-8/payload-4096` | 3037770 | 2972514–3171962 | 3.6% | 2634 | 10.79 | 335370 | 29 |
| `BenchmarkDirectAppendBatch/records-64/payload-1024` | 3596428 | 3489084–3717088 | 5.8% | 17795 | 18.23 | 553984 | 147 |
| `BenchmarkDirectAppendBatch/records-64/payload-256` | 2922130 | 2803814–3060776 | 5.0% | 21902 | 5.61 | 255888 | 147 |
| `BenchmarkDirectAppendBatch/records-64/payload-4096` | 6857912 | 6434381–7557709 | 5.8% | 9332 | 38.23 | 2233903 | 149 |
| `BenchmarkDirectAppendBatch/records-256/payload-1024` | 6605176 | 6289769–6967175 | 7.4% | 38758 | 39.69 | 2155895 | 537 |
| `BenchmarkDirectAppendBatch/records-256/payload-256` | 3884090 | 3757449–3969618 | 7.7% | 65910 | 16.88 | 712332 | 536 |
| `BenchmarkDirectAppendBatch/records-256/payload-4096` | 15117935 | 13645672–16905541 | 10.2% | 16934 | 69.36 | 9388302 | 540 |

#### Ingress append and batching

| Benchmark | Median ns/op | P10–P90 ns/op | CV | Records/s | MB/s | B/op | Allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| `BenchmarkIngressAppend` | 2471864 | 2253702–2950169 | 30.9% | 405 | 0.10 | 154160 | 18 |
| `BenchmarkIngressAppendParallel/producers-1/payload-1024` | 2501011 | 2266692–2669665 | 7.7% | 400 | 0.41 | 162426 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-256` | 2509468 | 2252242–2619869 | 6.8% | 398 | 0.10 | 149182 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-4096` | 2756686 | 2687322–3068664 | 7.6% | 363 | 1.49 | 160270 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-64` | 2454646 | 2291010–2608554 | 14.4% | 407 | 0.03 | 150932 | 19 |
| `BenchmarkIngressAppendParallel/producers-2/payload-1024` | 2366620 | 2216157–2654618 | 8.2% | 423 | 0.43 | 165325 | 18 |
| `BenchmarkIngressAppendParallel/producers-2/payload-256` | 2481281 | 2158281–2646298 | 9.0% | 403 | 0.10 | 144556 | 18 |
| `BenchmarkIngressAppendParallel/producers-2/payload-4096` | 2767639 | 2569941–2865954 | 9.2% | 361 | 1.48 | 157154 | 18 |
| `BenchmarkIngressAppendParallel/producers-2/payload-64` | 2574728 | 2235198–2704992 | 8.2% | 388 | 0.02 | 143375 | 18 |
| `BenchmarkIngressAppendParallel/producers-4/payload-1024` | 1331824 | 1257714–1372258 | 6.2% | 751 | 0.77 | 77300 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-256` | 1258901 | 1190754–1310947 | 6.8% | 794 | 0.20 | 76562 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-4096` | 1428854 | 1332380–1466801 | 5.5% | 700 | 2.87 | 92776 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-64` | 1230388 | 1113310–1284650 | 7.4% | 813 | 0.05 | 76434 | 16 |
| `BenchmarkIngressAppendParallel/producers-8/payload-1024` | 662081 | 597347–692491 | 5.7% | 1510 | 1.54 | 44809 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-256` | 639389 | 567230–654305 | 6.0% | 1564 | 0.40 | 39050 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-4096` | 720278 | 625049–762012 | 8.0% | 1388 | 5.69 | 62670 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-64` | 631138 | 593207–649056 | 5.4% | 1584 | 0.10 | 38222 | 13 |
| `BenchmarkIngressAppendParallel/producers-16/payload-1024` | 356830 | 328803–362407 | 5.4% | 2802 | 2.87 | 26662 | 12 |
| `BenchmarkIngressAppendParallel/producers-16/payload-256` | 324390 | 298070–342996 | 6.3% | 3083 | 0.79 | 21727 | 12 |
| `BenchmarkIngressAppendParallel/producers-16/payload-4096` | 394146 | 346786–415153 | 7.1% | 2537 | 10.39 | 42857 | 13 |
| `BenchmarkIngressAppendParallel/producers-16/payload-64` | 310300 | 286325–324843 | 5.6% | 3222 | 0.21 | 20784 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-1024` | 191646 | 155893–195014 | 9.5% | 5218 | 5.35 | 17557 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-256` | 174432 | 164019–179022 | 4.2% | 5732 | 1.47 | 12677 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-4096` | 257772 | 219475–268241 | 8.8% | 3880 | 15.89 | 38873 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-64` | 157700 | 142525–167333 | 5.7% | 6342 | 0.41 | 11418 | 12 |
| `BenchmarkIngressAppendParallel/producers-64/payload-1024` | 109257 | 104004–114618 | 6.2% | 9152 | 9.37 | 12321 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-256` | 94140 | 87856–96637 | 5.5% | 10622 | 2.72 | 8142 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-4096` | 174967 | 149076–204356 | 13.1% | 5716 | 23.41 | 38914 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-64` | 87433 | 81427–90039 | 4.0% | 11437 | 0.73 | 6509 | 11 |
| `BenchmarkIngressBatchLinger/producers-8/linger-0s` | 693260 | 650964–706344 | 5.4% | 1442 | 1.48 | 43005 | 14 |
| `BenchmarkIngressBatchLinger/producers-8/linger-100µs` | 554967 | 535711–572739 | 4.9% | 1802 | 1.85 | 28180 | 14 |
| `BenchmarkIngressBatchLinger/producers-8/linger-1ms` | 545846 | 523767–556798 | 3.6% | 1832 | 1.88 | 21088 | 13 |
| `BenchmarkIngressBatchLinger/producers-8/linger-5ms` | 1053090 | 1007189–1062247 | 2.7% | 950 | 0.97 | 14727 | 13 |
| `BenchmarkIngressBatchLinger/producers-32/linger-0s` | 194161 | 183450–197295 | 3.4% | 5150 | 5.28 | 17696 | 12 |
| `BenchmarkIngressBatchLinger/producers-32/linger-100µs` | 189748 | 175319–207856 | 7.5% | 5270 | 5.39 | 16324 | 12 |
| `BenchmarkIngressBatchLinger/producers-32/linger-1ms` | 166977 | 163063–175590 | 5.0% | 5989 | 6.13 | 12708 | 11 |
| `BenchmarkIngressBatchLinger/producers-32/linger-5ms` | 287860 | 281295–295272 | 1.8% | 3474 | 3.56 | 12107 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-0s` | 106961 | 94756–112957 | 7.0% | 9350 | 9.57 | 12392 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-100µs` | 118096 | 96391–127641 | 10.2% | 8468 | 8.67 | 12224 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-1ms` | 101454 | 90021–108589 | 10.5% | 9858 | 10.09 | 10975 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-5ms` | 156096 | 143661–157878 | 4.2% | 6406 | 6.56 | 10828 | 11 |

#### Fetch

| Benchmark | Median ns/op | P10–P90 ns/op | CV | Records/s | MB/s | B/op | Allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| `BenchmarkFetch/segment` | 72808 | 65558–85533 | 10.4% | 1758120 | 450.08 | 193886 | 258 |
| `BenchmarkFetch/tail` | 23482 | 22698–29698 | 12.1% | 5450936 | 1395.44 | 49152 | 129 |
| `BenchmarkFetchParallel` | 96040 | 83571–100636 | 11.4% | 1332785 | 341.19 | 193888 | 258 |

Highest coefficient-of-variation cases were `BenchmarkIngressAppend` (30.9%),
`BenchmarkIngressAppendParallel/producers-1/payload-64` (14.4%),
`BenchmarkIngressAppendParallel/producers-64/payload-4096` (13.1%),
`BenchmarkFetch/tail` (12.1%), `BenchmarkFetchParallel` (11.4%),
`BenchmarkIngressBatchLinger/producers-64/linger-1ms` (10.5%),
`BenchmarkFetch/segment` (10.4%),
`BenchmarkDirectAppendBatch/records-256/payload-4096` (10.2%), and
`BenchmarkIngressBatchLinger/producers-64/linger-100µs` (10.2%).

</details>

## Comparison and qualification policy

1. Compare the same benchmark selection, host, filesystem, payload, limits, Go
   toolchain, `GOMAXPROCS`, duration, and sample count. Do not compare results
   from different test-framework generations as if they were a baseline pair.
2. Prefer repeated samples and report medians/ranges rather than a single best
   run. Keep the tested commit and complete run artifacts with each entry.
3. Accept a sustained soak as stable evidence only when every run succeeds,
   samples are not dropped, oracles pass, assignment loss is zero, backlog
   slope is not positive, and delivery/commit lag remain bounded with resource
   headroom.
4. Treat a passing soak as correctness/stability evidence for its workload,
   not proof of power-loss safety, portability, storage-device independence, or
   production capacity.

Keep the data directory, logs, checkpoints, and generated profiles out of the
repository. Store run evidence separately and add only concise, dated summaries
here; retain newest results first within each test type.
