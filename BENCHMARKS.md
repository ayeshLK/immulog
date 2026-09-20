# Performance evidence

This document is the repository's guide to running and interpreting immulog
performance measurements. Results are dated, machine-specific evidence. They
are not portable throughput guarantees and are not release thresholds unless a
separate qualification record explicitly says otherwise.

## Benchmark inventory

### Durable append benchmarks

The `perf/benchmarks` package covers separate workload classes:

- `BenchmarkIngressAppend` measures one-record durable append latency through
  `Partition.Append`.
- `BenchmarkIngressAppendParallel` varies producer count and payload size while
  exercising the bounded ingress ring and terminal writer.
- `BenchmarkIngressBatchLinger` compares zero and positive upstream timed
  acquisition windows across producer counts.
- `BenchmarkDirectAppendBatch` varies records per batch and payload size through
  `Partition.AppendBatch`.
- `BenchmarkFetch` compares segment reads with rebuildable tail-cache hits.
- `BenchmarkFetchParallel` exercises concurrent independent readers.

Append cases use 64 MiB segments for steady-state throughput unless a benchmark
explicitly targets segment rolling. Each operation includes the normal
filesystem-backed append and synchronization path; these are not in-memory
queue measurements. Results must distinguish per-record durable latency from
batched records/sec and encoded MiB/sec. Positive `BatchLinger` values are
lib-disruptor timed acquisition windows; the writer still synchronizes each
formed immulog batch, so this benchmark must report `syncOps` rather than imply
group-commit fsync reduction.

### Mixed workload soak

`perf/soak/TestMixedWorkloadSoak` is an opt-in load harness. It combines
concurrent appends, fetch and consumer polling, commits, cancellation,
overload, retention, group membership replacement, snapshots, clean reopen
cycles, and independent per-partition oracles. It also samples bounded
runtime and storage diagnostics.

The soak's `acknowledged`, `unknown`, and `known_rejected` counters partition
the offered append attempts. `cancelled` and `overload_calls` are additional
observations and may overlap those outcome counters. The soak also samples
consumer delivery/commit lag, process RSS/I/O/CPU ticks, and append-to-scan and
append-to-consumer timing for acknowledged records.

## Reproduce locally

Use the benchmark runner rather than writing a shell loop. It selects sensible
Go benchmark settings, repeats samples, captures the tested commit and host
metadata, and preserves raw output under a dedicated run directory:

```sh
perf/benchmarks/run.sh --profile smoke
perf/benchmarks/run.sh --profile standard
perf/benchmarks/run.sh --profile qualification
```

The profiles are intentionally explicit:

| Profile | Use | Duration | Samples | Independent runs |
|---|---|---:|---:|---:|
| `smoke` | Fast local validation | 1s | 3 | 1 |
| `standard` | Development comparison | 5s | 5 | 1 |
| `qualification` | Release-quality evidence | 10s | 10 | 3 |

`qualification` requires a clean worktree unless `--allow-dirty` is supplied.
Use `--suite append` or `--suite fetch` to narrow the workload, and use
`--cpu 1,2,4,8` when comparing concurrency scaling:

```sh
perf/benchmarks/run.sh --profile standard --suite append
perf/benchmarks/run.sh --profile standard --suite fetch --cpu 1,2,4,8
```

Use `--analyze` to add `summary.md` and `summary.json` with aggregate
medians, P10–P90 ranges, coefficient of variation, throughput, allocations,
and high-variance warnings. Use `--analyze-only --run-dir PATH` to analyze an
existing artifact without rerunning benchmarks. To deliberately record a
successful report in this document, add `--append-to BENCHMARKS.md`; duplicate
commit/date entries are rejected.

The runner's `summary.tsv`, `configuration.txt`, `environment.txt`,
`command.txt`, `run-*.txt`, `summary.md`, and `summary.json` files are the
evidence artifact. A nonzero benchmark or process run status causes the
runner to fail. Use the direct Go
command only when developing a benchmark or debugging the runner:

```sh
go test ./perf/benchmarks -run '^$' -bench 'BenchmarkIngressAppendParallel|BenchmarkDirectAppendBatch' -benchmem -benchtime=1s -count=1
```

Every benchmark must validate the final durable end and report its workload
parameters. Append cases report explicit `producer-records/s` and
`producer-bytes/s`; fetch cases report `consumer-records/s` and
`consumer-bytes/s`. Use `b.SetBytes` for the standard Go bandwidth metric and
report records/sec for record throughput. Treat setup, reopen, retention, and
verification as separate lifecycle measurements rather than including them in
steady-state throughput.

The manual GitHub Actions workflow in
`.github/workflows/performance.yml` accepts `benchtime` and `benchmark_count`
inputs, records the Go and host environment, and uploads the raw output as an
artifact. It can optionally run the fixed-seed mixed-workload smoke and upload
its log, checkpoint, environment, and `metrics.json` sidecar. Its default is
five one-second microbenchmark samples with the soak smoke disabled.

Run a short, fixed-seed workload smoke with verbose metrics:

```sh
perf/soak/run.sh \
  --duration 20s \
  --timeout 90s \
  --seed 0x5eed5eed \
  --minimum-free-bytes 0 \
  --minimum-open-files 0
```

For a local durable-throughput smoke without deliberate overload injection:

```sh
perf/soak/run.sh \
  --profile sustained \
  --duration 20s \
  --timeout 90s \
  --minimum-free-bytes 0 \
  --minimum-open-files 0
```

The configurable `perf/soak/run.sh` runner captures the tested commit and
host environment, writes the test log and checkpoint under a dedicated run
directory, and preserves the Go test exit status. Its defaults are a four-hour
duration, the fixed seed above, a 10-minute reopen interval, a 20ms append
interval, and duration-aware automatic resource estimates. The estimate uses
1.5 MiB/s of growth plus a 2 GiB reserve and 2.5 log segments/second with
headroom; for four hours this is approximately 24 GiB and 65,536 files. The
`mixed` profile retains the cancellation/overload stress behavior used by the
correctness soak. The `sustained` profile disables deliberate short-deadline
and overload injection so acknowledged throughput and lag can be evaluated as
a local durable-throughput workload. Membership replacement remains enabled
at a 750ms interval by default in both profiles; use
`--churn-interval 0` for a consumer-throughput control run without deliberate
assignment churn. Use `--warmup 30s` to exercise a separate pre-measurement
workload that is excluded from reported counters and rates. Use
`--producer-rate` to apply a monotonic aggregate offered
records-per-second schedule; omit it or use zero for unlimited stress mode. Use
`--sample-interval` and `--sample-limit` to control bounded backlog/resource
samples, and `--analyze` to write a measurement-aware Markdown summary. Use
`--rate-sweep 500,1000,1500` for isolated runs, one dedicated directory per
rate. Use `perf/soak/run.sh --help` to view all duration, seed, interval,
resource, timeout, and path options. Numeric resource values override
the estimates and zero disables a preflight. The runner records
initial/final data size, free space, the churn interval, producer-rate controls,
and the process open-file limit, and writes a machine-readable schema-v2
`metrics.json` sidecar containing phase durations, counters, bounded samples,
resource observations, aggregate and per-partition
delivery records/bytes/rates and lag, oracle results, and latency bucket data.
`--runs 3` automatically executes isolated runs and writes `runs.tsv`; it
replaces a user-side shell loop. Rate sweeps similarly write `rates.tsv`.

Recommended soak evidence sets:

| Set | Configuration | Purpose |
|---|---|---|
| Smoke | `mixed`, 20s, resource preflights disabled | Fast correctness check |
| Sustained sweep | `sustained`, 5m warmup, 30m per rate, `--churn-interval 0`, `--rate-sweep 500,1000,1500` | Find a stable offered-rate range |
| Qualification | `sustained`, 30m warmup, 4h, selected `--producer-rate`, `--churn-interval 0`, `--runs 3` | Repeatable capacity and liveness evidence |

Use the sweep to select a rate before starting qualification. Keep automatic
resource preflights enabled for sustained and qualification runs; only the
short smoke intentionally disables them.

### Production-grade qualification setups

Run qualification on a dedicated Linux host or reserved storage volume with
no competing workload. Start from a clean, recorded commit, use a dedicated
data directory, keep the runner's automatic free-space and open-file
preflights enabled, and preserve the complete run directory. Do not point the
soak at an application data directory.

Capture the host limits before starting:

```sh
git status --short
git rev-parse HEAD
ulimit -Sn
ulimit -Hn
df -T /mnt/immulog-benchmarks
```

First identify a sustainable offered rate. The following is a discovery sweep,
not final capacity evidence:

```sh
perf/soak/run.sh \
  --run-dir /mnt/immulog-benchmarks/rate-sweep \
  --profile sustained \
  --warmup 5m \
  --duration 30m \
  --timeout 40m \
  --seed 0x5eed5eed \
  --rate-sweep 500,1000,1500 \
  --churn-interval 0 \
  --analyze
```

Choose a rate whose backlog slope is not positive and whose delivery and
commit lag remain bounded. Then run three isolated qualification runs without
a user-side loop:

```sh
perf/soak/run.sh \
  --run-dir /mnt/immulog-benchmarks/qualification \
  --profile sustained \
  --warmup 30m \
  --duration 4h \
  --timeout 4h30m \
  --seed 0x5eed5eed \
  --producer-rate 1000 \
  --churn-interval 0 \
  --runs 3 \
  --analyze
```

Replace `1000` with the rate selected by the sweep. For resilience evidence,
run a separate mixed-profile qualification; do not interpret its throughput
as sustained capacity:

```sh
perf/soak/run.sh \
  --run-dir /mnt/immulog-benchmarks/resilience \
  --profile mixed \
  --warmup 5m \
  --duration 4h \
  --timeout 4h30m \
  --seed 0x5eed5eed \
  --producer-rate 500 \
  --runs 3 \
  --analyze
```

Accept a sustained qualification only when every run exits successfully and
has no assignment loss, dropped samples, failed oracle verification, positive
backlog slope, or unbounded delivery/commit lag. Also confirm that free space,
open files, RSS, and goroutines retain operational headroom. Compare runs only
when commit, seed, Go version, host, filesystem, payload, and workload settings
match. Keep `metrics.json`, `summary.md`, `runs.tsv`, logs, checkpoints, and
environment captures with the evidence record.

### Open-file limits for long runs

The soak keeps log-segment files open while a partition is active. A recorded
30-minute run reached 2,737 open files, so a four-hour qualification should
use the runner's automatic 65,536-file estimate rather than disabling the
preflight.

Check the current shell's limits:

```sh
ulimit -Sn
ulimit -Hn
```

If the hard limit permits it, raise the soft limit for the current shell only:

```sh
ulimit -n 65536
perf/soak/run.sh --duration 4h --timeout 4h30m
```

If both limits are 1,024, use a temporary systemd user scope; this does not
change persistent system configuration:

```sh
systemd-run --user --scope \
  -p LimitNOFILE=65536:65536 \
  bash

cd /path/to/immulog
perf/soak/run.sh --duration 4h --timeout 4h30m
```

Do not use `--minimum-open-files 0` for a long qualification unless the
resulting lower operating limit is intentional; the process can still fail
later when segment count exceeds the OS limit.

Run the same smoke under the race detector:

```sh
IMMULOG_SOAK=1 \
IMMULOG_SOAK_SEED=0x5eed5eed \
IMMULOG_SOAK_DURATION=20s \
go test -race -v ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout=180s
```

For release qualification, use a dedicated pre-sized directory and a planned
24-hour duration. Do not use a real application data directory:

```sh
perf/soak/run.sh \
  --run-dir /path/to/dedicated/soak-run \
  --seed 0x5eed5eed \
  --duration 24h \
  --timeout 25h \
  --minimum-free-bytes auto \
  --minimum-open-files auto
```

The soak checkpoint makes clean resumable runs possible. Keep the directory
private to the qualification run and preserve its checkpoint when collecting
evidence.

## Environment to record

Every result entry should record:

- date and tested commit;
- CPU model, core/logical-CPU count, and CPU governor when available;
- kernel/OS and architecture;
- Go version, `GOOS`, `GOARCH`, `GOAMD64`, and `GOMAXPROCS`;
- filesystem and storage device or volume;
- benchmark duration and sample count;
- payload sizes, batching, topic/partition counts, and operating limits; and
- soak duration, seed, reopen interval, retention policy, and diagnostics.

Soak latency p50/p90/p95/p99/p999 values are upper bounds of the configured
duration buckets, not exact order statistics. New buckets extend through
multi-second tails; legacy `0` still denotes the open-ended final bucket.
Schema-v2 `warmup_nanos` records the optional pre-measurement interval;
`measurement_nanos` covers the active workload only; `total_nanos`
includes cleanup, and phase durations identify drain and verification time.
Bounded microbenchmarks may report exact percentiles when they retain
per-operation samples. Process CPU values are Linux clock ticks and process
I/O values are cumulative `/proc` counters; they are not whole-device
utilization measurements. Analyze artifacts with:

```sh
go run ./perf/analyze --input metrics.json --format markdown
```

Treat a sustained result with a positive sampled backlog slope, dropped
samples, failed drain, failed verification, or assignment loss without
replacement as invalid capacity evidence even if the process exits cleanly.

A minimal environment capture is:

```sh
go version
go env GOOS GOARCH GOAMD64
grep -m1 'model name' /proc/cpuinfo
nproc
uname -a
git rev-parse HEAD
```

## Recorded results

<!-- microbenchmark-evidence:commit=ec3c3c9058b733c3018360c2a05cbca54e2d56a2,date=2026-09-20 -->

### Microbenchmark qualification — 2026-09-20

Measured from commit `ec3c3c9058b733c3018360c2a05cbca54e2d56a2` with profile `qualification`, suite `all`, `10s` per sample, 10 samples per process run, and 3 independent process runs.

Host: Intel(R) Core(TM) i7-10510U CPU @ 1.80GHz; Go: go1.26.2 linux/amd64; filesystem: ext4.

Values are medians across all captured samples; P10–P90 shows ns/op variability.

| Benchmark | Samples | Median ns/op | P10–P90 ns/op | CV | Median records/s | Median MB/s | Median B/op | Median allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `BenchmarkDirectAppendBatch/records-1/payload-1024` | 30 | 2494053 | 2394313–2702143 | 5.3% | 401 | 0.41 | 150213 | 8 |
| `BenchmarkDirectAppendBatch/records-1/payload-256` | 30 | 2440842 | 2106755–2662601 | 8.1% | 410 | 0.11 | 148162 | 8 |
| `BenchmarkDirectAppendBatch/records-1/payload-4096` | 30 | 2720244 | 2602843–2876461 | 4.4% | 368 | 1.50 | 154176 | 8 |
| `BenchmarkDirectAppendBatch/records-256/payload-1024` | 30 | 6605176 | 6289769–6967175 | 7.4% | 38758 | 39.69 | 2155895 | 537 |
| `BenchmarkDirectAppendBatch/records-256/payload-256` | 30 | 3884090 | 3757449–3969618 | 7.7% | 65910 | 16.88 | 712332 | 536 |
| `BenchmarkDirectAppendBatch/records-256/payload-4096` | 30 | 15117935 | 13645672–16905541 | 10.2% | 16934 | 69.36 | 9388302 | 540 |
| `BenchmarkDirectAppendBatch/records-64/payload-1024` | 30 | 3596428 | 3489084–3717088 | 5.8% | 17795 | 18.23 | 553984 | 147 |
| `BenchmarkDirectAppendBatch/records-64/payload-256` | 30 | 2922130 | 2803814–3060776 | 5.0% | 21902 | 5.61 | 255888 | 147 |
| `BenchmarkDirectAppendBatch/records-64/payload-4096` | 30 | 6857912 | 6434381–7557709 | 5.8% | 9332 | 38.23 | 2233903 | 149 |
| `BenchmarkDirectAppendBatch/records-8/payload-1024` | 30 | 2774228 | 2664853–2933735 | 3.4% | 2884 | 2.96 | 190794 | 27 |
| `BenchmarkDirectAppendBatch/records-8/payload-256` | 30 | 2694334 | 2506570–2809474 | 4.7% | 2970 | 0.76 | 155203 | 27 |
| `BenchmarkDirectAppendBatch/records-8/payload-4096` | 30 | 3037770 | 2972514–3171962 | 3.6% | 2634 | 10.79 | 335370 | 29 |
| `BenchmarkFetch/segment` | 30 | 72808 | 65558–85533 | 10.4% | 1758120 | 450.08 | 193886 | 258 |
| `BenchmarkFetch/tail` | 30 | 23482 | 22698–29698 | 12.1% | 5450936 | 1395.44 | 49152 | 129 |
| `BenchmarkFetchParallel` | 30 | 96040 | 83571–100636 | 11.4% | 1332785 | 341.19 | 193888 | 258 |
| `BenchmarkIngressAppend` | 30 | 2471864 | 2253702–2950169 | 30.9% | 405 | 0.10 | 154160 | 18 |
| `BenchmarkIngressAppendParallel/producers-1/payload-1024` | 30 | 2501011 | 2266692–2669665 | 7.7% | 400 | 0.41 | 162426 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-256` | 30 | 2509468 | 2252242–2619869 | 6.8% | 398 | 0.10 | 149182 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-4096` | 30 | 2756686 | 2687322–3068664 | 7.6% | 363 | 1.49 | 160270 | 19 |
| `BenchmarkIngressAppendParallel/producers-1/payload-64` | 30 | 2454646 | 2291010–2608554 | 14.4% | 407 | 0.03 | 150932 | 19 |
| `BenchmarkIngressAppendParallel/producers-16/payload-1024` | 30 | 356830 | 328803–362407 | 5.4% | 2802 | 2.87 | 26662 | 12 |
| `BenchmarkIngressAppendParallel/producers-16/payload-256` | 30 | 324390 | 298070–342996 | 6.3% | 3083 | 0.79 | 21727 | 12 |
| `BenchmarkIngressAppendParallel/producers-16/payload-4096` | 30 | 394146 | 346786–415153 | 7.1% | 2537 | 10.39 | 42857 | 13 |
| `BenchmarkIngressAppendParallel/producers-16/payload-64` | 30 | 310300 | 286325–324843 | 5.6% | 3222 | 0.21 | 20784 | 12 |
| `BenchmarkIngressAppendParallel/producers-2/payload-1024` | 30 | 2366620 | 2216157–2654618 | 8.2% | 423 | 0.43 | 165325 | 18 |
| `BenchmarkIngressAppendParallel/producers-2/payload-256` | 30 | 2481281 | 2158281–2646298 | 9.0% | 403 | 0.10 | 144556 | 18 |
| `BenchmarkIngressAppendParallel/producers-2/payload-4096` | 30 | 2767639 | 2569941–2865954 | 9.2% | 361 | 1.48 | 157154 | 18 |
| `BenchmarkIngressAppendParallel/producers-2/payload-64` | 30 | 2574728 | 2235198–2704992 | 8.2% | 388 | 0.02 | 143375 | 18 |
| `BenchmarkIngressAppendParallel/producers-32/payload-1024` | 30 | 191646 | 155893–195014 | 9.5% | 5218 | 5.35 | 17557 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-256` | 30 | 174432 | 164019–179022 | 4.2% | 5732 | 1.47 | 12677 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-4096` | 30 | 257772 | 219475–268241 | 8.8% | 3880 | 15.89 | 38873 | 12 |
| `BenchmarkIngressAppendParallel/producers-32/payload-64` | 30 | 157700 | 142525–167333 | 5.7% | 6342 | 0.41 | 11418 | 12 |
| `BenchmarkIngressAppendParallel/producers-4/payload-1024` | 30 | 1331824 | 1257714–1372258 | 6.2% | 751 | 0.77 | 77300 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-256` | 30 | 1258901 | 1190754–1310947 | 6.8% | 794 | 0.20 | 76562 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-4096` | 30 | 1428854 | 1332380–1466801 | 5.5% | 700 | 2.87 | 92776 | 16 |
| `BenchmarkIngressAppendParallel/producers-4/payload-64` | 30 | 1230388 | 1113310–1284650 | 7.4% | 813 | 0.05 | 76434 | 16 |
| `BenchmarkIngressAppendParallel/producers-64/payload-1024` | 30 | 109257 | 104004–114618 | 6.2% | 9152 | 9.37 | 12321 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-256` | 30 | 94140 | 87856–96637 | 5.5% | 10622 | 2.72 | 8142 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-4096` | 30 | 174967 | 149076–204356 | 13.1% | 5716 | 23.41 | 38914 | 11 |
| `BenchmarkIngressAppendParallel/producers-64/payload-64` | 30 | 87433 | 81427–90039 | 4.0% | 11437 | 0.73 | 6509 | 11 |
| `BenchmarkIngressAppendParallel/producers-8/payload-1024` | 30 | 662081 | 597347–692491 | 5.7% | 1510 | 1.54 | 44809 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-256` | 30 | 639389 | 567230–654305 | 6.0% | 1564 | 0.40 | 39050 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-4096` | 30 | 720278 | 625049–762012 | 8.0% | 1388 | 5.69 | 62670 | 14 |
| `BenchmarkIngressAppendParallel/producers-8/payload-64` | 30 | 631138 | 593207–649056 | 5.4% | 1584 | 0.10 | 38222 | 13 |
| `BenchmarkIngressBatchLinger/producers-32/linger-0s` | 30 | 194161 | 183450–197295 | 3.4% | 5150 | 5.28 | 17696 | 12 |
| `BenchmarkIngressBatchLinger/producers-32/linger-100µs` | 30 | 189748 | 175319–207856 | 7.5% | 5270 | 5.39 | 16324 | 12 |
| `BenchmarkIngressBatchLinger/producers-32/linger-1ms` | 30 | 166977 | 163063–175590 | 5.0% | 5989 | 6.13 | 12708 | 11 |
| `BenchmarkIngressBatchLinger/producers-32/linger-5ms` | 30 | 287860 | 281295–295272 | 1.8% | 3474 | 3.56 | 12107 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-0s` | 30 | 106961 | 94756–112957 | 7.0% | 9350 | 9.57 | 12392 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-100µs` | 30 | 118096 | 96391–127641 | 10.2% | 8468 | 8.67 | 12224 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-1ms` | 30 | 101454 | 90021–108589 | 10.5% | 9858 | 10.09 | 10975 | 11 |
| `BenchmarkIngressBatchLinger/producers-64/linger-5ms` | 30 | 156096 | 143661–157878 | 4.2% | 6406 | 6.56 | 10828 | 11 |
| `BenchmarkIngressBatchLinger/producers-8/linger-0s` | 30 | 693260 | 650964–706344 | 5.4% | 1442 | 1.48 | 43005 | 14 |
| `BenchmarkIngressBatchLinger/producers-8/linger-100µs` | 30 | 554967 | 535711–572739 | 4.9% | 1802 | 1.85 | 28180 | 14 |
| `BenchmarkIngressBatchLinger/producers-8/linger-1ms` | 30 | 545846 | 523767–556798 | 3.6% | 1832 | 1.88 | 21088 | 13 |
| `BenchmarkIngressBatchLinger/producers-8/linger-5ms` | 30 | 1053090 | 1007189–1062247 | 2.7% | 950 | 0.97 | 14727 | 13 |

Variance notes:

- BenchmarkDirectAppendBatch/records-256/payload-4096 has 10.2% coefficient of variation for ns/op.
- BenchmarkFetch/segment has 10.4% coefficient of variation for ns/op.
- BenchmarkFetch/tail has 12.1% coefficient of variation for ns/op.
- BenchmarkFetchParallel has 11.4% coefficient of variation for ns/op.
- BenchmarkIngressAppend has 30.9% coefficient of variation for ns/op.
- BenchmarkIngressAppendParallel/producers-1/payload-64 has 14.4% coefficient of variation for ns/op.
- BenchmarkIngressAppendParallel/producers-64/payload-4096 has 13.1% coefficient of variation for ns/op.
- BenchmarkIngressBatchLinger/producers-64/linger-100µs has 10.2% coefficient of variation for ns/op.
- BenchmarkIngressBatchLinger/producers-64/linger-1ms has 10.5% coefficient of variation for ns/op.

The raw run artifacts and machine-readable analysis are preserved in the benchmark run directory. This is host-specific evidence, not a portable throughput guarantee.


### Ingress migration smoke — 2026-09-14

Measured from migration commit `46a044283fae5aaa6034e63ba27dcd64a5fc0225`
on Linux `7.0.0-31-generic` x86_64 with Go `1.26.2` and an Intel Core
i7-10510U (4 cores, 8 logical CPUs). The command was:

```sh
go test ./perf/benchmarks -run '^$' -bench . -benchmem -count=3
```

The three rows for each benchmark are sequential samples from one local run.
Filesystem load and scheduler variance are expected.

| Benchmark | Sample 1 | Sample 2 | Sample 3 | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| `BenchmarkIngressAppend` | 19.257 ms | 19.328 ms | 19.189 ms | 6,728 / 6,125 / 6,153 | 51 / 50 / 51 |
| `BenchmarkDirectAppendBatch` | 21.773 ms | 20.043 ms | 19.894 ms | 5,450 / 5,319 / 5,199 | 43 / 43 / 42 |

The ingress path was faster in this sample while using more allocations. This
single run is informational; it does not establish a regression threshold or
prove a general advantage over the direct reference path. No pre-migration
historical baseline was captured with the same command and host conditions.

### Timed ingress acquisition comparison — 2026-09-16

Measured on merged `main` commit `1e480154b21cd5090cdeed292bcd8b89819d85e5`
against pre-migration commit `7ed288d`, on Linux `7.0.0-31-generic` x86_64 with
Go `1.26.2`, an Intel Core i7-10510U, 8 logical CPUs, and an ext4 filesystem
mounted from `/dev/mapper/ubuntu--vg-ubuntu--lv`. The benchmark used the same
host, filesystem, limits, payload, and command for both revisions:

```sh
go test ./perf/benchmarks -run '^$' -bench '^BenchmarkIngressBatchLinger$' \
  -benchmem -benchtime=2s -count=3
```

Values below are medians across the three samples. The pre-migration revision
used the v0.4.0 post-selection sleep; merged `main` uses v0.6.0
`WithBatchTimeout`.

| Producers | Linger | Before records/s | After records/s | Change | Before syncs/record | After syncs/record |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 0s | 2,667 | 2,667 | +0.0% | 0.2500 | 0.2498 |
| 8 | 100µs | 1,501 | 2,936 | +95.6% | 0.2500 | 0.1256 |
| 8 | 1ms | 1,485 | 2,890 | +94.6% | 0.2501 | 0.1251 |
| 8 | 5ms | 547 | 1,093 | +99.7% | 0.2508 | 0.1252 |
| 32 | 0s | 9,647 | 9,556 | -0.9% | 0.06235 | 0.06227 |
| 32 | 100µs | 5,775 | 11,075 | +91.8% | 0.06240 | 0.03196 |
| 32 | 1ms | 5,589 | 10,503 | +87.9% | 0.06243 | 0.03127 |
| 32 | 5ms | 2,161 | 4,182 | +93.5% | 0.06257 | 0.03126 |
| 64 | 0s | 17,347 | 17,528 | +1.0% | 0.03015 | 0.03035 |
| 64 | 100µs | 11,316 | 19,211 | +69.8% | 0.03060 | 0.01662 |
| 64 | 1ms | 10,814 | 18,354 | +69.7% | 0.03036 | 0.01564 |
| 64 | 5ms | 4,251 | 7,988 | +87.9% | 0.03093 | 0.01567 |

Zero-linger performance is effectively unchanged, while positive windows
roughly halve syncs per record and improve throughput by 70–96% in the useful
100µs–1ms range. The 5ms window still improves on the old implementation but
has lower absolute throughput than shorter windows because it adds avoidable
latency. This is a comparative local smoke, not a release threshold; it does
not claim multi-batch group-commit fsync reduction.

### Mixed workload smoke — 2026-09-14

Both runs used seed `0x5eed5eed`, two producers per partition, two partitions
per fixture, and the repository's default short-soak settings. All independent
partition oracles passed.

| Run | Offered | Acknowledged | Unknown | Known rejected | Cancelled | Max goroutines | Max open files | Max heap |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Normal, 20s | 36,226 | 2,460 | 5,686 | 28,080 | 33,766 | 30 | 29 | 5.76 MiB |
| Race, 20s | 48,141 | 2,645 | 6,756 | 38,740 | 45,496 | 28 | 34 | 5.42 MiB |

The normal run completed in 20.907 seconds and the race run in 21.974
seconds. Neither run reported an oracle, test, or race failure. The workload
intentionally exercises cancellation and overload, so the rejection and
unknown-outcome counts are expected observations rather than failures.

### Four-hour mixed workload qualification — 2026-09-17

Evidence run: `/home/ayesh/immulog-soak-20260917-091511`. The run used commit
`d9da177823a241e080273d628a20da22b1fa3e99`, branch
`docs/benchmark-methodology`, Go `1.26.2`, Linux `7.0.0-31-generic`, an Intel
Core i7-10510U with 8 logical CPUs, and ext4 on
`/dev/mapper/ubuntu--vg-ubuntu--lv`.

Configuration:

```text
duration=4h
seed=0x5eed5eed
reopen_interval=10m
append_interval=20ms
timeout=4h30m
minimum_free_bytes=25769803776 (24 GiB)
minimum_open_files=65536
```

The process ran for 4h02m35s including cycle shutdown and verification. It
exited with status 0 and reported `PASS`. All four independent partition
oracles passed:

| Oracle class | Verified | Expired | Next offset |
|---|---:|---:|---:|
| Retained, two partitions | 6,174,226 | 10 | 6,211,670 |
| Stable, two partitions | 6,358,698 | 0 | 6,358,698 |

The ten retained-topic expirations are expected retention behavior. The stable
topic had no expirations, and no oracle reported corruption, discontinuity,
duplicate delivery, or recovery failure.

Workload counters were:

| Metric | Result |
|---|---:|
| Offered append calls | 15,405,768 |
| Acknowledged | 3,648,354 (23.68%) |
| Unknown outcome | 8,922,014 (57.91%) |
| Known rejected | 2,835,400 (18.40%) |
| Cancelled observations | 11,757,414 |
| Overload-window calls | 11,554,619 |
| Acknowledged bytes | 7,325,313,118 |

The cancellation and overload counters are intentionally stressed by the
harness and may overlap the outcome counters. Unknown outcomes are expected
under the short cancellation deadlines; they remain important evidence that
callers must reconcile ambiguous append results rather than blindly retrying.

Runtime and storage observations:

| Metric | Result |
|---|---:|
| Initial free space | 123,483,914,240 bytes |
| Final free space | 113,081,831,424 bytes |
| Final data size | 9,691,036,522 bytes |
| Maximum open files | 18,295 / 65,536 |
| Maximum goroutines | 33 |
| Maximum heap allocation | 485,440,872 bytes |
| GC cycles | 148,153 |

The stable topic produced 18,256 log segments and approximately 9.0 GiB of
storage. The retained topic remained bounded at two log segments. The observed
open-file peak closely tracks the stable segment count plus process overhead,
which is consistent with expected segment-file lifetime rather than an
independent descriptor leak. The four-hour free-space and open-file guardrails
had substantial remaining headroom.

Latency summaries are bucketed as `<1µs`, `1–10µs`, `10–100µs`, `100µs–1ms`,
`1–10ms`, `10–100ms`, `100ms–1s`, and `≥1s`:

| Operation | Operations | Average | Distribution summary |
|---|---:|---:|---|
| Append | 15,405,768 | 1.42 ms | 53.5% in 100µs–1ms; 45.8% in 1–10ms |
| Poll | 1,194,230 | 6.08 ms | 41.8% in 1–10ms; 0.20% ≥1s |
| Commit | 1,045,164 | 3.40 ms | 98.7% in 1–10ms |
| Verify | 4,227,821 | 1.45 ms | 43.7% in 10–100µs; 14.2% in 1–10ms |

This is a successful four-hour correctness and stability qualification for the
configured workload. It is not a 24-hour capacity result: at the observed
segment and storage rates, substantially longer runs require larger disk and
open-file limits. The result also does not establish power-loss, portability,
or production-capacity guarantees.

### Four-hour sustained workload qualification — 2026-09-17

Evidence run: `/home/ayesh/immulog-soak-20260917-155444`. The run used commit
`05eb0d5d44caef59a1f3ccb017a21072ae55792e`, branch `main`, Go `1.26.2`, Linux
`7.0.0-31-generic`, an Intel Core i7-10510U with 8 logical CPUs, and ext4 on
`/dev/mapper/ubuntu--vg-ubuntu--lv`.

Configuration:

```text
duration=4h
profile=sustained
seed=0x5eed5eed
reopen_interval=10m
append_interval=20ms
timeout=4h30m
minimum_free_bytes=25769803776 (24 GiB)
minimum_open_files=65536
```

The process ran for 4h05m23s including shutdown and verification. It exited
with status 0 and reported `PASS`. All four independent partition oracles
passed. The retained topics reported 5,215,937 and 5,270,456 verified records
with 334 and 266 expected expirations; the stable topics reported 5,281,001
and 5,278,469 verified records with no expirations. No corruption,
discontinuity, duplicate delivery, or recovery failure was reported.

The sustained profile disables deliberate overload and short-deadline
cancellation. Workload counters were:

| Metric | Result |
|---|---:|
| Offered append calls | 21,046,519 |
| Acknowledged | 21,046,373 (99.9993%) |
| Unknown outcome | 140 |
| Known rejected | 6 |
| Cancelled observations | 146 |
| Overload-window calls | 0 |
| Acknowledged bytes | 42,252,399,859 |
| Acknowledged records/s, configured 4h | 1,461 |
| Acknowledged bytes/s, configured 4h | 2,934,194 (2.93 MB/s) |

Runtime and storage observations:

| Metric | Result |
|---|---:|
| Initial free space | 123,613,425,664 bytes |
| Final free space | 100,449,792,000 bytes |
| Final data size | 22,961,852,725 bytes |
| Maximum open files | 44,421 / 65,536 |
| Maximum goroutines | 27 |
| Maximum heap allocation | 3,227,474,848 bytes |
| Maximum RSS | 4,088,864,768 bytes |
| GC cycles | 34,149 |
| Process read bytes | 183,758,938,112 |
| Process write bytes | 96,765,956,096 |

Consumer lag remained bounded but substantial: average delivery and commit
lag were 27,296 records, with a maximum of 229,188 records across 60,133
samples. The acknowledged-to-scan latency averaged 4.58ms with a 10ms p50
bucket and 100ms p95/p99 buckets. Append latency averaged 4.10ms with a 10ms
p50/p95 bucket and 100ms p99 bucket; poll averaged 3.13ms with 1ms p50, 10ms
p95, and 100ms p99 buckets; commit averaged 4.21ms with 10ms p50/p95 and
100ms p99 buckets; and verify averaged 2.40ms with 10ms p50/p95 and 100ms p99
buckets.

Ack-to-delivery latency reflects the consumer backlog rather than append
latency: it averaged 119.85s, had a 1s p50 bucket, and had p95 and p99 in the
open-ended `≥1s` bucket. This is a successful four-hour sustained durability,
correctness, and bounded-resource run, and its acknowledged rate is useful
local throughput evidence for this configuration. It is not a latency target
or a production-capacity guarantee; the observed delivery lag should be
investigated before treating the workload as healthy end-to-end sustained
consumer throughput. The run also does not establish power-loss, portability,
or storage-device-independent durability guarantees.

### Four-hour sustained workload with per-partition consumer metrics — 2026-09-18

Evidence run: `/home/ayesh/immulog-soak-20260917-225016`. The run used commit
`05eb0d5d44caef59a1f3ccb017a21072ae55792e`, branch `main`, Go `1.26.2`, Linux
`7.0.0-31-generic`, an Intel Core i7-10510U with 8 logical CPUs, and ext4 on
`/dev/mapper/ubuntu--vg-ubuntu--lv`.

Configuration:

```text
duration=4h
profile=sustained
seed=0x5eed5eed
reopen_interval=10m
append_interval=20ms
timeout=4h30m
minimum_free_bytes=25769803776 (24 GiB)
minimum_open_files=65536
```

The test completed in 4h01m45s including shutdown and verification, exited with
status 0, and reported `PASS`. All four independent partition oracles passed.
The retained topics reported 5,297,831 and 5,362,204 verified records with
298 and 256 expected expirations; the stable topics reported 5,372,214 and
5,371,485 verified records with no expirations. No corruption, discontinuity,
duplicate delivery, or recovery failure was reported.

The sustained profile disables deliberate overload and short-deadline
cancellation. Workload counters were:

| Metric | Result |
|---|---:|
| Offered append calls | 21,404,334 |
| Acknowledged | 21,404,201 (99.9994%) |
| Unknown outcome | 131 |
| Known rejected | 2 |
| Cancelled observations | 133 |
| Overload-window calls | 0 |
| Acknowledged bytes | 42,970,599,312 |
| Acknowledged records/s, configured 4h | 1,486 |
| Acknowledged bytes/s, configured 4h | 2,984,069 (2.98 MB/s) |

Per-partition consumer metrics used the full recorded measurement interval of
14,504.66s for the delivery-rate calculation:

| Partition | Delivered records | Delivery rate | Avg delivery lag | Max delivery lag | Assignment-loss events |
|---|---:|---:|---:|---:|---:|
| `soak-stable/0` | 5,200,172 | 358.5 records/s | 29,684 | 233,920 | 54,269 |
| `soak-stable/1` | 5,174,650 | 356.8 records/s | 32,516 | 251,319 | 60,248 |

The partitions were balanced within 0.5% of delivery rate. Neither partition
had an empty poll; poll batches averaged 9.69 and 9.64 records, while commit
batches averaged 10.87 and 10.95 records. The two partitions recorded 114,517
assignment-loss events in total. This is a diagnostic observation from the
harness, not proof that every event represents a distinct membership churn
operation.

Runtime and storage observations:

| Metric | Result |
|---|---:|
| Initial free space | 123,752,935,424 bytes |
| Final free space | 100,051,357,696 bytes |
| Final data size | 23,352,895,385 bytes |
| Maximum open files | 45,180 / 65,536 |
| Maximum goroutines | 27 |
| Maximum heap allocation | 3,218,811,680 bytes |
| Maximum RSS | 3,668,885,504 bytes |
| GC cycles | 34,018 |
| Process read bytes | 229,460,484,096 |
| Process write bytes | 97,966,284,800 |

Append latency averaged 4.03ms with a 10ms p50/p95 bucket and 100ms p99
bucket. Poll averaged 3.04ms with 1ms p50, 10ms p95, and 100ms p99 buckets;
commit averaged 4.15ms with 10ms p50/p95 and 100ms p99 buckets; and verify
averaged 2.35ms with 10ms p50/p95 and 100ms p99 buckets. Ack-to-scan latency
averaged 4.67ms with 10ms p50 and 100ms p95/p99 buckets.

Consumer lag increased relative to the previous four-hour sustained run:
average delivery and commit lag rose from 27,296 to 31,095 records, and the
maximum rose from 229,188 to 251,319. Ack-to-delivery latency averaged 134.84s,
with a 1s p50 bucket and p95/p99 in the open-ended `≥1s` bucket. Poll and
commit operation averages were slightly lower than the previous run, while
delivery lag increased. The balanced partition rates and high assignment-loss
count make membership churn and discarded consumer work important follow-up
investigation targets. This is successful durability and correctness evidence,
but not a healthy end-to-end consumer-latency result or a production-capacity
guarantee.

## Interpretation and comparison policy

When comparing changes:

1. Compare the same benchmark selection, host class, filesystem, payloads,
   limits, Go toolchain, `GOMAXPROCS`, duration, and sample count.
2. Prefer repeated sequential samples and report the range or median rather
   than a single best result.
3. Separate durable storage latency from in-memory claim/publication
   microbenchmarks; do not compare their units as if they measured the same
   operation.
4. Investigate material changes in latency, allocations, CPU, memory, file
   descriptors, or soak outcome distributions before assigning a threshold.
5. Treat a passing soak as correctness and stability evidence for the tested
   workload, not as proof of power-loss safety, portability, or production
   capacity.

`go test -bench` output and soak logs are the primary evidence. Do not commit
large generated profiles or raw output files; attach them as workflow artifacts
or retain them with the qualification run.

## Qualification limits

The recorded migration smoke predates the expanded benchmark matrix and is not
comparable to current results. It does not establish a high-performance target.

The recorded migration smoke does not replace:

- the deterministic filesystem fault and subprocess crash matrix;
- parser and recovery fuzz qualification;
- a long-running 24-hour mixed-workload soak; or
- platform and storage-device-specific durability qualification.

Those checks remain release-qualification work and should be reported with the
same environment and commit metadata.
