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

Run the repository microbenchmarks with repeated samples:

```sh
go test ./perf/benchmarks -run '^$' -bench . -benchmem -benchtime=5s -count=5
go test ./perf/benchmarks -run '^$' -bench . -benchmem -cpu 1,2,4,8 -count=3
```

Use focused runs while iterating:

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
a local durable-throughput workload. Use
`perf/soak/run.sh --help` to view all duration, seed, interval, resource,
timeout, and path options. Numeric resource values override the estimates and
zero disables a preflight. The runner records initial/final data size, free
space, and the process open-file limit, and writes a machine-readable
`metrics.json` sidecar containing counters, resource observations, lag, oracle
results, and latency bucket data.

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

Soak latency p50/p95/p99 values are upper bounds of the configured duration
buckets, not exact order statistics. Bounded microbenchmarks may report exact
percentiles when they retain per-operation samples. Process CPU values are
Linux clock ticks and process I/O values are cumulative `/proc` counters; they
are not whole-device utilization measurements.

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
