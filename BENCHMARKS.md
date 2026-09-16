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
observations and may overlap those outcome counters.

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
parameters. Use `b.SetBytes` for payload bandwidth and report records/sec for
record throughput. Treat setup, reopen, retention, and verification as separate
lifecycle measurements rather than including them in steady-state throughput.

The manual GitHub Actions workflow in
`.github/workflows/performance.yml` accepts `benchtime` and `benchmark_count`
inputs, records the Go and host environment, and uploads the raw output as an
artifact. Its default is five one-second samples.

Run a short, fixed-seed workload smoke with verbose metrics:

```sh
IMMULOG_SOAK=1 \
IMMULOG_SOAK_SEED=0x5eed5eed \
IMMULOG_SOAK_DURATION=20s \
go test -v ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout=90s
```

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
IMMULOG_SOAK=1 \
IMMULOG_SOAK_DIR=/path/to/dedicated/soak-directory \
IMMULOG_SOAK_SEED=0x5eed5eed \
IMMULOG_SOAK_DURATION=24h \
go test -v ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout=25h
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
