# Repository Guidelines

## Project Structure & Module Organization

`immulog` is a Go 1.26 module for a local, durable, append-only event log.
Public contracts and stable domain errors are in `api/`; encoding, segment
management, locking, partitions, and the store live in `storage/`. Core
correctness tests are co-located with their package (`*_test.go`); benchmark
and long-running soak harnesses live under `perf/`. `README.md` describes the
current scope, `BENCHMARKS.md` is the source of truth for performance
methodology and dated evidence, and `PROGRESS.md` records the implementation
checkpoint and next steps. Keep future metadata, consumer, retention, and
ingress work within the planned package boundaries; do not add network or
replication code to this slice.

The `Store` owns the canonical directory, stable `LOCK`, catalog and consumer
offset system partitions, user partitions, retention, snapshots, and disk
admission. Catalog metadata is itself an append-only system log. Each partition
uses bounded multi-producer ingress with one terminal durable writer; segments
are authoritative, while indexes, snapshots, and live-tail caches are
rebuildable. Managed consumers are same-process, at-least-once assignments
with durable commits and fencing.

## Architecture Map

The exported surface is small (`Store`, `Partition`, `Reader`, `Consumer`,
`GroupConsumer`); nearly all behavior lives in unexported collaborators inside
`storage/`. The following traces are the fastest way to orient before editing.

**On-disk layout** (rooted at the store directory):

```
LOCK                                  stable ownership lock (lock_linux.go)
system/cluster-metadata/0/            catalog system log (__cluster_metadata)
system/consumer-offsets/0/            offsets system log (__consumer_offsets)
topics/<topic-uuid>/<partition>/      user partitions
  00000000000000000000.log            segment, base offset zero-padded to 20
  00000000000000000000.index          sampled offset index (rebuildable)
  00000000000000000000.timeindex      sampled time index (rebuildable)
  projection.snapshot                 system logs only, optional accelerator
  .topic-preparation-v1               crash-safe topic creation marker
```

`format.go` holds the wire constants (`SegmentMagic`, `BatchMagic`, header
sizes, CRC32C) and the `EventType` enum; `codec.go` encodes/decodes batches.
All integers are little-endian. Only `filesystem.go`'s `fileSystemOps` seam
touches the OS, which is how fault-injection tests simulate write/sync
failures.

**Append path** (`Partition.Append`, `writer.go:386`): validate and charge the
request → reserve disk through `Partition.reserveDisk` → `admit` against the
in-flight bound → copy caller bytes (`copyRequest`) → `enqueue` publishes into
the per-partition disruptor ring (`ingress.go`). A single `BatchProcessor`
goroutine is the only writer: `ingressHandler.Handle` accumulates an
end-of-batch group, `writeIngressBatches` splits it by `BatchRecords`/
`BatchBytes`, and `writeRequests` assigns offsets from `p.logEnd` and calls
`appendEncodedLocked` (`append_encoded.go`), which rolls segments, writes,
syncs, and only then completes the waiting callers. A sync failure marks the
partition unavailable and returns `ErrAppendOutcomeUnknown` — nothing is rolled
back. `ingress.go` is the *only* adapter over `lib-disruptor`; keep it that way.

**Read path** (`fetch.go`): bounds-check against segment zero's base offset and
`logEnd`, try the optional in-memory live tail (`tail.go`, byte-bounded per
store via `tailBudget`), else scan segments from the nearest sampled
`offsetIndex` hint. `Reader` is a thin cursor over `Fetch`.

**Metadata as logs**: the catalog and consumer offsets are themselves
append-only partitions of records carrying `EventTopicCreated`,
`EventPartitionLogStartAdvanced`, `EventGroupCreated`,
`EventLocalAssignmentChanged`, `EventOffsetCommitted`, and
`EventStoreInitialized` (`events.go`, `events_validation.go`). `Open` runs
`bootstrapMetadata` (`bootstrap.go`), replays both logs into
`catalogProjection` (`catalog.go`) and `offsetsProjection`
(`offsets_projection.go`), preflights user storage against the projection
(`preflight.go`), then reconciles retired artifacts. `snapshot.go` snapshots
are validated accelerators, never authority — a mismatch degrades to replay and
is reported through `Store.SnapshotDiagnostics`.

**Consumers**: `consumer.go` is one assignment (group, topic, partition);
`group_consumer.go` is a same-process multi-partition group with canonical
membership, generations, and fencing. Both serialize durable commits through
`Store.appendOffsetsEventLocked` on the offsets log.

**Cross-cutting**: `retention.go` (background loop, advances log start before
deleting), `disk_pressure.go` (class-aware byte/inode ledger, see the Safety
section), `stats.go`/`consumer_stats.go` (bounded diagnostics and latency
buckets), `config.go` and `capacity.go` (limit defaults and planning).

## Build, Test, and Development Commands

This is a single Go 1.26 module using the standard Go toolchain; there is no
separate build system or lint configuration. Linux is the only currently
qualified platform because locking and disk-pressure implementations are
Linux-specific.

Run these from the repository root:

```sh
gofmt -w api/*.go storage/*.go perf/benchmarks/*.go perf/soak/*.go
go mod tidy
go vet ./...
go test -shuffle=on ./...
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

Use `go test ./storage -run TestName` for a focused test. For concurrency
regressions, repeat the focused test with `-count` and use `-race`; avoid
sleep-only assertions.

Long-running or opt-in checks:

```sh
go test ./storage -run '^$' -fuzz=FuzzDecodeBatch -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzDecodeSegmentHeader -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzPreflightSystemLogSegment -fuzztime=60m -parallel=1
perf/benchmarks/run.sh --profile smoke
perf/soak/run.sh --profile mixed --duration 20s --timeout 90s --minimum-free-bytes 0 --minimum-open-files 0
```

There is no separate build script; `go test ./...` compiles all packages.
`perf/benchmarks/run.sh` owns reproducible microbenchmark profiles and captures
raw output and host metadata; use `smoke` for quick checks, `standard` for
comparisons, and `qualification` for release evidence. The soak test is
disabled unless `IMMULOG_SOAK=1`; use `IMMULOG_SOAK_DIR`
for a dedicated persistent directory. `IMMULOG_SOAK_DURATION`, `IMMULOG_SOAK_WARMUP`,
`IMMULOG_SOAK_REOPEN_INTERVAL`, `IMMULOG_SOAK_APPEND_INTERVAL`,
`IMMULOG_SOAK_PRODUCER_RATE`, `IMMULOG_SOAK_SAMPLE_INTERVAL`,
`IMMULOG_SOAK_SAMPLE_LIMIT`, `IMMULOG_SOAK_CHURN_INTERVAL`, and
`IMMULOG_SOAK_SEED` control resumable runs. A nonzero producer rate applies a
monotonic aggregate records-per-second schedule; zero preserves unlimited
producer mode. The runner's `--churn-interval 0` setting disables deliberate
consumer membership replacement. Prefer `perf/soak/run.sh` for
long runs because it records environment, resource guardrails, logs,
checkpoints, and `metrics.json`. Use `--runs` for isolated repeated evidence
runs and `--rate-sweep` for isolated offered-rate runs; do not wrap the runner
in a user-side shell loop. Explicit `--data-dir` cannot be combined with
`--runs`. Benchmark qualification rejects dirty worktrees unless `--allow-dirty`
is supplied.

### Performance and soak evidence

`BENCHMARKS.md` is authoritative for performance methodology and dated
results. The benchmark-only `perf/metrics` package provides rate, bounded
histogram, and exact-sample helpers; append benchmarks report
`producer-records/s` and `producer-bytes/s`, while fetch benchmarks report
`consumer-records/s` and `consumer-bytes/s`.

Use the `mixed` soak profile for correctness and resilience coverage. It
intentionally exercises cancellation, overload, retention, consumer churn,
reopen, and oracle verification, so its acknowledged throughput is not a
capacity result. Use the `sustained` profile for local durable-throughput and
backlog measurements; it removes the deliberate append delay, overload
windows, and short-deadline cancellation. A sustained run is only healthy
when delivery and commit lag remain bounded over time.

Soak evidence must use a dedicated data directory and preserve the run
artifacts. `metrics.json` includes outcome counters, oracle results, lag,
RSS/heap/goroutine/FD observations, process I/O and CPU ticks, latency
histograms, aggregate/per-partition delivered payload bytes and rates, and
bounded periodic backlog/resource samples, skipped observer/oracle checks,
and completion/failure status. Schema version 2 records optional warmup duration, separates measurement
duration from total cleanup time, and records measure, drain,
verify, and cleanup phases. Ingress `acknowledged_bytes` and consumer
`delivered_payload_bytes` count record `Value` bytes only; keys and on-disk
framing are excluded. Long-run p50/p90/p95/p99/p999 values are bucket upper
bounds; a `0` in legacy latency fields denotes the open-ended final bucket,
not zero latency. Process I/O
is cumulative `/proc` data and CPU values are Linux process clock ticks, not
device-wide utilization. Use `go run ./perf/analyze --input metrics.json`
to calculate measurement-only rates and sampled backlog trends. Mixed-profile
results with substantial lag should be reported as stress/correctness evidence,
not sustained-throughput evidence; sustained evidence with a positive backlog
slope is not a stable capacity result.

For reproducibility, compare runs using the same commit, seed, profile,
duration, payload/workload configuration, Go version, and filesystem. The
runner records the commit but not the complete working-tree diff, so release
or qualification runs should start from a clean, recorded commit. Long runs
can consume multiple GiB and thousands of open files; keep automatic resource
preflights enabled unless deliberately testing a lower limit.

A sustained no-churn run that reports consumer assignment loss without a
replacement is a failed liveness run, not throughput evidence. The soak's
consumer progress timeout is currently five seconds; diagnose the reported
assignment-loss invariant and last poll/commit timing before increasing that
timeout, because synchronous durable commits, snapshots, or filesystem
contention may prevent deadline renewal.

## Coding Style & Naming Conventions

Use standard `gofmt` formatting, tabs for Go indentation, and idiomatic Go
names: exported identifiers use PascalCase and unexported helpers use
camelCase. Preserve `errors.Is` compatibility for public domain errors in
`api/errors.go`. Keep on-disk formats explicit and little-endian, validate
inputs at package boundaries, and document exported types and methods.

## Testing Guidelines

Tests use Go's standard `testing` package and should be named
`Test<TypeOrBehavior>`. Add deterministic correctness regression tests beside
the implementation, especially for malformed bytes, CRC/checksum failures,
offset continuity, reopen/recovery behavior, locking, and nil-versus-empty
payload semantics. Storage tests use `t.TempDir()` and never write to a real
user data directory; use bounded deadlines and explicit barriers for
concurrency. Keep benchmarks in `perf/benchmarks` and opt-in long-running soak
workloads in `perf/soak`; they must use public package APIs rather than
production-private test seams. Run formatting, module tidy, vet, shuffled tests,
race tests, and coverage before submitting changes.

## Commit & Pull Request Guidelines

Use concise imperative Conventional Commit subjects such as `fix: ...`,
`feat: ...`, or `test: ...`; keep unrelated changes separate. Pull requests
should explain observable behavior, affected packages and invariants, relevant
plan sections, and the exact validation commands run. Call out compatibility,
durability, recovery, or on-disk format impact; include focused test details
when changing storage behavior. Pin third-party GitHub Actions to full commit
SHAs and retain least-privilege permissions.

## Safety & Configuration Notes

The filesystem log is authoritative. An acknowledged append requires the batch
write, file sync, and required namespace sync; unknown outcomes are not rolled
back. Indexes, snapshots, and the live tail are rebuildable and non-authoritative.
Startup preflights authoritative storage before mutation; only a verified
incomplete final tail may be truncated. Retention advances the durable log-start
boundary before deleting inventoried user artifacts and never reuses offsets;
system logs are not user-retained.

Storage tests should use `t.TempDir()` and never write to a real user data
directory. `Store.Open` owns the data directory through its stable `LOCK`
file; do not remove, replace, or truncate that file. Treat complete corrupt
batches as errors and preserve the conservative recovery rules documented in
`PROGRESS.md`.

Partition disk admission is class-aware: user partitions charge the user
ledger via `reserveDiskUser`, while the reserved system partitions
(`ClusterMetadataTopicID`, `ConsumerOffsetsTopicID`) charge the protected
control ledger via `reserveDiskControl` so fencing, commits, and retention
boundary events survive user-stop pressure. Route new append paths through
`Partition.reserveDisk` rather than either helper directly, and preserve the
`catalog.store`/`offsets.store` back-references set by `OpenWithOptions`.
Tests exercising pressure can swap `Store.disk.probe` under the ledger lock;
see `disk_pressure_test.go` for the pattern.

## CI/CD and Repository Automation

Pull requests and pushes to `main` run `.github/workflows/ci.yml` on Linux with
Go 1.26, source copyright-header checks, formatting, module-tidy, vet, shuffled
tests, race tests, and package coverage. Fuzzing and performance evidence are
manual workflows; use `BENCHMARKS.md` for performance commands, environment
capture, and interpretation; use the opt-in soak settings documented above
instead of running the soak in ordinary CI. Linux is the only currently qualified
platform, so do not add a cross-platform matrix without equivalent lock and
disk-pressure implementations.

All third-party GitHub Actions must be pinned to full commit SHAs and workflows
must retain least-privilege permissions. Release preparation and publication are
manual and pre-v1, with publication protected by the `release` environment. Do
not create, move, reuse, or delete release tags manually.

## Release Sequencing

The immediate goal is a pre-v1 release; do not block current hardening on
freezing the eventual v1 contract. The ingress adapter uses the released
`github.com/ayeshLK/lib-disruptor v0.6.0`. Future dependency upgrades remain
subject to the same adapter, durability, cancellation, lifecycle, and
performance requalification gates.

`PROGRESS.md` and `DEPENDENCY_AUDIT.md` are intentionally ignored local
working notes and must not be staged or committed. `coverage.out` and local
benchmark output are also not commit artifacts. Confirm these remain excluded
before using broad staging commands.
