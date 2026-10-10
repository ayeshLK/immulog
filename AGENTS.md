# Repository Guidelines

## Project Structure & Module Organization

`immulog` is a Go 1.25+ module for a local, durable, append-only event log.
Public contracts and stable domain errors are in `api/`; encoding, segment
management, locking, partitions, and the store live in `storage/`. Core
correctness tests are co-located with their package (`*_test.go`); benchmark
and long-running evidence harnesses live under `perf/`. `README.md` describes
the current scope, `docs/spec/spec.md` is the normative behavioral and
persistence contract, `docs/production.md` owns deployment and cold-backup
guidance, `docs/durability-qualification.md` records qualified platform claims,
and `BENCHMARKS.md` is the source of truth for performance methodology and dated
evidence. `docs/coordination.md` is a future architecture proposal, not current
runtime behavior. `PROGRESS.md` records the ignored local implementation
checkpoint. Keep changes within the existing package boundaries; do not add
network or replication code to this local single-process slice.

The `Store` owns the canonical directory, stable `LOCK`, catalog and consumer
offset system partitions, user partitions, retention, snapshots, and disk
admission. Catalog metadata is itself an append-only system log; explicit
system-log compaction can replace obsolete prefixes with authoritative
checkpoints and absolute-offset suffixes. Each partition uses bounded
multi-producer ingress with one terminal durable writer; segments and selected
system checkpoints are authoritative, while indexes, projection snapshots, and
live-tail caches are rebuildable. Managed consumers are same-process,
at-least-once assignments with durable commits and fencing.

## Architecture Map

The exported surface is small (`Store`, `Partition`, `Reader`, `Consumer`,
`GroupConsumer`); nearly all behavior lives in unexported collaborators inside
`storage/`. The following traces are the fastest way to orient before editing.

**On-disk layout** (rooted at the store directory):

```
LOCK                                  stable ownership lock (lock_linux.go)
system/cluster-metadata/0/            legacy catalog log (__cluster_metadata)
  projection.snapshot                 optional non-authoritative cache
system/consumer-offsets/0/            legacy offsets log (__consumer_offsets)
  projection.snapshot                 optional non-authoritative cache
system/metadata/active-manifest       selected compacted generation, when present
system/metadata/generations/<gen>/    immutable generation authority
  manifest                            binds both system checkpoints and suffixes
  cluster-metadata/0/checkpoint       catalog projection at an absolute revision
  consumer-offsets/0/checkpoint       offsets projection at an absolute revision
  <system-name>/0/<offset>.log        post-checkpoint absolute-offset suffix
topics/<topic-uuid>/<partition>/      user partitions
  00000000000000000000.log            segment, base offset zero-padded to 20
  00000000000000000000.index          sampled offset index (rebuildable)
  00000000000000000000.timeindex      sampled time index (rebuildable)
  .topic-preparation-v1               crash-safe topic creation marker
```

`format.go` holds the wire constants (`SegmentMagic`, `BatchMagic`, header
sizes, CRC32C) and the `EventType` enum; `codec.go` encodes/decodes batches.
All integers are little-endian. Only `filesystem.go`'s `fileSystemOps` seam
touches the OS, which is how fault-injection tests simulate write/sync
failures.

**Append path:** validate and reserve capacity before publication; copy caller
data; serialize offsets through one terminal writer; write and synchronize
authoritative bytes before acknowledging. Sync failures produce
`ErrAppendOutcomeUnknown` and fence the partition. `ingress.go` is the only
adapter over `lib-disruptor`.

**Read path:** enforce the retained `[L,H]` range, use the optional bounded live
tail when available, and otherwise scan authoritative segments using rebuildable
index hints. `Reader` is a cursor over `Fetch`.

**Metadata as logs:** catalog and consumer offsets are authoritative append-only
system partitions replayed into projections during `Open`. In the ordinary
non-compacted path, current bootstrap replays the authoritative logs before it
validates optional `projection.snapshot` files; snapshot mismatches are
diagnostic and reported by `Store.SnapshotDiagnostics`, not startup blockers
after successful replay. Do not confuse these replaceable snapshots with the
authoritative checkpoints selected by a compaction manifest. Compacted recovery
decodes the selected checkpoints and replays only their suffix logs.

**Consumers:** `consumer.go` implements one assignment; `group_consumer.go`
implements same-process multi-partition membership. Both use durable
generation/session fencing and synchronous offset commits. A stale consumer
close must never unregister its replacement.

**Cross-cutting:** retention advances the durable log-start boundary before
deletion; disk pressure uses class-aware byte/inode admission; diagnostics,
limits, snapshots, indexes, and tails are bounded and rebuildable where noted.
`StoreStats.RetentionMaintenance` reports the latest bounded background attempt,
success, and failure, while `StoreStats.SystemLogMaintenance` reports compaction
generation, reclaimed bytes, cleanup debt, and the latest bounded failure.

## Build, Test, and Development Commands

This is a single Go module supporting Go 1.25 and Go 1.26 using the standard
Go toolchain; there is no separate build system or lint configuration. Linux
is the only currently qualified durability platform. Native macOS and Windows
jobs provide build/test coverage, but their filesystem durability qualification
is pending; see `docs/durability-qualification.md`.

Run these from the repository root:

```sh
gofmt -w \
  api/*.go storage/*.go \
  perf/analyze/*.go perf/benchmarks/*.go perf/benchmarks/analyze/*.go \
  perf/history/*.go perf/metrics/*.go perf/soak/*.go
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
perf/history/run.sh \
  --run-dir "$HOME/immulog-history-$(date +%Y%m%d-%H%M%S)" \
  --records 10000,100000,1000000 \
  --catalog-topics 100 \
  --consumer-commits 1000 \
  --runs 3 \
  --snapshots
perf/soak/run.sh --profile mixed --duration 20s --timeout 90s --minimum-free-bytes 0 --minimum-open-files 0
perf/soak/run.sh --profile mixed --duration 20s --timeout 90s --compaction-interval 500ms --minimum-free-bytes 0 --minimum-open-files 0
```

There is no separate build script; `go test ./...` compiles all packages.
`perf/benchmarks/run.sh` owns reproducible microbenchmark profiles and captures
raw output and host metadata; use `smoke` for quick checks, `standard` for
comparisons, and `qualification` for release evidence. The soak test is
disabled unless `IMMULOG_SOAK=1`; use `IMMULOG_SOAK_DIR`
for a dedicated persistent directory. `IMMULOG_SOAK_DURATION`, `IMMULOG_SOAK_WARMUP`,
`IMMULOG_SOAK_REOPEN_INTERVAL`, `IMMULOG_SOAK_APPEND_INTERVAL`,
`IMMULOG_SOAK_PRODUCER_RATE`, `IMMULOG_SOAK_SAMPLE_INTERVAL`,
`IMMULOG_SOAK_SAMPLE_LIMIT`, `IMMULOG_SOAK_CHURN_INTERVAL`,
`IMMULOG_SOAK_COMPACTION_INTERVAL`, `IMMULOG_SOAK_METRICS_FILE`, and
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

`perf/history/run.sh` owns isolated recovery-scaling matrices. It can vary user
record/segment history, total catalog topics, durable consumer commits, and
valid/invalid projection snapshots; `--runs` creates independent repetitions.
Its evidence root records configuration, commands, statuses, commit/worktree
state, Go/host details, and filesystem metadata. “Cold” means a fresh `Store`
instance; the runner does not evict operating-system page caches after creating
the history. Use a clean recorded commit and preserve raw artifacts outside the
repository.

### Performance and soak evidence

`BENCHMARKS.md` is authoritative for performance methodology and dated
results. The benchmark-only `perf/metrics` package provides rate, bounded
histogram, and exact-sample helpers; append benchmarks report
`producer-records/s` and `producer-bytes/s`, while fetch benchmarks report
`consumer-records/s` and `consumer-bytes/s`.

The long-history matrix recorded on 2026-10-10 used three repetitions at
10,000, 100,000, and 1,000,000 records with 100 catalog topics and 1,000
consumer commits. At one million records and 5,310 segments, baseline median
reopen was 343.55ms on the recorded Linux/ext4 host. Valid projection snapshots
showed no repeatable recovery advantage, consistent with replay-before-
validation in the ordinary bootstrap path. Treat the result as recovery-scaling
and snapshot-validation evidence, not a device-independent threshold or proof
of disk-cold startup. See `BENCHMARKS.md` for ranges and full provenance.

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
and completion/failure status. Schema version 2 records configured and actual
warmup durations, separates measurement duration from total cleanup time, and
records measure, drain, verify, and cleanup phases. Ingress `acknowledged_bytes`
and consumer
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

Keep `BENCHMARKS.md` reader-oriented: include methodology, runnable commands,
newest comparable results first, concise environment tables, and explicit
“not captured” values. Do not commit raw run artifacts or local evidence paths.
Compare runs using the same commit, seed, profile, workload, Go version, and
filesystem. `warmup_nanos` is the configured warmup; `warmup_elapsed_nanos` is
the measured elapsed warmup including reopen/verification. The analyzer marks
reports invalid when elapsed warmup is missing or shorter than configured.
Sustained evidence with positive backlog growth or assignment loss is not a
stable capacity result.

### Durability qualification

`docs/durability-qualification.md` is the source of truth for platform
qualification status and evidence boundaries. The recorded Linux baseline at
commit `8728522540ccdc9e258ae84a0e7703eec51e9514` passed shuffled storage tests,
race-enabled storage tests, ten repetitions of the process-crash/persistence-
boundary suite, all-package tests, and vet. This validates deterministic fault
injection and abrupt process termination on Linux/ext4; it does not prove
physical power-loss behavior.
Native macOS and Windows durability qualification remains incomplete. Preserve
the tested commit, host/filesystem assumptions, exact commands, exit statuses,
and raw artifacts outside the repository for future qualification runs. Issue
#67 remains on hold until native macOS and Windows evidence is available; a
Linux Docker container is not a substitute for their native kernel/filesystem
durability behavior.

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
concurrency. Keep microbenchmarks in `perf/benchmarks`, recovery-scaling
workloads in `perf/history`, and opt-in long-running soak workloads in
`perf/soak`; they must use public package APIs rather than production-private
test seams. Run formatting, module tidy, vet, shuffled tests, race tests, and
coverage before submitting changes.

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
back. Legacy system logs or a manifest-selected checkpoint plus suffix are
authoritative. Indexes, projection snapshots, and the live tail are rebuildable
and non-authoritative. Startup preflights authoritative storage before mutation;
only a verified incomplete final tail may be truncated. Retention advances the
durable log-start boundary before deleting inventoried user artifacts and never
reuses offsets; system logs are not user-retained.

Projection snapshots are bound to their log by a projection-prefix digest and
are safe only as replaceable, non-authoritative caches. Keep system-log appends
and snapshot builds under the store mutex. `SaveSnapshots` should build under
the store mutex and publish outside it under `snapshotMu`; do not hold the store
mutex over file write and sync. `CompactSystemLogs` is different: it explicitly
publishes one authoritative generation for both system logs, preserves absolute
revisions and fencing state, requires protected temporary byte/inode headroom,
and has no automatic production worker. Preserve its lock ordering, manifest
commit point, unknown-outcome fencing, and old-generation cleanup debt.

The supported file-level backup is a cold, quiesced copy after successful
`Store.Close`. Copy and restore the complete directory, including the stable
`LOCK`, system authority, topics, sidecars, snapshots, and preparation markers;
never treat a projection snapshot as a backup. Restore into an isolated
destination and validate it by opening and checking catalog, retained bounds,
records, and consumer progress. Live filesystem snapshots require separately
qualified whole-directory crash consistency. Keep
`TestColdCopyRestorePreservesAuthoritativeState` as a compatibility gate.

A partition keeps a live descriptor only for its active segment. Sealed
segments are reopened through the bounded `segmentFileCache`; new code reading
segment bytes must use `Partition.acquireSegmentFile` or `readSegmentFile`
rather than assuming `segment.file` is non-nil. Preserve the sidecar
checkpointing/`indexDirty` behavior when changing segment lifecycle code.

Storage tests should use `t.TempDir()` and never write to a real user data
directory. `Store.Open` owns the data directory through its stable `LOCK`
file; do not remove, replace, or truncate that file. Treat complete corrupt
batches as errors and preserve the conservative recovery rules documented in
`PROGRESS.md`.

Disk admission is class-aware: user partitions use the user ledger while the
reserved system partitions use protected control capacity. Route new append
paths through `Partition.reserveDisk`; preserve the catalog/offsets store
back-references established by `OpenWithOptions`. Pressure tests may replace
`Store.disk.probe` under the ledger lock.

## CI/CD and Repository Automation

Pull requests and pushes to `main` run `.github/workflows/ci.yml` on Linux with
Go 1.25 and Go 1.26, source copyright-header checks, formatting, module-tidy,
vet, shuffled tests, race tests, and package coverage. Fuzzing and performance
evidence are manual workflows; use `BENCHMARKS.md` for performance commands,
environment capture, and interpretation; use the opt-in soak settings documented
above instead of running the soak in ordinary CI. Linux is the only currently
qualified durability platform. Native macOS and Windows CI are build/test checks,
not durability qualification; do not expand platform claims without the matrix
and evidence requirements in `docs/durability-qualification.md`.

All third-party GitHub Actions must be pinned to full commit SHAs and workflows
must retain least-privilege permissions. Release preparation and publication are
manual and pre-v1, with publication protected by the `release` environment. Do
not create, move, reuse, or delete release tags manually. Dispatch-controlled
performance inputs must pass through environment variables, be validated before
runner invocation, and never be interpolated directly into shell source;
`scripts/check-performance-workflow.sh` protects this rule.

## Release Sequencing

The immediate goal is a pre-v1 release; do not block current hardening on
freezing the eventual v1 contract. The ingress adapter uses the released
`github.com/ayeshLK/lib-disruptor v0.6.0`. Future dependency upgrades remain
subject to the same adapter, durability, cancellation, lifecycle, and
performance requalification gates.

As of 2026-10-10, the completed local hardening includes system-log compaction,
background-retention outcome diagnostics, the long-history recovery harness,
and the tested cold backup/restore procedure. The remaining tracked work is:

- #67: native durability qualification, on hold pending macOS and Windows
  evidence;
- #70: adopter decision guidance plus a compiled/tested safe-restart example;
  this is independent local documentation/example work;
- #11 and #36: future brokered coordination and cluster-mode design; and
- #82: the narrower HA replicated-persistence ADR and deterministic model.

The distributed items remain proposals. Do not add production transport,
replication, consensus, or shared-directory multi-process behavior to the
current storage slice without a separately reviewed architecture milestone.

`PROGRESS.md` and `DEPENDENCY_AUDIT.md` are intentionally ignored local
working notes and must not be staged or committed. `coverage.out` and local
benchmark output are also not commit artifacts. Confirm these remain excluded
before using broad staging commands.
