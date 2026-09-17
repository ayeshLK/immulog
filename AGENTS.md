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

## Build, Test, and Development Commands

Run these from the repository root:

```sh
go test ./...          # run all package tests
go vet ./...           # report suspicious Go constructs
go test -race ./...   # exercise tests with the race detector
gofmt -w api/*.go storage/*.go perf/benchmarks/*.go perf/soak/*.go
go test ./storage -run TestName
```

Long-running or opt-in checks:

```sh
go test ./storage -run '^$' -fuzz=FuzzDecodeBatch -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzDecodeSegmentHeader -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzPreflightSystemLogSegment -fuzztime=60m -parallel=1
go test ./perf/benchmarks -run '^$' -bench .
perf/soak/run.sh --duration 20s --timeout 90s
```

There is no separate build script; `go test ./...` compiles all packages.
The soak test is disabled unless `IMMULOG_SOAK=1`; use `IMMULOG_SOAK_DIR`
for a dedicated persistent directory. `IMMULOG_SOAK_DURATION`,
`IMMULOG_SOAK_REOPEN_INTERVAL`, `IMMULOG_SOAK_APPEND_INTERVAL`, and
`IMMULOG_SOAK_SEED` control resumable runs.

## Coding Style & Naming Conventions

Use standard `gofmt` formatting, tabs for Go indentation, and idiomatic Go
names: exported identifiers use PascalCase and unexported helpers use
camelCase. Preserve `errors.Is` compatibility for public domain errors in
`api/errors.go`. Keep on-disk formats explicit and little-endian, validate
inputs at package boundaries, and document exported types and methods.

## Testing Guidelines

Tests use Go's standard `testing` package and should be named
`Test<TypeOrBehavior>`. Add correctness regression tests beside the
implementation, especially for malformed bytes, CRC/checksum failures, offset
continuity, reopen/recovery behavior, locking, and nil-versus-empty payload
semantics. Keep benchmarks in `perf/benchmarks` and opt-in long-running soak
workloads in `perf/soak`; they must use public package APIs rather than
production-private test seams. Run the full test, vet, and race commands before
submitting changes.

## Commit & Pull Request Guidelines

Use concise, imperative commit subjects (for example, `storage: validate
segment recovery`) and keep unrelated changes separate. Pull requests should
explain the behavioral or format change, identify affected packages, link any
related issue or plan section, and include the exact verification commands
run. Call out compatibility, durability, recovery, or on-disk format impact;
include focused test details when changing storage behavior.

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
Go 1.26, formatting, module-tidy, vet, shuffled tests, race tests, and package
coverage. Fuzzing and performance evidence are manual workflows; use
`BENCHMARKS.md` for performance commands, environment capture, and
interpretation, and use the opt-in soak settings documented above rather than
running the soak in ordinary CI. Linux is the only currently qualified
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

`PROGRESS.md` and `DEPENDENCY_AUDIT.md` are local working notes and must not be
staged or committed. Confirm both remain excluded before using broad staging
commands.
