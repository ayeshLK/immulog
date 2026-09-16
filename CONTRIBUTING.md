# Contributing

Thank you for helping improve `immulog`.

## Documentation

Start with the [README](README.md) for the project scope and quick start. The
[usage guide](docs/usage.md) covers the public API, while the [production
guide](docs/production.md) covers durability, recovery, capacity, retention,
and operations. Read [Benchmark evidence](BENCHMARKS.md) before running or
interpreting performance measurements. Please also follow the [Code of Conduct](CODE_OF_CONDUCT.md)
when participating in issues, pull requests, and reviews.

## Development setup

Install Go 1.26 or newer. The repository is a Linux-focused, single-node Go
module; non-Linux locking and disk-pressure implementations are not currently
qualified.

## Validate changes

Run the complete suite from the repository root:

```sh
gofmt -w api/*.go storage/*.go perf/benchmarks/*.go perf/soak/*.go
go mod tidy
go vet ./...
go test -shuffle=on ./...
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

For parser and recovery changes, run the relevant fuzz target. Release
qualification runs each target for at least 60 minutes:

```sh
go test ./storage -run '^$' -fuzz=FuzzDecodeBatch -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzDecodeSegmentHeader -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzPreflightSystemLogSegment -fuzztime=60m -parallel=1
```

Performance evidence is collected by the manual workflow or with the
repeated-sample commands in [BENCHMARKS.md](BENCHMARKS.md). For a quick local
check:

```sh
go test ./perf/benchmarks -run '^$' -bench . -benchmem -benchtime=1s -count=1
```

Record the host, filesystem, payload, batching, concurrency, and sample
parameters with any result; do not compare the one-record durability case with
batched throughput.

The mixed workload soak is opt-in and must use a dedicated directory:

```sh
IMMULOG_SOAK=1 IMMULOG_SOAK_DURATION=20s \
  go test ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout=90s
```

Do not commit `coverage.out`, local benchmark output, or the ignored
`PROGRESS.md` and `DEPENDENCY_AUDIT.md` working notes.

## Testing expectations

Add deterministic regression coverage beside changed storage behavior. Avoid
sleep-only assertions; use bounded deadlines and explicit barriers for
concurrency. Storage tests use `t.TempDir()` and never write to a real user
data directory.

## Commits and pull requests

Use concise imperative Conventional Commit subjects such as `fix: ...`,
`feat: ...`, or `test: ...`; keep unrelated changes separate. Pull requests
should explain observable behavior, affected packages, relevant plan sections,
exact validation commands, and compatibility, durability, or on-disk impact.
Workflow changes must preserve least-privilege permissions and pin third-party
Actions to full commit SHAs.

## Releases

The project uses pre-v1 semantic-version tags such as `v0.1.0`. Release Please
owns `CHANGELOG.md`; maintainers manually prepare a release PR and publish it
through the protected `release` environment after review. Do not create, move,
reuse, or delete release tags manually.
