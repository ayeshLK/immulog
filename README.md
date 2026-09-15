# immulog

`immulog` is a local, durable, append-only, partitioned event-log library for Go.

This repository follows the companion plan at
`/home/ayesh/projects/samples/immutable-event-log/IMPLEMENTATION_PLAN.md`.
Phases 0–6 and the core Phase 7 retention protocol are implemented. Operational capacity hardening, replication, and networking remain later work.

## Current status

- Public value types and domain errors live in `api/`.
- Version-1 scalar, record, batch, segment, event, index, and snapshot encoding
  lives in `storage/` with explicit little-endian fields and CRC32C.
- `Store.Open` initializes and replays `system/cluster-metadata/0` before
  `system/consumer-offsets/0`; StoreID and segment anchors are durable facts.
- Before any recovery mutation, startup read-only preflights both system logs,
  catalog-owned topic storage, and complete segment/batch identities; only a
  verified incomplete final tail may be repaired later by partition open.
- Durable topic-preparation markers preserve recognized unpublished creation
  artifacts for reconciliation, while ambiguous unreferenced storage fails closed.
- Catalog mutations use bounded context-aware admission. Cancellation before
  sequencer ownership is definitive; later cancellation returns an explicit
  unknown outcome while the owned mutation resolves durably.
- `CreateTopic`, `DescribeTopic`, `ListTopics`, and `OpenTopic` use the replayed
  catalog. Topic IDs and creation-time partition configuration are immutable.
- Projection snapshots are replaceable caches with bounded streaming validation
  and observable per-log diagnostics; full authoritative replay remains recovery.
- Each active partition uses a bounded multi-producer ingress ring with one
  terminal durable writer; appends retain explicit cancellation and failure
  outcomes while Close seals and drains the ring.
- Raw readers, single-key consumers, and `OpenConsumerGroup` membership snapshots
  fetch bounded caller-owned records. Group snapshots persist canonical complete
  assignments, fresh sessions, fenced next offsets, and durable transition causes.
  Membership changes are explicit complete snapshots that eagerly fence old handles.
- An optional durable tail cache uses per-partition `TailSlots`/`TailBytes` and
  the Store-wide `StoreOptions.TailBytes` cap supplied through `OpenWithOptions`.
  `StoreOptions` also enforces finite catalog topic, total user-partition,
  active user-partition writer/ring, consumer-group/progress-key, and
  per-system-log logical-history limits; none rewrites durable state. Existing
  system history remains readable after a lower history ceiling is selected,
  but new catalog or commit growth is refused. Exhausted or contended cache
  capacity drops an offer; Fetch falls back to segments at the requested cursor,
  while bounded managed `Poll` waits wake on durable appends, failure, or Close.
  `Store.Stats` and `Partition.Stats` expose bounded pull-based L/H, logical
  bytes, system-history headroom, projection counts, admission, tail, and
  lifecycle snapshots without retaining payloads.
- The opt-in `TestMixedWorkloadSoak` exercises mixed append/fetch/commit traffic,
  cancellation and overload, retention/rolling, group churn, tail-cache paths,
  snapshots, clean reopen cycles, and an independent per-partition oracle. It
  writes bounded verifier checkpoints when `IMMULOG_SOAK_DIR` is supplied.
- Catalog topics persist explicit time/size retention policy. A bounded maintenance
  worker rolls eligible idle active segments, records `PartitionLogStartAdvanced`
  before publishing L, and removes only its inventoried `.log`, `.index`, and
  `.timeindex` artifacts. Reopen validates the retained anchor and safely resumes
  authorized partial cleanup without reviving expired offsets.
- Replication and networking remain outside the implemented slice.

See [`PROGRESS.md`](PROGRESS.md) for the implementation checkpoint.

Run the checks from the repository root:

```sh
go test ./...      # unit and integration tests
go test ./storage -run '^$' -fuzz=FuzzDecodeBatch -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzDecodeSegmentHeader -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzPreflightSystemLogSegment -fuzztime=60m -parallel=1
go test ./perf/benchmarks -run '^$' -bench .
IMMULOG_SOAK=1 IMMULOG_SOAK_DURATION=20s go test ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout=90s
# For qualification, use a pre-sized dedicated directory and set duration=24h.
go vet ./...       # static analysis
go test -race ./... # race detector
```

### Mixed-workload soak

The release soak is opt-in and writes only to its supplied data directory. A
short smoke is:

```sh
IMMULOG_SOAK=1 IMMULOG_SOAK_DURATION=20s \
  go test ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout=90s -v
```

For qualification, set `IMMULOG_SOAK_DIR` to a pre-sized dedicated volume and
run with `IMMULOG_SOAK_DURATION=24h` and a test timeout longer than 24 hours.
The harness performs clean reopen cycles and persists bounded verifier offsets;
separate invocations can resume clean chunks against the same directory. It
does
not model process crashes, torn writes, or power-loss storage behavior; focused
storage tests cover process termination and injected filesystem failures.

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
