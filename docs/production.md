# Production guide

`immulog` is a local, filesystem-backed storage library. This guide covers the
operational decisions that matter when embedding it in an application. Read
the [usage guide](usage.md) for API examples and the [security policy](../SECURITY.md)
for reporting and security boundaries.

> [!WARNING]
> Linux is the only currently qualified platform. The project is pre-v1 and
> does not provide a network protocol, replication, failover, or multi-process
> coordination.

## Use a dedicated data directory

Give each store a dedicated directory on a filesystem owned by the
application. `storage.Open`:

1. creates the directory if necessary;
2. resolves its canonical path;
3. acquires the stable `LOCK` file;
4. validates and replays the system logs; and
5. opens partitions on demand.

Only one store may own a data directory at a time. Keep the directory and its
parent on the storage device whose durability characteristics you intend to
rely on. Do not remove, replace, truncate, or edit `LOCK`, segment files, index
files, snapshots, or topic-preparation markers while the store is running.

A clean shutdown is:

```go
if err := store.Close(); err != nil {
	return err
}
```

`Store.Close` fences new work, drains commands already admitted to their
sequencers, closes partitions, and releases directory ownership last. Stop and
join application appenders before calling it. A repeated close is safe, but
new work must not be started after shutdown begins.

## Understand the durability boundary

An acknowledged append has passed the complete batch write, file sync, and
required namespace synchronization path. This is a storage durability
boundary, not a guarantee that arbitrary hardware honors flush requests. Use a
filesystem and device configuration appropriate for the data's value, and
manage filesystem permissions and at-rest encryption at the deployment layer.

Cancellation and I/O failures can produce an unknown result:

- before admission, cancellation can prevent the append from starting;
- after ingress publication, cancellation may return
  `api.ErrAppendOutcomeUnknown` while the writer finishes; and
- a persistence failure can make a partition unavailable and require
  reconciliation or reopening.

Never assume an unknown append was rolled back, and never reuse its offset
blindly. Preserve an application-level request identity and make retry logic
idempotent or reconcile the durable log before retrying.

The same rule applies to catalog and consumer-offset mutations. An unknown
metadata or commit outcome is not evidence that the durable event did not
exist.

`BatchLinger` is zero by default and disables timed acquisition. A positive
value allows the ingress processor to collect newly published contiguous
requests after the first available request, up to the configured processor
batch limit. Appends remain synchronous: successful calls return only after the
formed durable batch is written and synchronized.

## Plan capacity before opening traffic

`PartitionOptions` defines persisted writer and retention settings for a new
catalog topic. The important bounds are:

- `SegmentBytes` controls the maximum segment size and affects roll frequency;
- `BatchBytes` and `BatchRecords` bound encoded batches;
- `RecordBytes` bounds one record;
- `InFlightBytes`, `InFlightRecords`, and `AdmissionWaiters` bound admission;
- `BatchLinger` bounds the ingress processor's timed acquisition window; and
- `TailSlots` and `TailBytes` enable an optional, rebuildable in-memory tail.

`StoreOptions` applies instance-only limits for topics, user partitions, open
partitions, system-log history, consumer groups, progress keys, tail bytes,
open segment file descriptors, and disk headroom. These limits are finite
safeguards. Lowering a limit on reopen does not delete existing durable state;
it may instead refuse new growth or opening work until a larger operating
profile is selected.

`MaxOpenSegmentFiles` bounds descriptors held for sealed (immutable) segments
across the whole store; each open partition separately keeps one writer handle
for its active segment. A partition's log length does not otherwise bound
descriptor usage: a reader briefly exceeding the cap while a segment is pinned
is expected and does not fail the read.

Size a deployment for the worst expected in-flight payload and burst, not only
average throughput. Include:

- the largest record and encoded batch;
- concurrent producers and their admission budget;
- segment headers, batches, and index artifacts;
- catalog and consumer-offset history;
- retention cleanup debt; and
- filesystem and inode headroom outside the store's own logical bytes.

Use `Stats` to observe actual bounded state and measure representative payloads
on the target filesystem. A larger limit does not remove backpressure from a
permanently slow consumer or a full device.

## Treat disk pressure as an admission contract

Disk admission uses byte and inode safety floors when the platform can measure
them. User partitions consume the user ledger; the reserved catalog and
consumer-offset partitions use protected control headroom so metadata, fencing,
commits, and retention-boundary events can still complete under user-stop
pressure.

Applications should:

- monitor `StoreStats.DiskPressure` and rejected-append counters;
- react to `api.ErrDiskPressure` as an operational signal, not as a transient
  validation error;
- leave space for cleanup, compaction-free segment replacement, and system-log
  growth; and
- avoid treating logical log bytes as the complete filesystem usage.

Keep the data directory on a volume with predictable free-space and inode
behavior. If the probe cannot establish a safe capacity view, admission fails
closed rather than pretending that capacity is available.

## Retention and disk capacity

Retention is configured per catalog-owned user topic. It can use time, size,
or both policies. It works on closed segments; an active segment may be rolled
when the policy requires an eligible boundary.

The retention sequence is deliberately ordered:

1. inspect the retained segments and build an exact retirement proposal;
2. append and sync a catalog event that advances the durable log start (`L`);
3. publish the new retained anchor; and
4. remove the inventoried `.log`, `.index`, and `.timeindex` artifacts and sync
   the partition directory.

This means expired offsets become unavailable when the durable boundary moves,
even if physical cleanup later fails. Cleanup debt remains observable through
`StoreStats.Cleanup` and is retried by later maintenance. Pending artifacts do
not count as reclaimed capacity until they are actually removed and synced.

System logs are never user-retained. Offsets are never reused, and a retention
failure must not make an expired offset readable again. Do not delete old
segments manually to create space; that removes the evidence the catalog uses
to validate its retained anchor.

When deterministic maintenance is useful in an operator or test command, call:

```go
if err := store.RunRetention(ctx); err != nil {
	return err
}
```

Configured retention also starts the store's bounded maintenance worker. Treat
`RunRetention` errors as operational evidence and inspect `Store.Stats` before
trying to change policy.

## Operate consumers at least once

Managed consumers and consumer groups persist assignment and commit state in a
reserved system log. They provide at-least-once delivery, not exactly-once
application effects.

A safe processing order is:

1. poll a bounded prefix;
2. process or durably hand off the records in application-owned memory;
3. commit the returned `NextOffset`; and
4. retry idempotently after a restart or assignment loss.

A consumer replacement fences the old assignment. `ErrAssignmentLost` means
the old handle must stop processing; it is not a signal to continue polling.
`ProgressTimeout` must be longer than the maximum admitted poll wait and long
enough for the application's scheduling and processing budget.

Watch both delivery and commit lag. A low delivery lag with a high commit lag
can indicate slow application processing or a missing commit. A commit cannot
advance past the delivered prefix, and closing a consumer does not auto-commit
its delivered records.

## Observe bounded diagnostics

`Store.Stats` is a bounded point-in-time operational summary. It includes:

- lifecycle phase and close timing;
- open partitions and active assignments;
- catalog and offsets history headroom;
- tail-cache use;
- append outcomes and rejection classes;
- write, file-sync, and namespace-sync latency histograms;
- retention cleanup debt; and
- disk-pressure state.

`Partition.Stats` provides per-partition durable end (`H`), log start (`L`),
logical bytes, in-flight admission, lifecycle state, tail use, and append
outcomes. Consumer stats expose delivery lag and commit lag for one assignment
at a time. These snapshots never retain record payloads and are not a
transactional view across partitions.

Export the values through the application's existing metrics or diagnostics
system. Keep the raw snapshots bounded and avoid logging record keys or values
as part of routine health checks.

## Reopen and recover conservatively

A reopen validates authoritative storage before making recovery decisions. The
filesystem log, not an index, tail cache, or projection snapshot, is the source
of truth.

Recovery may repair only a verified incomplete final batch tail. It refuses to
mutate the directory for complete checksum failures, invalid batch identities,
unsupported formats, offset gaps, ambiguous trailing bytes, unexpected files,
or mismatched retained anchors. A refused recovery returns a corruption or
unsupported-format error and preserves authoritative bytes for investigation.

After a process crash or an unknown persistence outcome:

1. stop all writers that still have access to the directory;
2. preserve the directory for inspection or backup;
3. reopen the store and inspect the returned error and diagnostics;
4. reconcile application-level request identities and consumer effects; and
5. resume only after the store and affected partitions are available.

The qualification suite models process termination and injected filesystem
failures. It does not prove behavior for every torn write or power-loss mode;
validate the target filesystem and device separately.

## Backups and file handling

There is no general backup/restore protocol in this release. `SaveSnapshots`
creates replaceable projection caches for faster startup; it is not a backup
and cannot replace the authoritative log.

For a file-level backup, stop the store cleanly before copying the complete
data directory, or use a filesystem snapshot whose consistency guarantees are
understood and tested by the deployment. Preserve the directory structure and
all authoritative files. Do not restore by copying selected segments, deleting
`LOCK`, or editing catalog metadata by hand.

## Security boundary

`immulog` does not provide network transport, authentication, authorization,
segment-file encryption, or protection against an operator who can modify the
data directory. The deployment owns:

- filesystem ownership and permissions;
- key management and at-rest encryption;
- network exposure around the application;
- backup access controls; and
- storage hardware and mount options that determine flush behavior.

Malformed records, corrupted files, unbounded caller workloads, ignored
cancellation, stale consumer handles, and incorrect lock or retention handling
can affect availability or data integrity. Report suspected vulnerabilities
through the private process in [SECURITY.md](../SECURITY.md), not a public issue.

## Common mistakes

- Sharing one data directory between processes or stores.
- Removing or replacing the stable `LOCK` file.
- Treating `ErrAppendOutcomeUnknown` as proof that no record was written.
- Retrying unknown operations without an application-level identity.
- Deleting segment or index files manually to reclaim space.
- Treating snapshots, indexes, or the in-memory tail as authoritative.
- Assuming retention makes physical bytes disappear synchronously.
- Assuming consumer commits provide exactly-once external effects.
- Keeping record byte slices after handing them to an asynchronous component
  without an application-owned copy.
- Running unqualified non-Linux locking or disk-pressure deployments as if they
  had the same guarantees.
- Calling `Store.Close` while application writers can still append.

## Operational validation

Before a production rollout, run the normal checks and the workload-specific
checks that match the deployment. See [Benchmark evidence](../BENCHMARKS.md)
for reproducible commands, environment capture, interpretation guidance, and
qualification limits:

```sh
go test ./...
go vet ./...
go test -race ./...
go test ./storage -run '^$' -fuzz=FuzzDecodeBatch -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzDecodeSegmentHeader -fuzztime=60m -parallel=1
go test ./storage -run '^$' -fuzz=FuzzPreflightSystemLogSegment -fuzztime=60m -parallel=1
go test ./perf/benchmarks -run '^$' -bench .
```

The mixed workload soak is opt-in and must use a dedicated pre-sized directory:

```sh
IMMULOG_SOAK=1 IMMULOG_SOAK_DURATION=20s \
  go test ./perf/soak -run '^TestMixedWorkloadSoak$' -count=1 -timeout=90s
```

Record the filesystem, storage device, Go version, payload sizes, topic and
partition counts, retention policy, operating limits, and observed diagnostics.
Benchmark results are workload- and machine-specific evidence, not portable
throughput guarantees.

## Explicit non-goals

The current release slice does not include:

- network clients or a broker protocol;
- replication, consensus, or failover;
- multi-process writers or shared-directory coordination;
- topic deletion or arbitrary on-disk migration tools;
- exactly-once application processing; or
- a promise of power-loss behavior beyond the configured filesystem and device.
