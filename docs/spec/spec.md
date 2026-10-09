# immulog Technical Specification

- **Specification version:** 0.1-draft
- **Describes:** the implemented pre-v1 behavior on 2026-09-28
- **Status:** living normative specification

## Contents

- [1. Status and conformance](#1-status-and-conformance)
- [2. Normative language](#2-normative-language)
- [3. Scope and non-goals](#3-scope-and-non-goals)
- [4. Model and terminology](#4-model-and-terminology)
- [5. Identifiers, records, and ownership](#5-identifiers-records-and-ownership)
- [6. Store ownership, layout, and lifecycle](#6-store-ownership-layout-and-lifecycle)
- [7. Catalog and topic lifecycle](#7-catalog-and-topic-lifecycle)
- [8. Partition append contract](#8-partition-append-contract)
- [9. Read contract](#9-read-contract)
- [10. Managed consumers](#10-managed-consumers)
- [11. Retention](#11-retention)
- [12. Capacity and disk admission](#12-capacity-and-disk-admission)
- [13. Errors](#13-errors)
- [14. Diagnostics](#14-diagnostics)
- [15. Authoritative persistence format](#15-authoritative-persistence-format)
- [16. Rebuildable persistence formats](#16-rebuildable-persistence-formats)
- [17. Recovery and corruption](#17-recovery-and-corruption)
- [18. Concurrency and shutdown](#18-concurrency-and-shutdown)
- [19. Security and operational assumptions](#19-security-and-operational-assumptions)
- [20. Evolution and proposal process](#20-evolution-and-proposal-process)
- [Appendix A. Public API inventory](#appendix-a-public-api-inventory)
- [Appendix B. Default limits](#appendix-b-default-limits)
- [Appendix C. Conformance checklist](#appendix-c-conformance-checklist)

## 1. Status and conformance

This document is the authoritative behavioral and persistence specification for
`immulog`. The implementation and this document are required to agree. A
difference between implemented behavior and this specification is a bug in one
or the other and MUST be resolved explicitly; it MUST NOT be treated as an
undocumented extension.

The specification serves two audiences:

1. developers implementing a compatible package; and
2. contributors changing this implementation.

`immulog` is pre-v1. Public Go APIs and persisted formats can still change, but a
change is not implicit merely because the project is pre-v1. Section 20 governs
changes.

Conformance is divided into independently useful classes:

- **API conformance** implements the public data model, ownership rules,
  operations, errors, defaults, and observable lifecycle behavior.
- **Durability conformance** implements acknowledgement, synchronization,
  unknown-outcome, ordering, and conservative recovery requirements.
- **Persistence-format conformance** reads and writes the authoritative v1
  directory, segment, batch, record, configuration, and system-event formats.
- **Consumer conformance** implements durable baselines, assignments,
  generations, fencing, commits, and at-least-once delivery.
- **Operational conformance** implements exclusive directory ownership,
  bounded admission, configured operating limits, retention ordering, and
  protected control-plane capacity.

A package claiming full compatibility MUST satisfy all five classes. A tool
that only inspects segment files MAY claim persistence-format conformance
without implementing the runtime API, but MUST state that narrower scope.
Indexes and snapshots are optional accelerators and are not required for
persistence-format conformance to authoritative data. A tool that emits them
MUST use their v1 formats in section 16.

There are three separate version axes:

- this document's specification version;
- the Go module/public API version; and
- persisted format or schema versions embedded in files and records.

Advancing one axis does not automatically advance another.

Linux is the only currently qualified runtime platform. The durability contract
also depends on the selected filesystem, mount configuration, kernel, and
storage device honoring write and synchronization operations. Other platforms
are not part of the qualified deployment contract unless separately qualified.

## 2. Normative language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
and **MAY** are to be interpreted as described by RFC 2119 and RFC 8174 when
shown in uppercase.

Unless a section says otherwise:

- integers in persisted data are unsigned and little-endian;
- byte offsets in layout tables are zero-based and the end offset is exclusive;
- sizes include both endpoints' framing where the named object includes it;
- reserved bytes written by a conforming writer MUST be zero;
- a reader MUST reject nonzero reserved bytes when this specification marks
  them as validated;
- `uintN` denotes an N-bit unsigned integer;
- `int64` values are stored as their two's-complement bit pattern in a `uint64`;
- a zero 16-byte identifier is invalid unless explicitly allowed; and
- “caller-owned” means the caller may mutate the value after the call returns
  without changing storage or another result.

Text outside normative requirements may explain rationale, examples, current
implementation techniques, or operational recommendations. The requirements,
not a private implementation technique, define compatibility.

## 3. Scope and non-goals

`immulog` is an embedded, local, durable, append-only, partitioned event log.
One process owns one data directory. Topics and partitions are cataloged in an
append-only system log. Consumer group state and commits are recorded in a
second append-only system log.

The implemented scope includes:

- exclusive store ownership;
- catalog-owned user topics;
- contiguous per-partition offsets;
- synchronous durable append acknowledgement;
- bounded concurrent producer admission;
- bounded durable reads and cursor readers;
- same-process managed consumers and complete local group snapshots;
- at-least-once delivery with explicit synchronous commits;
- segment time/size retention for user topics;
- disk-pressure and finite history admission;
- bounded diagnostics; and
- rebuildable indexes, snapshots, and in-memory durable tails.

The following are non-goals and are not promised:

- a network service or wire protocol;
- replication, distributed consensus, failover, or high availability;
- multi-process consumer coordination;
- transport authentication, authorization, or encryption;
- transparent segment encryption;
- exactly-once external side effects;
- automatic backup or disaster recovery; or
- portability of benchmark throughput or latency numbers.

The deployment owns filesystem permissions, backup policy, at-rest encryption,
hardware selection, and qualification of flush behavior.

## 4. Model and terminology

### 4.1 Core objects

A **Store** exclusively owns one canonical data directory and contains two
reserved system partitions plus zero or more user topics.

A **Topic** has an immutable nonzero `TopicID`, a unique immutable name, and one
or more numbered partitions. User partition numbers are contiguous starting at
zero.

A **Partition** is an ordered append-only sequence. It has exactly one topic ID
and partition number. Its records occupy the half-open retained range `[L, H)`:

- **L**, the log-start offset, is the first retained offset; and
- **H**, the durable end, is the next offset after the last durable record.

`L <= H`. A fresh partition has `L == H == InitialOffset`. Offsets below `L`
have expired, offsets in `[L,H)` identify durable records, and offset `H` is a
valid empty read position. Offsets above `H` are invalid. Offsets never decrease
or get reused, including after retention.

A **Record** contains its topic, partition, offset, timestamp, nullable key,
nullable value, and ordered headers. Header duplicates and order are
significant.

A **RecordBatch** is one nonempty contiguous sequence from exactly one
partition. Its records begin at `BaseOffset` and increment by one.

A **Segment** is an authoritative `.log` file containing one immutable segment
header followed by complete encoded batches. A partition has one or more
segments ordered by base offset. Only the final segment is active for append.

A **catalog revision** is the next record offset in the cluster-metadata system
log. An **offsets revision** is the next record offset in the consumer-offsets
system log.

### 4.2 Durability terms

An append is **acknowledged** only after all authoritative bytes required for
that append have been written and synchronized as specified in section 8. If a
segment had to be created, its namespace publication is also synchronized
before it can hold acknowledged data.

An operation has an **unknown outcome** when the implementation cannot prove
whether its authoritative mutation became durable. Unknown does not mean
unwritten. Callers MUST reconcile from durable state and MUST NOT assume a
retry is deduplicated.

**Authoritative bytes** are the segment `.log` files, including the two system
logs. They define recovered records and metadata.

Indexes, projection snapshots, and in-memory tails are **rebuildable**. They
MUST NOT override or hide authoritative bytes.

An **anchor** is a nonzero 16-byte segment ID followed by the nonzero SHA-256
hash of that segment's 72-byte encoded header.

### 4.3 Consumer terms

A **group** is identified by a durable group ID and immutable start policy.

An **assignment** maps subscribed topic-partition keys to local member session
IDs. A **generation** is a monotonically increasing `uint64` fencing token. An
**assignment instance** is an additional nonzero 16-byte token binding commits
to the current assignment. The current implementation uses the store ID for
this token; compatibility depends on token equality and fencing semantics, not
on that private choice.

A **baseline** is the first durable next offset for a group/topic/partition. A
**commit** is a later durable next offset. Polling changes only an in-memory
delivery cursor. It does not commit.

**Cleanup debt** consists of artifacts that a durable retention event retired
but that have not yet been physically deleted and directory-synchronized.

## 5. Identifiers, records, and ownership

### 5.1 Identifiers

`TopicID`, `SegmentID`, and `StoreID` are 16 bytes. Their zero values are
invalid identities. `TopicID.String()` and `SegmentID.String()` produce exactly
32 lowercase hexadecimal characters.

The reserved topic IDs are immutable values returned by accessors:

```text
__cluster_metadata = 00000000000000000000000000000001
__consumer_offsets = 00000000000000000000000000000002
```

User topics MUST NOT use either reserved ID. The accessors return values rather
than mutable shared storage.

A fresh store and fresh user topics/segments require nonzero identifiers. An
implementation SHOULD generate unpredictable identifiers and MUST avoid an
identity already present in the same store.

### 5.2 Public record semantics

The Go-level model is equivalent to:

```go
type Header struct {
    Name  string
    Value []byte
}

type AppendRequest struct {
    Topic     TopicID
    Partition uint32
    Key       []byte
    Value     []byte
    Headers   []Header
}

type Record struct {
    Topic     TopicID
    Partition uint32
    Offset    uint64
    Timestamp int64
    Key       []byte
    Value     []byte
    Headers   []Header
}
```

`AppendRequest` has no offset; the partition writer assigns it. On ordinary
`Append`, the writer also assigns the timestamp as Unix milliseconds.

Nil and non-nil empty byte slices are semantically distinct for keys, values,
and header values and MUST survive encode/decode and append/read round trips.
Header names MUST be valid UTF-8 and at most 65,535 encoded bytes. Empty header
names are allowed. A record has at most 1,024 headers.

### 5.3 Copy and aliasing rules

Append input is caller-owned. Before an accepted request can be processed
asynchronously, the implementation MUST deep-copy the key, value, header slice,
header names as needed, and header values. The caller may reuse or mutate input
as soon as `Append` returns or while an admitted append is completing.

Every public read, fetch, catalog view, subscription list, and diagnostic value
that contains slices or byte payloads MUST be caller-owned and MUST NOT expose
mutable authoritative or shared cache storage. Results from separate calls MUST
not alias mutable payload storage.

## 6. Store ownership, layout, and lifecycle

### 6.1 Canonical layout

A conforming store uses this layout relative to its canonical root:

```text
LOCK
system/cluster-metadata/0/
  00000000000000000000.log
  [*.index]
  [*.timeindex]
  [projection.snapshot]
system/consumer-offsets/0/
  00000000000000000000.log
  [*.index]
  [*.timeindex]
  [projection.snapshot]
system/metadata/
  active-manifest
  generations/<20-zero-padded-generation>/
    manifest
    cluster-metadata/0/
      checkpoint
      <20-zero-padded-absolute-offset>.log
      [*.index]
      [*.timeindex]
      [projection.snapshot]
    consumer-offsets/0/
      checkpoint
      <20-zero-padded-absolute-offset>.log
      [*.index]
      [*.timeindex]
      [projection.snapshot]
topics/<32-lowercase-hex-topic-id>/<decimal-partition>/
  <20-zero-padded-decimal-base>.log
  [<20-zero-padded-decimal-base>.index]
  [<20-zero-padded-decimal-base>.timeindex]
  [.topic-preparation-v1]
```

Square brackets denote optional/rebuildable files except the preparation
marker, which is an internal crash-safety artifact and may exist during
reconciliation. Unexpected authoritative files or catalog/storage disagreement
MUST be treated conservatively as described in section 17.

The two legacy system-log paths are authoritative when `active-manifest` is
absent. When it exists, it selects exactly one authoritative metadata
generation and both legacy logs cease to be authority. Generation numbers and
suffix segment bases use exactly 20 zero-padded decimal digits.

Segment filenames are the segment base offset as exactly 20 zero-padded decimal
digits plus `.log`. Sidecars use the same stem. Partition directory names are
canonical base-10 partition numbers.

### 6.2 Exclusive ownership

`Open` MUST resolve a nonempty path to a canonical absolute path, create the
root when needed, acquire process-local ownership, and acquire an exclusive
operating-system lock on the stable `LOCK` file. The `LOCK` file MUST NOT be
removed, renamed, replaced, or truncated during normal operation or close.

A concurrent open of the same directory MUST fail with `ErrDataDirLocked`.
When the platform cannot provide the required lock semantics, opening MUST fail
with `ErrLockUnsupported` rather than silently operating unlocked.

Ownership lasts until close has stopped background work, closed every
partition/cache/root handle, released the OS lock, and released process-local
ownership. If open fails, it MUST unwind any ownership it acquired and return no
usable store.

### 6.3 Bootstrap

Opening validates finite runtime options before mutating authoritative state.
Bootstrap then has three cases:

1. **Fresh store:** create and synchronize both reserved partitions, then append
   `StoreInitialized` as catalog record zero.
2. **Partial initialization with an empty catalog:** complete the fresh-store
   protocol only when existing bytes are consistent with that state.
3. **Existing initialized store:** preflight authoritative system bytes, require
   `StoreInitialized` at catalog offset zero, bind the persisted store ID,
   anchors, and system configurations, replay the catalog, preflight user
   storage, reconcile retired artifacts, replay offsets, enforce operating
   limits, and validate optional snapshots.

Startup MUST preflight authoritative storage before mutating it. Merely opening
a corrupt store MUST NOT silently discard complete authoritative records.

`StoreInitialized` binds the store lineage, initial system-segment anchors, and
immutable system partition configurations. An existing store MUST reuse that
identity and those policies.

### 6.4 Lifecycle

The observable lifecycle is `open -> closing -> closed`.

- Open operations MAY proceed subject to their own concurrency rules.
- Once closing begins, new user work MUST be rejected with `ErrClosing`.
- Work already admitted before the fence MUST be drained or settled where its
  contract requires it, including reserved metadata/offset commands.
- After closure, operations return `ErrClosed` unless their documented close is
  idempotent.

`Store.Close` MUST be safe for concurrent calls and idempotent after successful
completion. It MUST stop snapshot publication and retention before tearing down
partitions, drain and close user partitions, close the two system partitions,
close rebuildable descriptor caches, and release root ownership last. It joins
and returns underlying teardown errors; the current implementation does not add
`ErrCloseIncomplete` to that result. Even when teardown returns an error, the
store remains fenced against new work.

`RootPath` returns the canonical path owned by the store.

## 7. Catalog and topic lifecycle

### 7.1 Topic names and identities

A user topic name MUST:

- contain 1 through 249 bytes;
- use only ASCII letters, ASCII digits, `.`, `_`, and `-`;
- not be `.` or `..`; and
- not begin with `__`.

A topic has 1 through 65,536 partitions. Partition numbers MUST be exactly
`0..count-1`. Topic name, topic ID, partition count, per-partition persisted
configuration, and initial anchors are immutable after successful creation.
Names and IDs are unique within the store.

`DescribeTopic` resolves by name. `ListTopics` returns caller-owned descriptors
in deterministic name order. `OpenTopic` preflights the active-partition limit
for all missing partitions, reuses existing handles, and opens missing
partitions in partition-number order. If a later partition fails to open or
validate after that preflight, the method returns the error and any earlier
partitions opened by that call remain registered and open.

Catalog lookup of an unknown name and managed use of an unknown topic partition
return `ErrUnknownTopic`. Creating an existing name returns `ErrTopicExists`.
The lower-level `OpenPartition` behavior for an uncataloged ID is defined in
Appendix A. Reserved IDs and names are not user topics.

### 7.2 Crash-safe creation

Topic creation is serialized through catalog admission. A successful creation
MUST order authority as follows:

1. choose a nonzero user topic ID;
2. prepare topic/partition directories and marker state;
3. create and synchronize each initial segment and required namespaces;
4. capture each initial segment anchor;
5. append and synchronize one `TopicCreated` event to the catalog;
6. apply the catalog projection; and
7. publish live partition registrations.

The preparation marker, when present, is exactly:

```text
IEL-TOPIC-PREPARATION-V1\n
<32-lowercase-hex-topic-id>\n
```

The catalog event is the durable publication point. Prepared storage not owned
by a catalog event is not a silently adoptable user topic; bootstrap MUST
reconcile only states proven safe by the creation protocol.

`CreateTopicContext` cancellation before sequencer ownership is definitive and
no topic is created. Once authoritative mutation begins, cancellation can no
longer prove absence; the method MAY return the context error joined with
`ErrMetadataOutcomeUnknown`, and durable completion continues. An unknown
metadata result MUST be reconciled through `DescribeTopic` or replay before
retrying.

A catalog write/sync failure that makes durability uncertain MUST return
`ErrMetadataOutcomeUnknown`, fence metadata-dependent mutations with
`ErrMetadataUnavailable`, and MUST NOT roll back or reuse its possible catalog
offset.

### 7.3 Persisted partition policy

`PartitionConfigV1` is immutable catalog policy:

```go
type PartitionConfigV1 struct {
    MaxRecordBytes       uint32
    MaxBatchBytes        uint32
    MaxBatchRecords      uint32
    SegmentMaxBytes      uint64
    BatchLinger          time.Duration
    RetentionMask        uint8
    RetentionMillis      uint64
    RetentionBytes       uint64
    MaxSegmentAgeMillis  uint64
    RetentionCheckMillis uint64
}
```

Ingress memory limits, admission waiters, index stride, and tail settings are
runtime settings and are not persisted in this structure. Retention policy is
persisted only for catalog-owned user topics. System logs MUST have retention
disabled.

## 8. Partition append contract

### 8.1 Ordinary append

`Append(ctx, request)` appends one record and returns the assigned durable
record. A nil context is treated as `context.Background()`.

Before publication to the per-partition ordering point, the implementation
MUST:

1. validate context, topic/partition identity, header count/name, and encoded
   size;
2. reserve estimated disk growth in the correct admission class;
3. acquire bounded record/byte ingress capacity and a bounded waiter slot;
4. recheck cancellation; and
5. deep-copy caller data and assign a Unix-millisecond timestamp.

Exactly one terminal serialization point per partition MUST assign offsets and
perform authoritative writes. The implementation MAY use a ring, channel,
mutex, or another mechanism; the current disruptor ring is not normative.

Selected requests MAY be grouped, but encoded batches MUST obey the partition's
`BatchRecords` and `BatchBytes` limits. Records receive contiguous offsets from
current `H` in serialization order.

### 8.2 Cancellation boundary

Cancellation before publication to the terminal writer is a known-unwritten
outcome. The call returns the context error and MUST NOT consume an offset.

After publication, cancellation cannot prove absence. If cancellation wins
before completion is observed, the call returns the context error joined with
`ErrAppendOutcomeUnknown`; the writer continues and the record may become
durable. If completion is already recorded when cancellation is observed, the
call returns the actual append result, including possible success. A possibly
durable offset MUST NOT be rolled back or reused.

Bounded ingress saturation returns `ErrBackpressure` when no waiter can be
admitted. It is known unwritten. Disk admission, validation, closing, closed,
and unavailable failures before publication are also known unwritten.

### 8.3 Segment rolling and durability

Before a batch that would exceed `SegmentMaxBytes`, the writer rolls if the
active segment already contains records. A single legal batch that cannot fit
an empty configured segment is rejected as `ErrRecordTooLarge`.

A new segment is published by:

1. creating a temporary file with its complete header;
2. synchronizing the file;
3. closing it;
4. renaming it to the canonical segment filename; and
5. synchronizing the parent directory.

Only after successful namespace publication may it become the active segment.
The prior segment then becomes immutable.

An append is acknowledged only after the complete encoded batch has been
written to the active segment and the segment file has been synchronized.
Short writes, write errors, or sync errors after the write attempt produce an
error matching both `ErrAppendOutcomeUnknown` and `ErrPartitionUnavailable`.
The partition MUST be fenced unavailable, no later offset may be allocated by
that instance, and no rollback is attempted.

After successful sync, the implementation advances `H`, updates derived state,
wakes waiters, and may publish a tail-cache copy. Failure to write an index or
populate an in-memory tail MUST NOT invalidate already synchronized
acknowledgement.

### 8.4 Direct batch append

`AppendBatch` accepts one caller-supplied `RecordBatch`. The batch topic and
partition MUST match the target, `BaseOffset` MUST equal current `H`, and record
offsets MUST be contiguous from that base. The configured per-record,
per-batch-byte, and per-batch-record limits apply. It is serialized directly
under the partition's durable write lock and returns the supplied base offset.
It has no context-cancellation boundary.

This method follows the same disk reservation, segment rolling,
synchronization, unknown-outcome, and fencing rules as ordinary append.

### 8.5 Offset exhaustion

V1 offsets are restricted so a nonempty batch's base and exclusive end are at
most `math.MaxInt64`. A writer MUST reject a range outside that limit and MUST
not wrap an offset or generation counter.

## 9. Read contract

### 9.1 Fetch

`Fetch(ctx, offset, options)` returns a caller-owned contiguous prefix beginning
at `offset`. A nil context is background. Raw partition fetch is non-waiting;
it validates `MaxWait` but does not wait for future data.

The default and maximum limits are:

| Field | Zero-value default | Maximum |
|---|---:|---:|
| `MaxRecords` | 1,024 | 65,536 |
| `MaxBytes` | 4 MiB | 64 MiB |
| `MaxWait` | 0 | 24 hours |

Result byte accounting uses encoded v1 record bytes, not just payload bytes.
The method returns complete records only. If the first requested record cannot
fit under `MaxBytes`, it returns `ErrFetchLimitTooSmall`, no records, and
`NextOffset == offset`. It MUST NOT skip that record.

Offsets are interpreted against `[L,H]`:

- `offset < L` or `offset > H` returns `ErrOffsetOutOfRange`;
- `offset == H` returns an empty success with `NextOffset == H`; and
- `L <= offset < H` returns a prefix or a validation/error result.

On every fetch error, `NextOffset` is the requested offset. On success it is one
past the final returned record, or the requested offset for an empty result.

A bounded in-memory durable tail MAY satisfy a fetch. A miss MUST fall back to
authoritative segments. The tail contains only records published after segment
sync and MUST preserve the same copy, range, and limit behavior.

### 9.2 Read

`Read(offset, maxRecords)` is the simpler non-waiting record-count API. Zero
`maxRecords` selects 65,536. It uses the same `[L,H]` range rules and returns
caller-owned records. `offset == H` returns an empty success.

### 9.3 Reader

A `Reader` is an in-memory cursor over raw fetch:

- `NewReader(start)` validates `start` in `[L,H]`;
- `Fetch` starts at the cursor and advances it only after success;
- `Seek` validates and changes the cursor only after success;
- `NextOffset` exposes the current in-memory cursor; and
- `Close` is idempotent.

A failed fetch or seek leaves the cursor unchanged. Overlapping operations on
one reader return `ErrConcurrentOperation`. A reader has no group identity,
lease, or durable commit.

### 9.4 Corruption while reading

A `Fetch` validation or I/O error indicating possible authoritative corruption
fences the partition unavailable; later operations observe
`ErrPartitionUnavailable`. Expected context cancellation, range/limit errors,
lifecycle errors, and concurrent operation errors do not trigger that fence.
The simpler `Read` method returns segment I/O and decode errors directly and
does not itself mark the partition unavailable.

## 10. Managed consumers

### 10.1 Delivery guarantee and durable state

Managed consumers provide at-least-once delivery. `Poll` advances only an
in-memory next-delivery cursor after the fetched result passes the current
assignment fence. Only explicit synchronous `Commit` advances durable progress.
Close never auto-commits. Reopen, replacement, or crash may redeliver every
record after the last durable commit or initial baseline.

Group IDs are valid UTF-8, contain no NUL, and occupy 1 through 255 bytes. Start
policy is one of:

| Value | Name | Initial next offset |
|---:|---|---|
| 1 | earliest | current `L` |
| 2 | latest | current `H` |
| 3 | explicit | supplied value in `[L,H]` |

A zero Go option selects earliest. The policy is persisted on first group
creation and is immutable. Later opens MUST provide the same policy. Explicit
starts are immutable and MUST exactly cover the relevant initial subscription
keys.

Progress timeout defaults to 30 seconds and MUST be in `[1ms,24h]`. `Poll`
`MaxWait` MUST be strictly less than the progress timeout. An admitted poll or
commit renews/protects its member lease while waiting for internal admission or
durable work. An idle expired assignment is fenced.

Managed waits use bounded partition waiters; no partition admits more than 64
fetch waiters.

### 10.2 Single-partition consumer

`OpenConsumer` owns one `(group, topic, partition)` assignment. On first use it
persists `GroupCreated`, resolves the baseline, and then persists
`LocalAssignmentChanged` before returning a handle. A later open for the same
key durably advances the group generation and fences the prior handle.

`Poll` returns a bounded durable prefix for the assignment. Once replaced,
expired, closed, or otherwise stale, operations return `ErrAssignmentLost`.
Overlapping operations on one handle return `ErrConcurrentOperation`.

`Commit(next)` MUST satisfy all of these:

- the assignment is current;
- `next >=` the durable committed-or-initial position;
- `next <=` the furthest position delivered by this handle; and
- `next <= H`.

A smaller value returns `ErrCommitRegression`. A value beyond delivered or
beyond `H` returns `ErrInvalidCommit`. Recommitting the same already committed
position is a successful no-op.

A commit is acknowledged only after its offsets-system event is written and
synchronized. An uncertain write returns `ErrCommitOutcomeUnknown`, and the
offsets subsystem is fenced with `ErrGroupUnavailable`. The caller MUST reopen
and inspect durable progress before retrying.

`NextOffset` is the in-memory next-delivery cursor, not a durable commit.
`Close` durably installs an empty next generation when the assignment is still
current, then withdraws the handle. It is idempotent and does not commit
records.

### 10.3 Group consumer

`OpenConsumerGroup` receives the complete same-process membership snapshot for
one group. A snapshot has 1 through 4,096 members and at most 65,536 total
subscription keys. An individual member may have zero subscriptions. Each
nonempty subscription list is unique and canonicalized by raw topic ID then
partition. A topic-partition key MUST belong to only one member.

The public member type does not expose a session ID; a fresh nonzero session ID
is generated for every newly installed member. Canonical member ordering is by
the canonical subscription lists for request equivalence; persisted members
are sorted by generated session ID. Assignments are sorted by topic key.

An equivalent live request, including start policy, explicit starts, fetch
options, timeout, and membership, coalesces to the current live snapshot and
MUST NOT advance its generation. A changed request atomically installs a fresh
complete snapshot, advances the durable generation, and fences the entire prior
snapshot. Expiry of any idle member fences the entire local snapshot.

Every subscribed key is assigned to exactly one declared member. `Poll` and
`Commit` select a subscribed topic/partition and otherwise follow the
single-consumer rules. Independent keys have independent operation fences.
`Subscriptions` returns all currently assigned keys in canonical topic-key
order and caller-owned storage.

`Close` durably revokes the complete snapshot without committing delivered
records. Replacement after close must be explicit.

### 10.4 Durable projection invariants

Offsets events replay contiguously. Each event binds to a nonzero catalog
revision no newer than the catalog projection and may reference only a
partition created at or before that revision.

An assignment's `newGeneration` MUST equal `expectedGeneration + 1`, and the
expected value MUST match projected state. A commit MUST match generation,
assignment instance, owner session, topic key, and the exact previous baseline
or commit. These compare-and-append fields make stale or reordered history
corrupt rather than silently acceptable.

## 11. Retention

Retention applies only to catalog-owned user partitions and is persisted in
`PartitionConfigV1`. The mask uses bit 0 for time and bit 1 for size. Other bits
are unsupported. An enabled zero duration or byte threshold is meaningful: all
otherwise eligible closed segments satisfy that threshold.

A nonzero mask requires a positive check interval. Time retention also requires
a positive maximum segment age. Disabled time and size policies encode their
retention duration and byte threshold as zero, respectively; fully disabled
retention encodes a zero check interval. `MaxSegmentAgeMillis` may remain
nonzero when time retention is disabled, but it has no rolling effect unless
the time-retention bit is enabled. System-log retention is forbidden.

`RunRetention(ctx)` serializes passes and evaluates every configured user
partition once. Nil context is background. A background worker runs only when
at least one persisted policy requires it and stops before partition teardown.

Each pass MUST:

1. reconcile earlier cleanup debt first;
2. respect the finite active-partition limit, without evicting a public handle;
3. roll a nonempty active segment when maintenance requires an immutable
   candidate;
4. consider only an oldest contiguous prefix of non-active segments;
5. retain at least one segment as the successor anchor;
6. append and synchronize `PartitionLogStartAdvanced` in the catalog; and
7. only after that durable logical boundary, delete exactly the inventoried
   `.log`, `.index`, and `.timeindex` artifacts and synchronize the directory.

The logical transition advances `L` before physical deletion. If deletion or
directory sync fails, `L` remains advanced and the artifacts remain cleanup
debt. They MUST NOT become readable authority again. Offsets below new `L`
return `ErrOffsetOutOfRange` and are never reused.

Time eligibility uses a sealed segment's maximum record timestamp and excludes
future or unknown timestamps. Size retention triggers while retained bytes are
strictly greater than the configured byte limit. When both reasons apply, the
event records the combined reason mask.

An unknown catalog outcome MUST preserve candidate artifacts until replay can
prove which boundary is authoritative.

## 12. Capacity and disk admission

### 12.1 Finite operating limits

Every store has finite limits for catalog topics, catalog user partitions,
active partitions, sealed-segment descriptors, system-log history, consumer
groups, consumer progress keys, and tail bytes. Zero option values select the
defaults in Appendix B; zero does not mean unlimited.

An existing catalog or offsets history above a newly lowered operating limit
remains readable only where documented, but growth requiring exhausted capacity
MUST fail with `ErrResourceLimit` or `ErrSystemLogCapacity`. Operating limits do
not delete persisted state.

Each partition also has finite record, batch, segment, in-flight record,
in-flight byte, waiter, and optional tail limits. Invalid values return
`ErrInvalidArgument`; values above a supported implementation ceiling return
`ErrResourceLimit`.

### 12.2 System-history budgets

The catalog and offsets logs each have an independent logical history budget,
defaulting to 1 GiB. User retention never reclaims system history. A new system
event that would exceed its budget returns `ErrSystemLogCapacity`. Lowering a
budget below existing complete history does not itself make recovery invalid,
but no new event can be admitted until a sufficiently larger budget is used.

For a checkpointed generation, the budget applies to the active suffix log,
not to the checkpoint containing the retired prefix projection. A successful
`CompactSystemLogs` starts each suffix at its checkpoint's covered absolute
offset and thereby restores logical append headroom. The checkpoint itself is
bounded by the 256 MiB checkpoint-file limit and 1,048,576-entry limit.

Compaction requires temporary byte and inode capacity for both checkpoints,
two fresh suffix segments, and two manifests while the prior authority still
exists. It uses protected control admission and fails before publication when
that temporary capacity cannot be reserved.

### 12.3 Disk-pressure ledger

All appends reserve estimated bytes and inodes against one store-wide ledger.
The canonical estimate for a batch is:

```text
bytes  = encoded batch bytes + SegmentHeaderBytes
inodes = 1
```

The estimate conservatively allows a segment roll and excludes derived index
and snapshot bytes. Reservations are released after the attempt; the
filesystem observation remains advisory because external writers and quotas
can race it.

The ledger has byte and, where measurable, inode floors satisfying:

```text
safety < user-stop < resume
```

User growth stops before crossing the user-stop floor. Once pressured, it
remains stopped until observations reach the resume floor. Reserved control
writes for the catalog and offsets logs may continue below user-stop but MUST
stop before crossing the safety floor. This protection lets fencing, commits,
and retention-boundary events survive ordinary user-stop pressure.

A filesystem-capacity probe failure fails closed with `ErrDiskPressure`.
Reservation arithmetic overflow also fails without writing.

## 13. Errors

All domain errors are stable sentinel values. Public operations MAY wrap or join
them with contextual errors. Callers MUST use `errors.Is` and MUST NOT depend on
error strings or concrete wrapper types.

| Sentinel | Contract |
|---|---|
| `ErrUnknownTopic` | catalog name/ID or partition is unknown |
| `ErrOffsetOutOfRange` | requested offset is outside `[L,H]` |
| `ErrPartitionUnavailable` | partition is fenced after a serious write/read failure |
| `ErrAppendOutcomeUnknown` | append may or may not be durable |
| `ErrDataDirLocked` | another owner holds the data directory |
| `ErrLockUnsupported` | required ownership lock is unavailable |
| `ErrTopicExists` | topic name already exists |
| `ErrMetadataUnavailable` | catalog mutations are fenced |
| `ErrMetadataOutcomeUnknown` | catalog mutation may or may not be durable |
| `ErrAssignmentLost` | consumer generation/session/lease is stale |
| `ErrGroupUnavailable` | offsets/group subsystem is fenced |
| `ErrCommitOutcomeUnknown` | commit may or may not be durable |
| `ErrCommitRegression` | proposed next offset is below durable progress |
| `ErrInvalidCommit` | proposed next offset was not validly delivered or exceeds `H` |
| `ErrBackpressure` | bounded append admission could not accept another waiter |
| `ErrRecordTooLarge` | record or batch cannot fit a format/configured limit |
| `ErrFetchLimitTooSmall` | first requested complete record cannot fit result bytes |
| `ErrResourceLimit` | finite runtime/format capacity is exhausted or unsupported |
| `ErrSystemLogCapacity` | catalog or offsets history cannot grow under its budget |
| `ErrDiskPressure` | class-specific disk admission failed |
| `ErrClosing` | lifecycle fence rejects new work |
| `ErrClosed` | target is closed |
| `ErrCloseIncomplete` | exported reserved sentinel; current `Store.Close` does not add it to teardown errors |
| `ErrConcurrentOperation` | one-handle operation fence or nonblocking diagnostic lock failed |
| `ErrCorruptLog` | authoritative or claimed cache bytes violate a supported format/invariant |
| `ErrUnsupportedFormat` | recognized structure uses an unsupported version/feature value |
| `ErrInvalidArgument` | caller input is invalid |

Unknown-outcome errors can be joined with an unavailability sentinel and a
context or I/O error. Matching one sentinel does not imply no other sentinel is
present.

## 14. Diagnostics

Diagnostics are bounded observations and are not authoritative state. Counters
MUST saturate instead of wrapping.

`Store.Stats` captures fields independently. It is not a transactionally
consistent cross-partition snapshot and callers MUST NOT infer one. It reports
lifecycle phase, close state, active/open objects, tail use, system-history
headroom, consumer projection use, reserved partition stats, retention status,
disk pressure, and bounded cleanup debt. Stats after close return the partial
lifecycle view with `ErrClosed`; during close they return a view with
`ErrClosing`.

`Partition.Stats` reports identity, `L`, `H`, logical bytes/segments, bounded
ingress and tail state, lifecycle/fencing state, append outcome classes,
rejection classes, and write/sync/namespace latency. It retains no payload.

`Consumer.Stats` and `GroupConsumer.Stats` report one assignment key at a time:
log bounds, next delivery, committed-or-initial position, delivery lag, commit
lag, and expired committed distance. Lag is an offset distance, not elapsed
time or proof of external processing. Inactive assignments report no lag
values. Contended nonblocking diagnostics return `ErrConcurrentOperation`.

Latency totals use eight fixed buckets:

```text
<1µs, <10µs, <100µs, <1ms, <10ms, <100ms, <1s, >=1s
```

Cleanup inspection is capped at 1,024 retired segments. `ScanTruncated` and
`ScanError` disclose incomplete observation. Cleanup bytes do not become usable
capacity until physical deletion and directory synchronization complete.

`SnapshotDiagnostics` may contain catalog or offsets errors while the store is
usable. Snapshot failures concern optional accelerators; successful
system-log replay remains authoritative.

## 15. Authoritative persistence format

### 15.1 Common rules

V1 uses little-endian integers and CRC32C with the Castagnoli polynomial.
`CRC32C(data)` is the numeric checksum; checksum fields store it as a
little-endian `uint32`.

Global v1 limits are:

| Item | Limit |
|---|---:|
| batch bytes | 64 MiB |
| record bytes | 16 MiB |
| records per batch | 65,536 |
| headers per record | 1,024 |
| header-name bytes | 65,535 |
| partition number | `math.MaxInt32` |
| base/exclusive-end offset | `math.MaxInt64` |

`0xffffffff` in a nullable byte-length field means nil. Length zero means a
non-nil empty value.

### 15.2 Segment header

Every `.log` begins with this 72-byte header:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `IELSEG00` |
| 8 | 2 | segment format version `1` |
| 10 | 2 | header bytes `72` |
| 12 | 4 | reserved zero, validated |
| 16 | 16 | nonzero topic ID |
| 32 | 4 | partition, at most `MaxInt32` |
| 36 | 2 | batch format version `1` |
| 38 | 2 | reserved zero, validated |
| 40 | 8 | base offset, at most `MaxInt64` |
| 48 | 16 | nonzero segment ID |
| 64 | 4 | reserved zero, validated |
| 68 | 4 | CRC32C of bytes `[0,68)` |

The header does not store record count, end offset, or active state. Those are
derived by scanning batches. The topic and partition MUST match the directory
being opened. The first segment's base is `L`; adjacent segment and batch ranges
MUST be contiguous.

### 15.3 Batch

A batch is a 48-byte header, concatenated complete records, and a 4-byte
trailer:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `IELB` |
| 4 | 2 | batch format version `1` |
| 6 | 2 | header bytes `48` |
| 8 | 4 | total batch bytes including trailer |
| 12 | 4 | record count, `1..65,536` |
| 16 | 8 | base offset |
| 24 | 8 | maximum record timestamp as `int64` bits |
| 32 | 4 | required attributes, zero and validated |
| 36 | 2 | record encoding version `1` |
| 38 | 2 | reserved zero, validated |
| 40 | 4 | reserved zero, validated |
| 44 | 4 | CRC32C of bytes `[0,44)` |
| 48 | variable | encoded records |
| `total-4` | 4 | CRC32C of bytes `[0,total-4)` |

The declared total is at least `48 + 28 + 4` and at most 64 MiB. It MUST equal
the complete supplied batch size. Record offsets MUST be contiguous from base,
and the exclusive end MUST not exceed `MaxInt64`. The stored maximum timestamp
MUST equal the maximum decoded record timestamp.

A complete batch with invalid magic, lengths, reserved fields, header checksum,
trailer checksum, count, timestamp, or record continuity is corrupt and MUST
NOT be silently truncated. A recognized batch or record encoding version other
than v1 is `ErrUnsupportedFormat`, not corruption, and likewise MUST NOT be
truncated.

### 15.4 Record

A record begins with a 28-byte fixed prefix followed by nullable key/value
fields and headers:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | total record bytes |
| 4 | 4 | offset delta from batch base |
| 8 | 8 | Unix-millisecond timestamp as `int64` bits |
| 16 | 8 | canonical zero/reserved bytes |
| 24 | 4 | header count |
| 28 | 4 + N | key length and bytes; `0xffffffff` means nil |
| variable | 4 + N | value length and bytes; `0xffffffff` means nil |
| variable | variable | ordered header entries |

Each header entry is:

```text
nameBytes:uint32
valueBytes:uint32       // 0xffffffff means nil
name:nameBytes          // valid UTF-8
value:valueBytes        // absent when nil
```

`recordBytes` includes all record framing and data. It is in
`[RecordPrefixBytes, MaxRecordBytes]` and the record MUST consume exactly that
many bytes. Offset deltas in a batch are exactly `0..count-1`. Header order and
duplicates are preserved. A conforming v1 writer emits zero at record bytes
`[16,24)`; those bytes have no defined application semantics.

### 15.5 System-event envelope

System events are stored as the **record value** of one record in the applicable
reserved partition. The event envelope is 16 bytes:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `IELE` |
| 4 | 2 | event type |
| 6 | 2 | schema version `1` |
| 8 | 4 | payload bytes |
| 12 | 4 | flags/reserved zero, validated |
| 16 | variable | payload |

The declared payload length MUST consume the remainder exactly. Catalog accepts
event types 1, 2, and 6; offsets accepts 3, 4, and 5. Other placements are
corrupt. Event values are:

| Value | Event |
|---:|---|
| 1 | `TopicCreated` |
| 2 | `PartitionLogStartAdvanced` |
| 3 | `GroupCreated` |
| 4 | `LocalAssignmentChanged` |
| 5 | `OffsetCommitted` |
| 6 | `StoreInitialized` |

System event records have nil keys and no headers. Payload strings are a
`uint32` byte length followed by valid UTF-8 without NUL. All payload decoders
require complete consumption; trailing bytes are corrupt.

### 15.6 `PartitionConfigV1` encoding

The immutable configuration encoding is exactly 64 bytes:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 2 | config version `1` |
| 2 | 2 | encoded size `64` |
| 4 | 4 | maximum record bytes |
| 8 | 4 | maximum batch bytes |
| 12 | 4 | maximum records per batch |
| 16 | 8 | maximum segment bytes |
| 24 | 4 | batch linger in whole milliseconds |
| 28 | 1 | retention mask: bit 0 time, bit 1 size |
| 29 | 1 | reserved zero, validated |
| 30 | 2 | reserved zero, validated |
| 32 | 8 | retention duration in milliseconds |
| 40 | 8 | retention bytes |
| 48 | 8 | maximum segment age in milliseconds |
| 56 | 8 | retention check interval in milliseconds |

The configured record maximum is `28..16MiB`. Batch maximum can hold at least
one minimum record plus framing, is at most 64 MiB, and can hold the configured
record maximum plus batch framing. Batch count is `1..65,536`. Segment maximum
can hold its header plus one maximum configured batch and is at most
`MaxInt64`. Durations fit Go `time.Duration`; batch linger is a whole
millisecond fitting `uint32`. Retention bytes are at most `MaxInt64`.

### 15.7 Catalog event payloads

An anchor in these payloads is `segmentID[16] || SHA-256(segmentHeader)[32]` and
both components are nonzero.

#### 15.7.1 `StoreInitialized`

The payload is exactly 240 bytes and MUST be catalog record offset zero:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 16 | nonzero store ID |
| 16 | 48 | catalog initial anchor |
| 64 | 48 | offsets initial anchor |
| 112 | 64 | catalog system `PartitionConfigV1` |
| 176 | 64 | offsets system `PartitionConfigV1` |

Both configurations MUST have retention disabled. Replay requires this event to
be first and unique.

#### 15.7.2 `TopicCreated`

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 16 | store ID |
| 16 | 8 | expected next catalog offset |
| 24 | 16 | nonzero, non-reserved topic ID |
| 40 | 4 + N | topic-name length and bytes |
| `44+N` | 4 | partition count |
| variable | `116 * count` | partition entries |

A partition entry is:

```text
partition:uint32
config:PartitionConfigV1[64]
initialAnchor:anchor[48]
```

Count is `1..65,536`. Entry number `i` MUST carry partition `i`. The payload
MUST fit in the enclosing record. Replay requires matching store ID and expected
catalog offset, a valid topic name, and unique name/ID.

#### 15.7.3 `PartitionLogStartAdvanced`

The portion before retired entries is exactly 140 bytes:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 16 | store ID |
| 16 | 8 | expected next catalog offset |
| 24 | 16 | topic ID |
| 40 | 4 | partition |
| 44 | 8 | expected old `L` |
| 52 | 8 | new `L` |
| 60 | 8 | evaluated durable `H` |
| 68 | 48 | retained successor anchor |
| 116 | 1 | reason mask, bits 0..1, nonzero |
| 117 | 3 | reserved zero, validated |
| 120 | 8 | evaluation Unix milliseconds as `int64` bits |
| 128 | 8 | evaluated retained bytes |
| 136 | 4 | retired entry count |
| 140 | `72 * count` | retired entries |

Each retired entry is:

```text
base:uint64
end:uint64
bytes:uint64
anchor:anchor[48]
```

Count is `1..65,536`; bases are strictly increasing; every `base < end`; bytes
are positive during replay. Replay additionally requires:

- the store, topic, and partition exist and the expected catalog offset matches;
- expected old `L` equals projected `L`;
- `oldL < newL <= evaluatedH`;
- reason bits are enabled by persisted policy;
- evaluated `H` does not regress below the recorded retirement high-water mark;
- entries begin at old `L`, cover contiguously and exactly through new `L`;
- segment IDs are unique and do not retire the successor anchor; and
- the retained anchor matches the authoritative successor segment.

### 15.8 Consumer-offset event payloads

All three payloads begin with:

```text
storeID:[16]
catalogNext:uint64
```

The store ID must match. `catalogNext` MUST be nonzero and no greater than the
replayed catalog revision. A topic key is `topicID[16] || partition:uint32`, with
a nonzero ID and partition at most `MaxInt32`. Topic keys sort by raw topic-ID
bytes, then partition.

#### 15.8.1 `GroupCreated`

After the common prefix:

```text
groupID:string
startMode:uint8
reserved:[3]zero
explicitStartCount:uint32
repeat explicitStartCount:
    topicKey:[20]
    initialNext:uint64
```

Earliest and latest require count zero. Explicit requires count
`1..65,536`. Entries are strictly increasing by topic key and reference
catalog-visible partitions. Replay rejects duplicate group IDs.

#### 15.8.2 `LocalAssignmentChanged`

After the common prefix:

```text
groupID:string
assignmentInstanceID:[16]nonzero
expectedGeneration:uint64
newGeneration:uint64
reason:uint8
reserved:[3]zero
memberCount:uint32
repeat memberCount:
    sessionID:[16]nonzero
    subscriptionCount:uint32
    repeat subscriptionCount: topicKey:[20]
assignmentCount:uint32
repeat assignmentCount:
    topicKey:[20]
    ownerSessionID:[16]nonzero
    resumeNext:uint64
    baselineAction:uint8
    reserved:[3]zero
    initialNext:uint64
```

`memberCount <= 4,096`; aggregate subscriptions are at most 65,536. Members
are strictly sorted by raw session ID. Each member's keys and all assignments
are strictly sorted. Subscription keys have one owner. Every assignment owner
is a declared member, and assignments cover every subscription exactly once.

`newGeneration == expectedGeneration + 1`; generation overflow is invalid. The
reason value is in `1..6`. Current writers use 1 initial, 2 close/revoke, 3 live
replacement, 4 timeout replacement, and 5 reopen; value 6 is reserved as a
recognized v1 reason.

Baseline action 0 reuses existing progress: `initialNext` is zero and
`resumeNext` equals latest projected progress. Action 1 creates progress: no
prior progress exists and `initialNext == resumeNext`. Other actions are
unsupported.

#### 15.8.3 `OffsetCommitted`

After the common prefix:

```text
groupID:string
commitInstanceID:[16]nonzero
memberSessionID:[16]nonzero
generation:uint64
topicKey:[20]
previousStateKind:uint8
reserved:[3]zero
expectedPrevious:uint64
next:uint64
```

Previous-state kind 0 means the initial baseline and requires no prior commit.
Kind 1 means the prior durable commit and requires one. `expectedPrevious` MUST
match the selected projected position, and `next >= expectedPrevious`. The
instance, generation, member owner, and topic key MUST match the current
assignment.

### 15.9 Authoritative system-log generations

`CompactSystemLogs` checkpoints the catalog and offsets projections at their
current next offsets and creates empty suffix partitions whose initial offsets
are those same absolute values. Revisions, group generations, committed
positions, and record offsets MUST NOT be renumbered or reused.

The authority switch is one `system/metadata/active-manifest` file. The active
manifest and the selected generation's `manifest` MUST be byte-identical. A
committed manifest selects both logs atomically; recovery MUST NOT combine a
catalog checkpoint or suffix from one generation with offsets state from
another. Missing or corrupt selected generation data is authoritative
corruption and MUST NOT fall back to the legacy layout or an older generation.

#### 15.9.1 Checkpoint file

A checkpoint is a 104-byte header followed by its payload:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `IELCKP00` |
| 8 | 2 | checkpoint schema version `1` |
| 10 | 2 | header bytes `104` |
| 12 | 2 | kind: 1 catalog, 2 offsets |
| 14 | 2 | reserved zero, validated |
| 16 | 8 | nonzero metadata generation |
| 24 | 16 | store ID |
| 40 | 8 | covered absolute next offset |
| 48 | 8 | payload bytes |
| 56 | 8 | bounded entry count |
| 64 | 32 | SHA-256 payload digest |
| 96 | 4 | reserved zero, validated |
| 100 | 4 | CRC32C of bytes `[0,100)` |

The payload uses the existing snapshot-payload ceiling of 268,435,296 bytes,
and entry count is at most 1,048,576. The catalog payload contains the complete
current topic and partition projection plus every retired artifact still owed
physical cleanup.
The offsets payload contains every group creation policy, current assignment
body and fencing tokens, explicit starts, baselines, and committed positions.
The checkpoint header's covered offset is the recovered projection revision.

#### 15.9.2 Manifest

Each manifest is exactly 256 bytes:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `IELMNF00` |
| 8 | 2 | manifest schema version `1` |
| 10 | 2 | bytes `256` |
| 12 | 4 | reserved zero, validated |
| 16 | 8 | nonzero generation |
| 24 | 16 | store ID |
| 40 | 8 | catalog covered next offset |
| 48 | 8 | offsets covered next offset |
| 56 | 32 | SHA-256 complete catalog checkpoint |
| 88 | 32 | SHA-256 complete offsets checkpoint |
| 120 | 48 | catalog suffix initial anchor |
| 168 | 48 | offsets suffix initial anchor |
| 216 | 36 | reserved zero, validated |
| 252 | 4 | CRC32C of bytes `[0,252)` |

Both checkpoint digests, both nonzero suffix anchors, generation, store ID,
and coverage offsets MUST match the selected files. Catalog coverage is
nonzero because every store contains `StoreInitialized`; offsets coverage may
be zero.

#### 15.9.3 Publication and cleanup

Compaction serializes with retention, snapshot publication, shutdown, and all
catalog/offset mutations. It MUST fully write and synchronize both
checkpoints, fresh suffix headers, generation directories, and the generation
manifest before atomically publishing and parent-directory-synchronizing the
active manifest. Before that publication point the old authority remains
selected. After it, the new generation is selected.

If active-manifest publication has an unknown outcome, both metadata mutation
classes MUST be fenced until reopen resolves the durable authority. Obsolete
authority may be removed only after successful publication and live handoff.
Cleanup failure does not roll authority back and is exposed as bounded
maintenance cleanup debt.

## 16. Rebuildable persistence formats

Nothing in this section can create or remove authoritative records. A missing,
lagging, malformed, or unsupported index/snapshot MUST be ignored or rebuilt
after authoritative validation. It MUST NOT hide a segment record, advance
metadata, or make an append acknowledged.

### 16.1 Sparse index files

Both `.index` and `.timeindex` begin with a 128-byte header:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `IELIDX00` |
| 8 | 2 | format version `1` |
| 10 | 2 | header bytes `128` |
| 12 | 2 | kind: 1 offset, 2 time |
| 14 | 2 | entry bytes: 32 or 40 |
| 16 | 8 | reserved zero, validated |
| 24 | 16 | store ID |
| 40 | 16 | topic ID |
| 56 | 4 | partition |
| 60 | 4 | reserved zero, validated |
| 64 | 16 | segment ID |
| 80 | 8 | segment base |
| 88 | 32 | SHA-256 segment-header hash |
| 120 | 4 | nonzero sampling stride |
| 124 | 4 | CRC32C of bytes `[0,124)` |

An offset entry is 32 bytes: base `uint64`, file position `uint64`, batch bytes
`uint32`, record count `uint32`, reserved zero `uint32`, then CRC32C over the
first 28 bytes.

A time entry is 40 bytes: prefix-maximum timestamp as `int64` bits, base
`uint64`, file position `uint64`, batch bytes `uint32`, record count `uint32`,
reserved zero `uint32`, then CRC32C over the first 36 bytes.

An index file is at most 64 MiB. Entries MUST refer to valid complete batches in
the bound segment and be monotonic according to index kind. Index hints may
only choose an earlier safe scan point. The current fetch path uses the offset
index; the time index remains a validated accelerator for future/time-oriented
use.

### 16.2 Projection snapshot file

A snapshot is optional and appears only for a reserved system partition. It is:

```text
snapshotHeader[128] || payload[payloadBytes] || sha256(header || payload)[32]
```

Header layout:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 8 | ASCII `IELSNP00` |
| 8 | 2 | format version `1` |
| 10 | 2 | header bytes `128` |
| 12 | 2 | kind: 1 catalog, 2 offsets |
| 14 | 2 | snapshot schema version `1` |
| 16 | 8 | reserved zero, validated |
| 24 | 16 | store ID |
| 40 | 16 | reserved system topic ID |
| 56 | 4 | partition, currently zero |
| 60 | 4 | reserved zero, validated |
| 64 | 8 | covered next offset |
| 72 | 8 | payload bytes |
| 80 | 32 | covered-prefix digest |
| 112 | 8 | logical entry count |
| 120 | 4 | reserved zero, validated |
| 124 | 4 | CRC32C of bytes `[0,124)` |

The file is at most 256 MiB, so payload is at most `256MiB - 128 - 32`.
Entry count is at most 1,048,576.

The prefix digest is SHA-256 over:

```text
"IEL-PROJECTION-PREFIX-V1\x00"
storeID[16]
topicID[16]
partition:uint32
all complete encoded batch bytes below coveredNextOffset
coveredNextOffset:uint64
```

Validation checks identity, versions, lengths, count, prefix digest, whole-file
SHA-256, and exact equality to a freshly encoded projection. Publication uses a
temporary file, file sync, rename, and directory sync. Failure preserves the
previous valid snapshot when possible and is reported through diagnostics.

#### 16.2.1 Catalog snapshot payload

```text
StoreInitialized payload[240]
topicCount:uint32
repeat topics sorted by raw topic ID:
    creationOffset:uint64
    topicID:[16]
    name:string
    partitionCount:uint32
    repeat partitions in partition-number order:
        partition:uint32
        config:[64]
        initialAnchor:[48]
        retainedL:uint64
        currentRetainedAnchor:[48]
        boundaryCatalogNext:uint64
        maximumRecordedRetirementH:uint64
```

Entry count is `1 + topicCount + totalPartitionCount`.

#### 16.2.2 Offsets snapshot payload

```text
groupCount:uint32
repeat groups sorted by group ID string:
    creationOffset:uint64
    creationBodyBytes:uint32
    creationBody:[creationBodyBytes]
    generation:uint64
    assignmentOffset:uint64
    assignmentBodyBytes:uint32
    assignmentBody:[assignmentBodyBytes]   // zero length means absent
    progressCount:uint32
    repeat progress sorted by topic key:
        topicKey:[20]
        initialNext:uint64
        hasCommit:uint8
        reserved:[3]zero
        committedNext:uint64
```

Creation and assignment bodies are exact system-event payload bodies, without
the 16-byte `IELE` envelope. A creation body is required; assignment may be
absent. Each body is at most `MaxRecordBytes - 44` and MUST independently pass
its event-body validation.

Per group, entry count adds one group, its explicit-start entries, assignment
members plus subscriptions plus assignments, and progress entries.

## 17. Recovery and corruption

### 17.1 Authority and preflight

Recovery derives state from complete authoritative segment headers and batches,
not from indexes, snapshots, filenames alone, or in-memory state. Before
mutation, startup preflights the system logs and then every catalog-owned user
partition against IDs, paths, initial/current retained anchors, continuity, and
persisted policy.

When an active metadata manifest exists, recovery first validates its selected
generation, both checkpoints, checkpoint digests, suffix anchors, absolute
offset continuity, and catalog/offset cross-references. It loads each complete
checkpoint projection and replays only records from the covered absolute offset
through the suffix durable end. An unpublished generation is never authority.

Catalog and offsets replay require record offsets to be contiguous. Catalog
must begin with exactly one `StoreInitialized`. Event store IDs, expected
revisions, anchors, immutable identities, generation transitions, assignment
ownership, and compare-and-append fields MUST validate.

Unexpected user storage, missing catalog-owned storage, malformed canonical
names, anchor mismatch, gaps, overlaps, or complete invalid batches cause open
to fail with `ErrCorruptLog` or `ErrUnsupportedFormat`. Recovery MUST preserve
the evidence for inspection.

### 17.2 Permitted final-tail truncation

Only the body of a verified incomplete final batch in the active final segment
may be truncated. A partial batch header is corruption and is not truncatable.
Recovery MUST establish all of these before truncation:

- every prior segment and batch is complete and valid;
- the candidate begins with a complete 48-byte v1 batch header after the final
  segment's valid header and valid batch prefix;
- its magic, versions, header size, header checksum, reserved fields, count,
  base offset, and declared total are valid;
- the valid declared total is greater than the remaining file bytes;
- truncation restores the exact end of the last complete batch; and
- the truncated file is synchronized before recovery continues.

An incomplete segment header, corruption in a non-final location, bad checksum
on a complete batch, invalid complete lengths/counts/records, or unsupported
version MUST NOT be repaired by truncation. A truncation or synchronization
failure refuses recovery and is retried only on a later open.

### 17.3 Cache degradation

A missing or invalid index is rebuilt from its segment. A missing or invalid
snapshot degrades to full system-log replay and records diagnostics. A live-tail
miss scans segments. No accelerator validation failure can authorize mutation
of otherwise valid authoritative bytes.

## 18. Concurrency and shutdown

Public stores and partitions support concurrent use where method contracts do
not impose a one-handle operation fence. Per-partition append order is defined
by one terminal serialization point, not producer goroutine scheduling.

Readers, single consumers, and each group cursor reject overlapping operations
with `ErrConcurrentOperation`; they do not queue unbounded work. Store metadata
and offsets mutations each use bounded serialized admission.

A partition close fences new work and drains already admitted appends before
closing its active descriptor and derived state. It is idempotent. Store close
begins with a global user-work fence but permits already reserved system
commands to settle before system-partition teardown. Retention and snapshot
publication cannot outlive directory ownership.

The implementation MUST bound:

- append records and bytes in flight;
- append admission waiters;
- fetch waiters;
- open user partitions;
- sealed-segment descriptors;
- tail bytes and entries;
- consumer members, keys, groups, and progress entries;
- diagnostic scans and strings; and
- catalog/offsets history growth.

Private synchronization choices, goroutine topology, ring implementation,
cache replacement policy, and exact helper/package organization are not
compatibility requirements.

## 19. Security and operational assumptions

The stable `LOCK` file is part of correctness, not an advisory convenience.
Operators MUST prevent another process or user from modifying the store while
it is open. The entire data directory, including system logs, user segments,
sidecars, snapshots, markers, and lock file, SHOULD have restrictive ownership
and permissions.

`immulog` does not authenticate callers, authorize topics, encrypt records, or
scrub application payloads from storage. Deployments requiring confidentiality
MUST provide trusted-process isolation and filesystem/device encryption.

Acknowledgement assumes the OS, filesystem, mount options, and hardware honor
the write and sync calls. Important deployments SHOULD test crash recovery on
the exact stack and monitor:

- disk pressure and probe failures;
- catalog and offsets history headroom;
- cleanup debt;
- append unknown outcomes and partition unavailability;
- consumer delivery and commit lag;
- descriptor and active-partition limits; and
- snapshot diagnostics without confusing caches with authority.

Backups MUST capture a crash-consistent directory image or copy from a closed
store. Copying arbitrary live files independently can violate cross-file
metadata/segment ordering.

## 20. Evolution and proposal process

This is a living specification. Every implemented behavior covered by this
contract MUST be represented here. Contributors MUST use the following process
for a behavioral or persisted-format change:

1. Open a GitHub issue titled `[Proposal] <proposal title>`.
2. Describe the motivation, user-visible semantics, compatibility impact,
   durability/recovery implications, persistence impact, alternatives, and
   migration or rollout plan.
3. Obtain proposal review before implementation begins.
4. Implement the reviewed behavior and update this specification in the same
   implementation pull request.
5. Identify affected conformance classes and tests in the pull request.
6. Treat a pull request that changes behavior without the corresponding spec
   update as incomplete.

Pull-request review MUST explicitly answer:

- Does this change observable API, ownership, ordering, durability, recovery,
  retention, consumer, error, default, limit, or persistence behavior?
- If yes, where is the specification updated?
- Was the governing `[Proposal]` issue reviewed?
- Are old data, old callers, and unknown outcomes handled intentionally?
- Do tests exercise the new normative boundary and failure path?

Pure refactors that preserve every observable requirement do not need a
proposal, but their pull requests SHOULD state that no normative behavior or
format changes. Benchmark-only results belong in `BENCHMARKS.md`; they MUST NOT
be promoted to portable service-level guarantees here.

Breaking public or persistence changes MUST be explicit. A new decoder MUST
continue to classify unsupported versions separately from corruption. Reserved
fields MUST NOT gain meaning under the same schema version unless this document
already defines that interpretation.

## Appendix A. Public API inventory

This appendix identifies the current stable surface that API conformance must
cover. Go declarations remain the source for exact compile-time signatures;
the normative behavior is in the preceding sections.

### A.1 `api` package

- IDs: `TopicID`, `SegmentID`, their `IsZero`, `String`, and `GoString` methods,
  plus `ClusterMetadataTopicID()` and `ConsumerOffsetsTopicID()`.
- Data: `Header`, `AppendRequest`, `Record`, `RecordBatch`, `FetchOptions`, and
  `FetchResult`.
- Consumers: `GroupStart`, `ConsumerOptions`, `TopicPartition`,
  `ExplicitStart`, `ConsumerGroupMember`, and `ConsumerGroupOptions`.
- Domain errors: every sentinel listed in section 13.

### A.2 `storage.Store`

```go
Open(dir string) (*Store, error)
OpenWithOptions(dir string, options StoreOptions) (*Store, error)
(*Store).RootPath() string
(*Store).SnapshotDiagnostics() SnapshotDiagnostics
(*Store).OpenPartition(topic TopicID, partition uint32, options PartitionOptions) (*Partition, error)
(*Store).CreateTopic(name string, partitions uint32, options PartitionOptions) (TopicDescriptor, error)
(*Store).CreateTopicContext(ctx context.Context, name string, partitions uint32, options PartitionOptions) (TopicDescriptor, error)
(*Store).DescribeTopic(name string) (TopicDescriptor, error)
(*Store).ListTopics() ([]TopicDescriptor, error)
(*Store).OpenTopic(name string) ([]*Partition, error)
(*Store).OpenConsumer(ctx context.Context, groupID string, topic TopicID, partition uint32, options ConsumerOptions) (*Consumer, error)
(*Store).OpenConsumerGroup(ctx context.Context, groupID string, members []ConsumerGroupMember, options ConsumerGroupOptions) (*GroupConsumer, error)
(*Store).RunRetention(ctx context.Context) error
(*Store).CompactSystemLogs(ctx context.Context) error
(*Store).SaveSnapshots() error
(*Store).Stats() (StoreStats, error)
(*Store).Close() error
```

`OpenPartition` is also the lower-level API for a local partition that is not
owned by the catalog. For a nonzero, non-reserved uncataloged topic ID it
creates or opens `topics/<topic-id>/<partition>` with caller-supplied options;
it does not create a catalog entry. For a catalog-owned ID, the partition must
exist in the descriptor, persisted configuration replaces caller persistence
options, and the retained anchor MUST validate. Runtime tail limits remain
caller-selectable. Most applications SHOULD use `CreateTopic` and `OpenTopic`
so identity and policy are durably cataloged.

### A.3 `storage.Partition` and `storage.Reader`

```go
(*Partition).Append(ctx context.Context, request AppendRequest) (Record, error)
(*Partition).AppendBatch(batch RecordBatch) (uint64, error)
(*Partition).EndOffset() (uint64, error)
(*Partition).Read(offset uint64, maxRecords uint32) ([]Record, error)
(*Partition).Fetch(ctx context.Context, offset uint64, options FetchOptions) (FetchResult, error)
(*Partition).NewReader(start uint64) (*Reader, error)
(*Partition).Stats() PartitionStats
(*Partition).Close() error

(*Reader).Fetch(ctx context.Context, options FetchOptions) (FetchResult, error)
(*Reader).Seek(offset uint64) error
(*Reader).NextOffset() (uint64, error)
(*Reader).Close() error
```

### A.4 Managed consumers

```go
(*Consumer).Poll(ctx context.Context, options FetchOptions) (FetchResult, error)
(*Consumer).Commit(ctx context.Context, next uint64) error
(*Consumer).NextOffset() (uint64, error)
(*Consumer).Stats() (ConsumerStats, error)
(*Consumer).Close() error

(*GroupConsumer).Poll(ctx context.Context, topic TopicID, partition uint32, options FetchOptions) (FetchResult, error)
(*GroupConsumer).Commit(ctx context.Context, topic TopicID, partition uint32, next uint64) error
(*GroupConsumer).Subscriptions() []TopicPartition
(*GroupConsumer).Stats(topic TopicID, partition uint32) (ConsumerStats, error)
(*GroupConsumer).Close() error
```

### A.5 Format/tooling API

The exported v1 tooling surface is a lower-level compatibility class:

- constants `SegmentMagic`, `BatchMagic`, `FormatVersion`, framing sizes and
  global maxima;
- `StoreID`, `EventType`, and event values;
- `SegmentHeader`;
- `CRC32C`;
- `EncodeSegmentHeader` and `DecodeSegmentHeader`;
- `EncodeRecord` and `DecodeRecord`; and
- `EncodeBatch` and `DecodeBatch`.

Encoders reject invalid caller input with `ErrInvalidArgument` or
`ErrRecordTooLarge`. Decoders return `ErrCorruptLog` for malformed supported
bytes and `ErrUnsupportedFormat` for unsupported recognized versions/features.
Decoded payloads are caller-owned.

## Appendix B. Default limits

### B.1 Store defaults

| Option | Default |
|---|---:|
| tail bytes | 256 MiB |
| topics | 4,096 |
| user partitions | 65,536 |
| open user partitions | 1,024 |
| open sealed-segment files | 512 |
| catalog history | 1 GiB |
| offsets history | 1 GiB |
| consumer groups | 4,096 |
| consumer progress keys | 65,536 |
| disk safety/user-stop/resume bytes | 32/64/96 MiB |
| disk safety/user-stop/resume inodes | 16/32/64 |

Each open partition additionally owns one active writer descriptor. Pinned
readers may briefly make sealed-segment descriptors exceed the cache cap rather
than invalidating an in-use descriptor.

### B.2 Partition defaults

| Option | Default |
|---|---:|
| segment bytes | 72 bytes + 64 MiB |
| index stride | 4,096 bytes |
| batch bytes | 64 MiB |
| batch records | 65,536 |
| record bytes | min(16 MiB, bytes fitting configured batch) |
| in-flight bytes | 2 × batch bytes |
| in-flight records | 1,024 |
| admission waiters | 64 |
| batch linger | 0 |
| retention | disabled |
| tail | disabled unless both store and partition limits permit it |

### B.3 Fetch and consumer defaults

| Setting | Default |
|---|---:|
| fetch records | 1,024 |
| fetch bytes | 4 MiB |
| fetch wait | 0 |
| group start | earliest |
| progress timeout | 30 seconds |
| maximum partition fetch waiters | 64 |

## Appendix C. Conformance checklist

A full compatible implementation can use this minimum checklist:

- [ ] Acquires exclusive stable-directory ownership and never deletes `LOCK`.
- [ ] Preserves 16-byte IDs, reserved topic IDs, and caller ownership.
- [ ] Assigns contiguous offsets at one terminal per-partition serialization
      point.
- [ ] Acknowledges only after authoritative file/namespace sync boundaries.
- [ ] Distinguishes known-unwritten from unknown append/metadata/commit outcomes.
- [ ] Never rolls back or reuses a possibly durable offset.
- [ ] Implements `[L,H]` read-position rules and nil-versus-empty values.
- [ ] Treats `.log` files as authority and caches as replaceable.
- [ ] Replays catalog and offsets events with all revision/fencing invariants.
- [ ] Provides at-least-once delivery and explicit synchronous commits.
- [ ] Advances retention metadata before deletion and reports cleanup debt.
- [ ] Reserves protected disk capacity for control events.
- [ ] Applies finite defaults and all format maxima.
- [ ] Truncates only a verified incomplete final batch body after its complete
      valid header.
- [ ] Rejects complete corruption without modifying evidence.
- [ ] Returns stable domain errors through `errors.Is`.
- [ ] Bounds queues, memory, waiters, descriptors, diagnostics, and history.
- [ ] Updates this specification through the proposal process when behavior
      changes.
