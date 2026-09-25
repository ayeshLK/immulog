# Usage guide

This guide covers the public API for the common application journey. Start with
the [README](../README.md#quick-start) for the shortest example, and see the
[production guide](production.md) before selecting limits or relying on
recovery behavior.

`immulog` exposes durable storage through two packages:

- `api` contains caller-owned records, topic/partition identifiers, fetch and
  consumer options, and stable domain errors.
- `storage` contains the filesystem-backed store, topic catalog, partitions,
  consumers, retention, snapshots, and diagnostics.

## Open a store

A store owns one directory for its entire lifetime. `Open` creates the
necessary directory structure, acquires the stable `LOCK` file, and replays the
system logs before returning. A second process or store cannot open the same
directory until the first store is closed.

```go
store, err := storage.Open("/var/lib/my-service/events")
if err != nil {
	return err
}
defer store.Close()
```

Use a dedicated directory and do not remove, replace, truncate, or edit its
`LOCK` file. `OpenWithOptions` applies instance-only limits such as maximum
open partitions, history budgets, tail-cache bytes, and disk-pressure floors;
it does not rewrite persisted topic configuration.

## Create and open a topic

Topics are durable catalog entries. Their names, IDs, partition counts, and
partition configuration are fixed after successful creation.

```go
descriptor, err := store.CreateTopic("orders", 2, storage.PartitionOptions{
	SegmentBytes: 8 << 20,
	BatchBytes:   1 << 20,
	BatchRecords: 256,
})
if err != nil {
	return err
}

partitions, err := store.OpenTopic(descriptor.Name)
if err != nil {
	return err
}
```

`OpenTopic` returns the topic's partitions in partition-number order. It is
safe to call it again; already-open partitions are reused. Use
`DescribeTopic` for one catalog view or `ListTopics` for all catalog views.

`OpenPartition` is a lower-level API for a local partition that is not owned by
the topic catalog. Most applications should use `CreateTopic` and `OpenTopic`
so the catalog can record partition identity and configuration durably.

## Append records

`Partition.Append` accepts one `api.AppendRequest`. The request's `Topic` and
`Partition` must match the partition being written. The writer copies keys,
values, and headers before handing the request to its bounded ingress path, then
assigns the next offset and timestamp.

```go
record, err := partitions[0].Append(ctx, api.AppendRequest{
	Topic:     descriptor.ID,
	Partition: 0,
	Key:       []byte("order-123"),
	Value:     []byte(`{"status":"created"}`),
	Headers: []api.Header{
		{Name: "content-type", Value: []byte("application/json")},
	},
})
if err != nil {
	return err
}
fmt.Println("durable offset:", record.Offset)
```

A successful result represents a complete durable append after the required
file and namespace synchronization steps. `Append` has finite in-flight
limits and may return `api.ErrBackpressure`, `api.ErrDiskPressure`, or a
resource-limit error instead of waiting indefinitely.

Cancellation has an important boundary. If cancellation happens before the
request is admitted, the append does not proceed. Once the request has entered
the writer, cancellation may return `api.ErrAppendOutcomeUnknown`; the append
may still have become durable. Do not roll back or reuse that offset based only
on the error. Use an application-level idempotency key when retrying work whose
outcome is unknown.

For a complete pre-built batch, `AppendBatch` is available. It is a lower-level
operation: callers provide a contiguous `api.RecordBatch` with the matching
topic, partition, base offset, and record offsets. Prefer `Append` unless the
application already owns batch construction.

## Fetch records

`Partition.Fetch` returns a bounded, caller-owned prefix beginning at an
absolute offset. It does not wait for new records; `MaxWait` is used by managed
consumer polling instead.

```go
result, err := partitions[0].Fetch(ctx, record.Offset, api.FetchOptions{
	MaxRecords: 100,
	MaxBytes:   1 << 20,
})
if err != nil {
	return err
}
for _, record := range result.Records {
	process(record)
}
nextOffset := result.NextOffset
```

The next call can use `result.NextOffset`. An empty result leaves the requested
offset unchanged. The first retained offset is the current log start; an offset
before it or after the durable end returns `api.ErrOffsetOutOfRange`.

Fetch limits are hard bounds. A record that cannot fit in an otherwise empty
result returns `api.ErrFetchLimitTooSmall`; increase `MaxBytes` or use a
smaller record configuration. Zero record and byte limits select finite
library defaults.

Results and nested byte slices are caller-owned. The store does not retain
references to the request after append, and callers should not retain internal
references when handing records to other goroutines without making an
application-owned copy.

## Use a stateful reader

`NewReader` provides a serialized raw-read cursor without consumer-group
membership or commits.

```go
reader, err := partitions[0].NewReader(0)
if err != nil {
	return err
}
defer reader.Close()

for {
	result, err := reader.Fetch(ctx, api.FetchOptions{MaxRecords: 256})
	if err != nil {
		return err
	}
	if len(result.Records) == 0 {
		break
	}
	for _, record := range result.Records {
		process(record)
	}
}
```

A reader advances only after a successful fetch. `Seek` changes its next
absolute offset after validating the retained range. One reader does not
support concurrent operations; use separate readers when independent cursors
are required.

## Consume with a managed assignment

A managed `Consumer` stores a durable baseline and commit history in the
consumer-offsets system log. It provides at-least-once delivery for one
`(groupID, topic, partition)` key.

```go
consumer, err := store.OpenConsumer(ctx, "billing", descriptor.ID, 0, api.ConsumerOptions{
	Start: api.GroupStartEarliest,
	Fetch: api.FetchOptions{MaxRecords: 100},
})
if err != nil {
	return err
}
defer consumer.Close()

result, err := consumer.Poll(ctx, api.FetchOptions{})
if err != nil {
	return err
}
for _, record := range result.Records {
	if err := process(record); err != nil {
		return err
	}
}
if err := consumer.Commit(ctx, result.NextOffset); err != nil {
	return err
}
```

Commit only after the application has successfully processed the returned
prefix. `Commit` accepts a next offset no greater than the delivered prefix and
never moves progress backward. Closing a consumer does not auto-commit
uncommitted records.

The first use of a group key persists its start policy. Later opens of that
key must use the same `GroupStartEarliest`, `GroupStartLatest`, or
`GroupStartExplicit` policy. A new local assignment fences the old handle; the
old handle then returns `api.ErrAssignmentLost` rather than continuing to
process records.

`ProgressTimeout` bounds how long an assignment can remain active without an
admitted poll. A poll wait must be shorter than that timeout. Use
`Consumer.Stats` to observe delivery and commit lag without retaining record
payloads.

## Use an explicit consumer-group snapshot

`OpenConsumerGroup` replaces the complete local membership snapshot in one
durable transition. Each subscription must occur exactly once across the
members supplied in that call.

```go
group, err := store.OpenConsumerGroup(ctx, "billing-workers", []api.ConsumerGroupMember{
	{Subscriptions: []api.TopicPartition{
		{Topic: descriptor.ID, Partition: 0},
	}},
}, api.ConsumerGroupOptions{
	Start: api.GroupStartEarliest,
})
if err != nil {
	return err
}
defer group.Close()

result, err := group.Poll(ctx, descriptor.ID, 0, api.FetchOptions{})
if err != nil {
	return err
}
for _, record := range result.Records {
	if err := process(record); err != nil {
		return err
	}
}
if err := group.Commit(ctx, descriptor.ID, 0, result.NextOffset); err != nil {
	return err
}
```

A changed replacement snapshot fences every handle from the previous snapshot.
If the same live snapshot is opened again with equivalent canonical membership,
effective fetch/progress options, and explicit-start settings, the existing
handle is returned without a new durable generation. Aliases from coalesced
opens refer to the same handle, so closing any alias closes that live snapshot.
After a process restart, the first open installs a new assignment because live
handles and fetch/progress settings are not persisted. The complete membership
input remains the assignment contract; this API does not perform network
discovery or cross-process coordination.

## Configure retention

Retention is a catalog-owned topic policy. Enable time retention, size
retention, or both when creating the topic:

```go
retention := storage.PartitionOptions{
	RetentionTimeEnabled: true,
	RetentionDuration:    7 * 24 * time.Hour,
	MaxSegmentAge:        time.Hour,
	RetentionCheck:       time.Minute,
}
descriptor, err := store.CreateTopic("audit", 1, retention)
```

Retention operates on closed segments. The durable catalog log-start boundary
advances before the corresponding `.log`, `.index`, and `.timeindex` artifacts
are removed. A cleanup failure therefore leaves an observable cleanup debt,
not an earlier readable offset. `RunRetention` performs an explicit maintenance
pass; configured retention also starts the store's bounded maintenance worker.

System logs are not user-retained, and offsets are never reused. See the
[production guide](production.md#retention-and-disk-capacity) for capacity and
cleanup behavior.

## Snapshots and diagnostics

`SaveSnapshots` publishes replaceable projection caches for the catalog and
consumer offsets. Snapshots can speed startup but are not authoritative and
are not a backup format; the durable system logs remain the recovery source of
truth.

Publication cost does not grow with the length of a system log, and the
snapshot file write and sync run outside the store lock, so calling it
periodically does not stall appends, polls, or commits. A snapshot that falls
behind its log is simply ignored at startup in favor of replay.

```go
if err := store.SaveSnapshots(); err != nil {
	return err
}

stats, err := store.Stats()
if err != nil && !errors.Is(err, api.ErrClosing) {
	return err
}
fmt.Printf("phase=%s open-partitions=%d\n", stats.Phase, stats.OpenPartitions)
```

`Store.Stats`, `Partition.Stats`, and consumer stats return bounded operational
observations. They intentionally do not include record payloads or an
unbounded all-partition history.

## Handle stable errors

Use `errors.Is` instead of matching error strings:

```go
if errors.Is(err, api.ErrAppendOutcomeUnknown) {
	// Preserve the request identity and reconcile before retrying.
}
if errors.Is(err, api.ErrOffsetOutOfRange) {
	// The requested offset is outside the currently retained range.
}
```

Important error classes include:

- `ErrAppendOutcomeUnknown` for a cancellation or failure whose durable result
  cannot be classified locally.
- `ErrBackpressure`, `ErrDiskPressure`, and `ErrResourceLimit` for bounded
  admission or operating-capacity refusal.
- `ErrOffsetOutOfRange` and `ErrFetchLimitTooSmall` for invalid or too-small
  reads.
- `ErrAssignmentLost`, `ErrGroupUnavailable`, and `ErrCommitOutcomeUnknown`
  for managed consumer lifecycle and persistence outcomes.
- `ErrCorruptLog` and `ErrUnsupportedFormat` when conservative recovery refuses
  authoritative storage.
- `ErrClosing` and `ErrClosed` when the store or partition lifecycle no longer
  accepts the operation.

The full stable error set is defined in [`api/errors.go`](../api/errors.go).

## Reopen after a clean shutdown

The directory can be reopened after `Store.Close`:

```go
if err := store.Close(); err != nil {
	return err
}

store, err = storage.Open(dataDir)
if err != nil {
	return err
}
defer store.Close()
descriptor, err := store.DescribeTopic("orders")
if err != nil {
	return err
}
partitions, err = store.OpenTopic(descriptor.Name)
if err != nil {
	return err
}
```

Startup validates authoritative system logs, catalog state, partition identity,
and offset continuity. Only a verified incomplete final batch tail is eligible
for repair. Complete corruption and ambiguous artifacts fail closed rather than
being silently discarded.
