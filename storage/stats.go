// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ayeshLK/immulog/api"
)

// StoreStats is a bounded point-in-time summary of one Store. Its values are
// independently captured; it is not a cross-partition transaction snapshot.
type StoreStats struct {
	CapturedAt              time.Time
	Phase                   string
	PhaseDuration           time.Duration
	Closing                 bool
	Closed                  bool
	CloseDuration           time.Duration
	CloseCause              string
	OpenPartitions          uint32
	ActiveConsumers         uint32
	ActiveGroupConsumers    uint32
	TailBytesUsed           uint64
	TailBytesLimit          uint64
	CatalogHistoryLimit     uint64
	CatalogHistoryRemaining uint64
	OffsetsHistoryLimit     uint64
	OffsetsHistoryRemaining uint64
	ConsumerGroups          uint32
	ConsumerProgressKeys    uint32
	MaxConsumerGroups       uint32
	MaxConsumerProgressKeys uint32
	Catalog                 PartitionStats
	ConsumerOffsets         PartitionStats
	RetentionRunning        bool
	DiskPressure            DiskPressureStats
	Cleanup                 CleanupStats
}

// CleanupStats describes retired artifacts that still need physical cleanup.
// Counts and bytes are bounded observations; they do not advance usable disk
// capacity until the files have actually been removed and synced.
type CleanupStats struct {
	PendingSegments uint32
	PendingBytes    uint64
	OldestBase      uint64
	HasOldest       bool
	ScanTruncated   bool
	ScanError       bool
}

// LatencyStats is an instance-lifetime total and fixed-width histogram for one
// operation class. Bucket boundaries are 1us, 10us, 100us, 1ms, 10ms, 100ms,
// 1s, and greater than 1s.
type LatencyStats struct {
	Operations uint64
	Nanos      uint64
	Buckets    [8]uint64
}

// PartitionStats is a bounded point-in-time summary of one local partition.
// L is LogStartOffset and H is DurableEnd; no record payload is retained here.
type PartitionStats struct {
	Topic                api.TopicID
	Partition            uint32
	LogStartOffset       uint64
	DurableEnd           uint64
	LogicalLogBytes      uint64
	RetainedSegments     uint32
	InFlightRecords      uint32
	InFlightBytes        uint64
	WaitingAppends       uint32
	FetchWaiters         uint32
	TailEntries          uint32
	TailBytes            uint64
	Closed               bool
	Closing              bool
	Unavailable          bool
	AppendAcked          uint64
	AppendKnownUnwritten uint64
	AppendUnknown        uint64
	AdmissionWaits       uint64
	AdmissionWaitNanos   uint64
	RejectedBackpressure uint64
	RejectedClosing      uint64
	RejectedClosed       uint64
	RejectedUnavailable  uint64
	RejectedDiskPressure uint64
	WriteLatency         LatencyStats
	SyncLatency          LatencyStats
	NamespaceLatency     LatencyStats
	RejectedCanceled     uint64
	RejectedResource     uint64
	RejectedOther        uint64
}

// Stats returns a bounded operational summary without opening user partitions.
// It performs only the capped retired-artifact inspection needed for cleanup
// debt; all other values come from already-open state.
func (store *Store) Stats() (StoreStats, error) {
	capturedAt := time.Now()
	store.mu.Lock()
	lifecycle := StoreStats{CapturedAt: capturedAt, Closed: store.closed, Closing: store.closing.Load() && !store.closed, CloseCause: store.closeCause}
	if lifecycle.Closed {
		lifecycle.Phase = "closed"
	} else if lifecycle.Closing {
		lifecycle.Phase = "closing"
	} else {
		lifecycle.Phase = "open"
	}
	if !store.closeStartedAt.IsZero() {
		if store.closeFinishedAt.IsZero() {
			lifecycle.CloseDuration = capturedAt.Sub(store.closeStartedAt)
		} else {
			lifecycle.CloseDuration = store.closeFinishedAt.Sub(store.closeStartedAt)
		}
		lifecycle.PhaseDuration = lifecycle.CloseDuration
	} else if !store.openedAt.IsZero() {
		lifecycle.PhaseDuration = capturedAt.Sub(store.openedAt)
	}
	if lifecycle.Closed {
		store.mu.Unlock()
		return lifecycle, api.ErrClosed
	}
	catalog, offsets := store.catalog, store.offsets
	options := store.options
	groups, progressKeys := offsetsProjectionUsage(store.offsetsState)
	openPartitions := uint32(len(store.partitions))
	activeConsumers := uint32(len(store.consumers))
	activeGroupConsumers := uint32(len(store.groupConsumers))
	tailBudget := store.tailBudget
	store.mu.Unlock()
	stats := lifecycle
	stats.OpenPartitions = openPartitions
	stats.ActiveConsumers = activeConsumers
	stats.ActiveGroupConsumers = activeGroupConsumers
	stats.Cleanup = store.cleanupStats()
	stats.RetentionRunning = store.retentionStarted.Load() && !store.retentionStopped.Load()
	stats.DiskPressure = store.disk.snapshot()
	stats.Catalog = partitionStats(catalog)
	stats.ConsumerOffsets = partitionStats(offsets)
	stats.CatalogHistoryLimit = options.MaxCatalogHistoryBytes
	stats.CatalogHistoryRemaining = remainingHistoryCapacity(stats.CatalogHistoryLimit, stats.Catalog.LogicalLogBytes)
	stats.OffsetsHistoryLimit = options.MaxOffsetsHistoryBytes
	stats.OffsetsHistoryRemaining = remainingHistoryCapacity(stats.OffsetsHistoryLimit, stats.ConsumerOffsets.LogicalLogBytes)
	stats.ConsumerGroups = uint32(groups)
	stats.ConsumerProgressKeys = uint32(progressKeys)
	stats.MaxConsumerGroups = options.MaxConsumerGroups
	stats.MaxConsumerProgressKeys = options.MaxConsumerProgressKeys
	if tailBudget != nil {
		stats.TailBytesUsed = tailBudget.used()
		stats.TailBytesLimit = tailBudget.limit
	}
	if lifecycle.Closing {
		return stats, api.ErrClosing
	}
	return stats, nil
}

const latencyBucketCount = 8

func latencyStats(operations, nanos *atomic.Uint64, buckets *[latencyBucketCount]atomic.Uint64) LatencyStats {
	stats := LatencyStats{Operations: operations.Load(), Nanos: nanos.Load()}
	for index := range buckets {
		stats.Buckets[index] = buckets[index].Load()
	}
	return stats
}

func partitionLatencyStats(partition *Partition) (write, sync, namespace LatencyStats) {
	return latencyStats(&partition.writeOps, &partition.writeNanos, &partition.writeBuckets), latencyStats(&partition.syncOps, &partition.syncNanos, &partition.syncBuckets), latencyStats(&partition.namespaceOps, &partition.namespaceNanos, &partition.namespaceBuckets)
}

func (partition *Partition) recordAppendOutcome(err error) {
	if err == nil {
		saturatingIncrement(&partition.appendAcked)
		return
	}
	if errors.Is(err, api.ErrAppendOutcomeUnknown) {
		saturatingIncrement(&partition.appendUnknown)
		return
	}
	saturatingIncrement(&partition.appendKnownUnwritten)
}

func (partition *Partition) recordAdmissionOutcome(err error) {
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, api.ErrBackpressure):
		saturatingIncrement(&partition.rejectedBackpressure)
	case errors.Is(err, api.ErrClosing):
		saturatingIncrement(&partition.rejectedClosing)
	case errors.Is(err, api.ErrClosed):
		saturatingIncrement(&partition.rejectedClosed)
	case errors.Is(err, api.ErrPartitionUnavailable):
		saturatingIncrement(&partition.rejectedUnavailable)
	case errors.Is(err, api.ErrDiskPressure):
		saturatingIncrement(&partition.rejectedDiskPressure)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		saturatingIncrement(&partition.rejectedCanceled)
	case errors.Is(err, api.ErrResourceLimit):
		saturatingIncrement(&partition.rejectedResource)
	default:
		saturatingIncrement(&partition.rejectedOther)
	}
}

func recordLatency(operations, nanos *atomic.Uint64, buckets *[latencyBucketCount]atomic.Uint64, elapsed time.Duration) {
	saturatingIncrement(operations)
	saturatingAdd(nanos, uint64(elapsed))
	boundaries := [...]time.Duration{time.Microsecond, 10 * time.Microsecond, 100 * time.Microsecond, time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second}
	bucket := len(boundaries)
	for index, boundary := range boundaries {
		if elapsed < boundary {
			bucket = index
			break
		}
	}
	saturatingIncrement(&buckets[bucket])
}

func saturatingIncrement(value *atomic.Uint64) {
	for {
		current := value.Load()
		if current == math.MaxUint64 || value.CompareAndSwap(current, current+1) {
			return
		}
	}
}

func saturatingAdd(value *atomic.Uint64, amount uint64) {
	for {
		current := value.Load()
		if amount > math.MaxUint64-current {
			amount = math.MaxUint64 - current
		}
		if value.CompareAndSwap(current, current+amount) {
			return
		}
	}
}

const maxCleanupDiagnosticSegments = 1024

func (store *Store) cleanupStats() CleanupStats {
	if store == nil {
		return CleanupStats{}
	}
	type candidate struct {
		dir  string
		base uint64
	}
	store.mu.Lock()
	candidates := make([]candidate, 0)
	candidateCount := 0
	for _, topic := range store.catalogState.topicsByID {
		for partition, retired := range topic.retired {
			for base := range retired {
				candidateCount++
				if len(candidates) >= maxCleanupDiagnosticSegments {
					continue
				}
				candidates = append(candidates, candidate{
					dir: filepath.Join(store.rootPath, "topics", topic.descriptor.ID.String(), fmt.Sprintf("%d", partition)), base: base,
				})
			}
		}
	}
	truncated := candidateCount > len(candidates)
	store.mu.Unlock()
	stats := CleanupStats{ScanTruncated: truncated}
	for _, candidate := range candidates {
		path := filepath.Join(candidate.dir, fmt.Sprintf("%020d.log", candidate.base))
		paths := []string{path, indexPath(path, false), indexPath(path, true)}
		var pending bool
		for _, artifact := range paths {
			info, err := fsStat(artifact)
			if err != nil {
				if !os.IsNotExist(err) {
					stats.ScanError = true
				}
				continue
			}
			pending = true
			if info.Size() > 0 {
				stats.PendingBytes = saturatingUint64Add(stats.PendingBytes, uint64(info.Size()))
			}
		}
		if pending {
			if stats.PendingSegments < math.MaxUint32 {
				stats.PendingSegments++
			}
			if !stats.HasOldest || candidate.base < stats.OldestBase {
				stats.OldestBase = candidate.base
				stats.HasOldest = true
			}
		}
	}
	return stats
}

func boundedDiagnosticError(err error) string {
	if err == nil {
		return ""
	}
	const maxDiagnosticErrorBytes = 512
	value := err.Error()
	if len(value) > maxDiagnosticErrorBytes {
		return value[:maxDiagnosticErrorBytes]
	}
	return value
}

func saturatingUint64Add(current, amount uint64) uint64 {
	if amount > math.MaxUint64-current {
		return math.MaxUint64
	}
	return current + amount
}

func remainingHistoryCapacity(limit, used uint64) uint64 {
	if used >= limit {
		return 0
	}
	return limit - used
}

// Stats returns a bounded operational summary of this local partition.
func (partition *Partition) Stats() PartitionStats {
	return partitionStats(partition)
}

func partitionStats(partition *Partition) PartitionStats {
	if partition == nil {
		return PartitionStats{}
	}
	partition.queueMu.Lock()
	inFlightRecords := partition.admittedRecords
	inFlightBytes := partition.admittedBytes
	waitingAppends := partition.waiting
	partition.queueMu.Unlock()

	writeLatency, syncLatency, namespaceLatency := partitionLatencyStats(partition)
	stats := PartitionStats{InFlightRecords: inFlightRecords, InFlightBytes: inFlightBytes, WaitingAppends: waitingAppends,
		AppendAcked: partition.appendAcked.Load(), AppendKnownUnwritten: partition.appendKnownUnwritten.Load(), AppendUnknown: partition.appendUnknown.Load(),
		AdmissionWaits: partition.admissionWaits.Load(), AdmissionWaitNanos: partition.admissionWaitNanos.Load(),
		RejectedBackpressure: partition.rejectedBackpressure.Load(), RejectedClosing: partition.rejectedClosing.Load(),
		RejectedClosed: partition.rejectedClosed.Load(), RejectedUnavailable: partition.rejectedUnavailable.Load(), RejectedDiskPressure: partition.rejectedDiskPressure.Load(),
		RejectedCanceled: partition.rejectedCanceled.Load(), RejectedResource: partition.rejectedResource.Load(), RejectedOther: partition.rejectedOther.Load(),
		WriteLatency: writeLatency, SyncLatency: syncLatency, NamespaceLatency: namespaceLatency}
	partition.mu.RLock()
	stats.Topic = partition.topic
	stats.Partition = partition.partition
	stats.DurableEnd = partition.logEnd
	stats.FetchWaiters = partition.fetchWaiters
	stats.Closed = partition.closed
	stats.Closing = partition.closing
	stats.Unavailable = partition.unavailable
	stats.RetainedSegments = uint32(len(partition.segments))
	if len(partition.segments) != 0 {
		stats.LogStartOffset = partition.segments[0].header.BaseOffset
	}
	for _, segment := range partition.segments {
		if segment == nil || segment.size < 0 {
			continue
		}
		if uint64(segment.size) > ^uint64(0)-stats.LogicalLogBytes {
			stats.LogicalLogBytes = ^uint64(0)
			break
		}
		stats.LogicalLogBytes += uint64(segment.size)
	}
	tail := partition.tail
	partition.mu.RUnlock()

	if tail != nil {
		tail.mu.Lock()
		stats.TailEntries = tail.count
		stats.TailBytes = tail.bytes
		tail.mu.Unlock()
	}
	return stats
}
