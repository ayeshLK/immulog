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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
)

func TestStoreCloseDoesNotDeadlockWithRetentionWorker(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("close-retention", 1, PartitionOptions{
		RetentionTimeEnabled: true, MaxSegmentAge: time.Millisecond, RetentionCheck: time.Millisecond,
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}

	store.retentionMu.Lock()
	closed := make(chan error, 1)
	go func() {
		closed <- store.Close()
	}()
	time.Sleep(20 * time.Millisecond)
	store.retentionMu.Unlock()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Store.Close deadlocked with the retention worker")
	}
}

func TestRetentionRollPublishesOnlyTheFormerActiveSegment(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		RetentionTimeEnabled: true,
		RetentionDuration:    time.Hour,
		MaxSegmentAge:        time.Millisecond,
		RetentionCheck:       time.Hour,
		IndexStride:          1,
	}
	descriptor, err := store.CreateTopic("retention-roll-index", 1, options)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partition := partitions[0]
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("value")}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	priorPath := partition.segments[0].path
	if err := store.runRetentionAt(context.Background(), time.Now().Add(2*time.Millisecond)); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if len(partition.segments) != 2 {
		_ = store.Close()
		t.Fatalf("segment count after retention roll = %d, want 2", len(partition.segments))
	}
	for _, timeIndex := range []bool{false, true} {
		if _, err := os.Stat(indexPath(priorPath, timeIndex)); err != nil {
			_ = store.Close()
			t.Fatalf("former active index (time=%t) = %v", timeIndex, err)
		}
		if _, err := os.Stat(indexPath(partition.segments[1].path, timeIndex)); !os.IsNotExist(err) {
			_ = store.Close()
			t.Fatalf("new active index exists before close (time=%t): %v", timeIndex, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionRollReleasesFormerActiveSegmentHandleWhenNotYetRetired(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenWithOptions(dir, StoreOptions{MaxOpenSegmentFiles: 1})
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		RetentionTimeEnabled: true,
		RetentionDuration:    time.Hour,
		MaxSegmentAge:        time.Millisecond,
		RetentionCheck:       time.Hour,
	}
	descriptor, err := store.CreateTopic("retention-roll-handle", 1, options)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partition := partitions[0]
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("value")}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	// MaxSegmentAge rolls the segment, but RetentionDuration keeps the former
	// active segment retained rather than retired, so cleanupRetiredSegments
	// never runs on it; the roll itself must release the handle.
	if err := store.runRetentionAt(context.Background(), time.Now().Add(2*time.Millisecond)); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if len(partition.segments) != 2 {
		_ = store.Close()
		t.Fatalf("segment count after retention roll = %d, want 2", len(partition.segments))
	}
	if partition.segments[0].file != nil {
		_ = store.Close()
		t.Fatal("retention roll left a live handle on the former active, now-retained segment")
	}
	if partition.segments[1].file == nil {
		_ = store.Close()
		t.Fatal("retention roll did not keep a live handle on the new active segment")
	}
	// The released segment must still be readable through the bounded cache.
	if _, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1 << 16}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSizeRetentionPublishesBoundaryCleansArtifactsAndReopens(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		RecordBytes: 512, BatchBytes: 600, BatchRecords: 1, SegmentBytes: 700,
		RetentionSizeEnabled: true, RetentionBytes: 0, RetentionCheck: time.Hour,
	}
	descriptor, err := store.CreateTopic("expiring", 1, options)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partition := partitions[0]
	for offset := uint64(0); offset < 4; offset++ {
		record, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: bytes.Repeat([]byte("x"), 400)})
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		if record.Offset != offset {
			_ = store.Close()
			t.Fatalf("append offset = %d, want %d", record.Offset, offset)
		}
	}
	partitionDir := filepath.Join(dir, "topics", descriptor.ID.String(), "0")
	retiredPath := filepath.Join(partitionDir, "00000000000000000000.log")
	retiredData, err := os.ReadFile(retiredPath)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.runRetentionAt(context.Background(), time.Now()); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	updated, err := store.DescribeTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if got := updated.Partitions[0].RetainedL; got != 4 {
		_ = store.Close()
		t.Fatalf("retained L = %d, want 4", got)
	}
	if _, err := partition.Fetch(context.Background(), 0, api.FetchOptions{}); !errors.Is(err, api.ErrOffsetOutOfRange) {
		_ = store.Close()
		t.Fatalf("fetch expired offset error = %v, want ErrOffsetOutOfRange", err)
	}
	atEnd, err := partition.Fetch(context.Background(), 4, api.FetchOptions{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if len(atEnd.Records) != 0 || atEnd.NextOffset != 4 {
		_ = store.Close()
		t.Fatalf("fetch at retained end = %#v", atEnd)
	}
	files, err := discoverSegments(partitionDir)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].base != 4 {
		_ = store.Close()
		t.Fatalf("remaining segment chain = %#v, want only base 4", files)
	}

	// Model a crash after catalog publication but before this old artifact's unlink.
	if err := os.WriteFile(retiredPath, retiredData, 0o644); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	stats, err := store.Stats()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if stats.Cleanup.PendingSegments != 1 || stats.Cleanup.PendingBytes == 0 || !stats.Cleanup.HasOldest {
		_ = store.Close()
		t.Fatalf("cleanup debt stats = %#v", stats.Cleanup)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := os.Stat(retiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered retired artifact stat error = %v, want not exist", err)
	}
	partitions, err = store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	partition = partitions[0]
	if _, err := partition.Fetch(context.Background(), 0, api.FetchOptions{}); !errors.Is(err, api.ErrOffsetOutOfRange) {
		t.Fatalf("fetch expired offset after reopen error = %v, want ErrOffsetOutOfRange", err)
	}
	record, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("after-retention")})
	if err != nil {
		t.Fatal(err)
	}
	if record.Offset != 4 {
		t.Fatalf("post-retention append offset = %d, want 4", record.Offset)
	}
}

func TestRetentionMaintenanceRollsIdleActiveSegment(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	options := PartitionOptions{
		RetentionTimeEnabled: true, RetentionDuration: 0,
		MaxSegmentAge: time.Millisecond, RetentionCheck: 5 * time.Millisecond,
	}
	descriptor, err := store.CreateTopic("idle-retention", 1, options)
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partitions[0].Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("idle")}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		updated, err := store.DescribeTopic(descriptor.Name)
		if err != nil {
			t.Fatal(err)
		}
		if updated.Partitions[0].RetainedL == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("idle retention did not advance L before %s", deadline)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := partitions[0].Fetch(context.Background(), 0, api.FetchOptions{}); !errors.Is(err, api.ErrOffsetOutOfRange) {
		t.Fatalf("fetch idle-retired offset error = %v, want ErrOffsetOutOfRange", err)
	}
}

func TestRetentionUnknownCatalogOutcomePreservesRetiredArtifacts(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		RecordBytes: 512, BatchBytes: 600, BatchRecords: 1, SegmentBytes: 700,
		RetentionSizeEnabled: true, RetentionBytes: 0, RetentionCheck: time.Hour,
	}
	descriptor, err := store.CreateTopic("uncertain-retention", 1, options)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partition := partitions[0]
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: bytes.Repeat([]byte("x"), 400)}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	retiredPath := filepath.Join(dir, "topics", descriptor.ID.String(), "0", "00000000000000000000.log")
	store.catalogAppend = func(api.RecordBatch) (uint64, error) {
		return 0, errors.Join(api.ErrAppendOutcomeUnknown, errors.New("injected lost catalog acknowledgement"))
	}
	if err := store.runRetentionAt(context.Background(), time.Now()); !errors.Is(err, api.ErrAppendOutcomeUnknown) {
		_ = store.Close()
		t.Fatalf("retention error = %v, want ErrAppendOutcomeUnknown", err)
	}
	if _, err := os.Stat(retiredPath); err != nil {
		_ = store.Close()
		t.Fatalf("retired artifact was removed before a known catalog boundary: %v", err)
	}
	if !store.metadataUnavailable {
		_ = store.Close()
		t.Fatal("uncertain catalog outcome did not close metadata gates")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	partitions, err = store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	result, err := partitions[0].Fetch(context.Background(), 0, api.FetchOptions{})
	if err != nil || len(result.Records) != 1 || result.Records[0].Offset != 0 {
		t.Fatalf("recovered records after uncertain catalog outcome = %#v, %v", result, err)
	}
}

func TestRetentionHonorsOpenPartitionLimitAcrossCatalogPartitions(t *testing.T) {
	dir, descriptor := prepareRetentionLimitFixture(t, 2)
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 2, MaxOpenPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.runRetentionAt(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	updated, err := store.DescribeTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	for index, partition := range updated.Partitions {
		if partition.RetainedL != 1 {
			t.Fatalf("partition %d retained L = %d, want 1", index, partition.RetainedL)
		}
	}
	for pass := 0; pass < 3; pass++ {
		if err := store.runRetentionAt(context.Background(), time.Now()); err != nil {
			t.Fatal(err)
		}
		stats, err := store.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.OpenPartitions != 0 {
			t.Fatalf("pass %d open partitions = %d, want 0", pass, stats.OpenPartitions)
		}
		store.mu.Lock()
		temporary := len(store.retentionPartitions)
		store.mu.Unlock()
		if temporary != 0 {
			t.Fatalf("pass %d temporary retention partitions = %d, want 0", pass, temporary)
		}
	}
}

func TestBackgroundRetentionHonorsOpenPartitionLimitAcrossCatalogPartitions(t *testing.T) {
	dir, descriptor := prepareRetentionLimitFixtureWithCheck(t, 2, time.Millisecond)
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 2, MaxOpenPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		updated, err := store.DescribeTopic(descriptor.Name)
		if err != nil {
			t.Fatal(err)
		}
		stats, err := store.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if updated.Partitions[0].RetainedL == 1 && updated.Partitions[1].RetainedL == 1 && stats.OpenPartitions == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background retention state = L[%d %d], open=%d", updated.Partitions[0].RetainedL, updated.Partitions[1].RetainedL, stats.OpenPartitions)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRetentionReportsFullOpenPartitionLimitWithoutEviction(t *testing.T) {
	dir, descriptor := prepareRetentionLimitFixture(t, 2)
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 2, MaxOpenPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	opened, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.runRetentionAt(context.Background(), time.Now()); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("retention error = %v, want ErrResourceLimit", err)
	}
	if _, err := opened.EndOffset(); err != nil {
		t.Fatalf("public partition after retention = %v", err)
	}
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.OpenPartitions != 1 {
		t.Fatalf("open partitions = %d, want 1", stats.OpenPartitions)
	}
	updated, err := store.DescribeTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Partitions[0].RetainedL != 1 || updated.Partitions[1].RetainedL != 0 {
		t.Fatalf("retained boundaries = [%d %d], want [1 0]", updated.Partitions[0].RetainedL, updated.Partitions[1].RetainedL)
	}
}

func TestRetentionPartitionPublicAdoptionPreventsCleanup(t *testing.T) {
	tests := []struct {
		name    string
		acquire func(*testing.T, *Store, TopicDescriptor) func()
	}{
		{
			name: "partition",
			acquire: func(t *testing.T, store *Store, descriptor TopicDescriptor) func() {
				t.Helper()
				if _, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{}); err != nil {
					t.Fatal(err)
				}
				return func() {}
			},
		},
		{
			name: "topic",
			acquire: func(t *testing.T, store *Store, descriptor TopicDescriptor) func() {
				t.Helper()
				if _, err := store.OpenTopic(descriptor.Name); err != nil {
					t.Fatal(err)
				}
				return func() {}
			},
		},
		{
			name: "consumer",
			acquire: func(t *testing.T, store *Store, descriptor TopicDescriptor) func() {
				t.Helper()
				consumer, err := store.OpenConsumer(context.Background(), "retention-adoption", descriptor.ID, 0, api.ConsumerOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return func() {
					if err := consumer.Close(); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
		{
			name: "group consumer",
			acquire: func(t *testing.T, store *Store, descriptor TopicDescriptor) func() {
				t.Helper()
				consumer, err := store.OpenConsumerGroup(context.Background(), "retention-group-adoption", []api.ConsumerGroupMember{{
					Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}},
				}}, api.ConsumerGroupOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return func() {
					if err := consumer.Close(); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, descriptor := prepareRetentionLimitFixture(t, 1)
			store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 1, MaxOpenPartitions: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			key := partitionKey{topic: descriptor.ID, partition: 0}
			maintenance, temporary, err := store.openRetentionPartition(key)
			if err != nil {
				t.Fatal(err)
			}
			if !temporary {
				t.Fatal("retention partition was not temporary")
			}
			closeHandle := test.acquire(t, store, descriptor)
			defer closeHandle()
			if err := store.releaseRetentionPartition(key, maintenance); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			claimed := store.partitions[key]
			_, stillTemporary := store.retentionPartitions[key]
			store.mu.Unlock()
			if claimed != maintenance || stillTemporary {
				t.Fatalf("claimed partition = %p, temporary = %t, want %p, false", claimed, stillTemporary, maintenance)
			}
			if _, err := maintenance.EndOffset(); err != nil {
				t.Fatalf("adopted partition = %v", err)
			}
		})
	}
}

func TestRetentionPartitionRetirementFencesConcurrentPublicOpen(t *testing.T) {
	dir, descriptor := prepareRetentionLimitFixture(t, 1)
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 1, MaxOpenPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key := partitionKey{topic: descriptor.ID, partition: 0}
	maintenance, temporary, err := store.openRetentionPartition(key)
	if err != nil {
		t.Fatal(err)
	}
	if !temporary {
		t.Fatal("retention partition was not temporary")
	}

	maintenance.queueMu.Lock()
	released := make(chan error, 1)
	go func() {
		released <- store.releaseRetentionPartition(key, maintenance)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		store.mu.Lock()
		registration := store.retentionPartitions[key]
		retiring := registration.partition == maintenance && registration.retiring
		store.mu.Unlock()
		if retiring {
			break
		}
		if time.Now().After(deadline) {
			maintenance.queueMu.Unlock()
			t.Fatal("retention partition did not enter retiring state")
		}
		runtime.Gosched()
	}
	if _, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{}); !errors.Is(err, api.ErrConcurrentOperation) {
		maintenance.queueMu.Unlock()
		t.Fatalf("open during retirement error = %v, want ErrConcurrentOperation", err)
	}
	maintenance.queueMu.Unlock()
	select {
	case err := <-released:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retention partition cleanup did not finish")
	}

	opened, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if opened == maintenance {
		t.Fatal("public open reused the retired partition")
	}
	if _, err := opened.EndOffset(); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionFailureReleasesTemporaryPartition(t *testing.T) {
	dir, descriptor := prepareRetentionLimitFixture(t, 1)
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 1, MaxOpenPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	injected := errors.New("injected retention catalog append failure")
	store.catalogAppend = func(api.RecordBatch) (uint64, error) {
		return 0, injected
	}
	if err := store.runRetentionAt(context.Background(), time.Now()); !errors.Is(err, injected) {
		t.Fatalf("retention error = %v, want injected failure", err)
	}
	store.catalogAppend = nil
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.OpenPartitions != 0 {
		t.Fatalf("open partitions after failed retention = %d, want 0", stats.OpenPartitions)
	}
	store.mu.Lock()
	temporary := len(store.retentionPartitions)
	store.mu.Unlock()
	if temporary != 0 {
		t.Fatalf("temporary retention partitions after failure = %d, want 0", temporary)
	}
	opened, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.EndOffset(); err != nil {
		t.Fatal(err)
	}
}

func prepareRetentionLimitFixture(t *testing.T, partitionCount uint32) (string, TopicDescriptor) {
	t.Helper()
	return prepareRetentionLimitFixtureWithCheck(t, partitionCount, time.Hour)
}

func prepareRetentionLimitFixtureWithCheck(t *testing.T, partitionCount uint32, retentionCheck time.Duration) (string, TopicDescriptor) {
	t.Helper()
	dir := t.TempDir()
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: partitionCount, MaxOpenPartitions: partitionCount})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("retention-limit", partitionCount, PartitionOptions{
		RecordBytes: 512, BatchBytes: 600, BatchRecords: 1, SegmentBytes: 700,
		RetentionSizeEnabled: true, RetentionBytes: 0, RetentionCheck: retentionCheck,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	for index, partition := range partitions {
		if _, err := partition.Append(context.Background(), api.AppendRequest{
			Topic: descriptor.ID, Partition: uint32(index), Value: bytes.Repeat([]byte("x"), 400),
		}); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, descriptor
}

func TestTimeRetentionDoesNotExpireFutureTimestamp(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	options := PartitionOptions{
		RetentionTimeEnabled: true, RetentionDuration: 0,
		MaxSegmentAge: time.Millisecond, RetentionCheck: time.Hour,
	}
	descriptor, err := store.CreateTopic("future-time", 1, options)
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour).UnixMilli()
	if _, err := partitions[0].AppendBatch(api.RecordBatch{
		Topic: descriptor.ID, Partition: 0, BaseOffset: 0,
		Records: []api.Record{{Topic: descriptor.ID, Partition: 0, Offset: 0, Timestamp: future, Value: []byte("future")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.runRetentionAt(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	updated, err := store.DescribeTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Partitions[0].RetainedL != 0 {
		t.Fatalf("future-timestamp retention advanced L to %d, want 0", updated.Partitions[0].RetainedL)
	}
}
