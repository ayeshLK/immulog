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
