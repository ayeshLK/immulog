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
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestSegmentFileCacheEvictsLeastRecentlyUsedUnpinnedHandle(t *testing.T) {
	dir := t.TempDir()
	paths := make([]string, 4)
	for index := range paths {
		path := dir + "/" + fmt.Sprintf("segment-%d", index)
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		paths[index] = path
	}
	cache := newSegmentFileCache(2)

	first, err := cache.acquire(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	cache.release(paths[0])
	if _, err := cache.acquire(paths[1]); err != nil {
		t.Fatal(err)
	}
	cache.release(paths[1])
	if size := cache.size(); size != 2 {
		t.Fatalf("cache size = %d, want 2", size)
	}

	// paths[0] is now the least recently used unpinned entry; acquiring a third
	// path must evict it rather than the more recently used paths[1].
	if _, err := cache.acquire(paths[2]); err != nil {
		t.Fatal(err)
	}
	cache.release(paths[2])
	if size := cache.size(); size != 2 {
		t.Fatalf("cache size after eviction = %d, want 2", size)
	}
	reopened, err := cache.acquire(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	cache.release(paths[0])
	if reopened == first {
		t.Fatal("evicted path returned its closed handle instead of reopening")
	}
	if err := cache.closeAll(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentFileCacheNeverEvictsAPinnedHandle(t *testing.T) {
	dir := t.TempDir()
	pinnedPath := dir + "/pinned"
	if err := os.WriteFile(pinnedPath, []byte("pinned-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	cache := newSegmentFileCache(1)
	pinned, err := cache.acquire(pinnedPath)
	if err != nil {
		t.Fatal(err)
	}
	// pinnedPath is never released, so a burst of other acquisitions over
	// budget must not close the handle out from under the pinned reader.
	for index := range 5 {
		path := dir + "/" + fmt.Sprintf("other-%d", index)
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := cache.acquire(path); err != nil {
			t.Fatal(err)
		}
		cache.release(path)
	}
	data := make([]byte, 11)
	if _, err := pinned.ReadAt(data, 0); err != nil {
		t.Fatalf("pinned handle was closed while still acquired: %v", err)
	}
	cache.release(pinnedPath)
	if err := cache.closeAll(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenPartitionBoundsOpenSegmentFileDescriptors(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{SegmentBytes: 200, BatchBytes: 128, BatchRecords: 1, RecordBytes: 64}
	topic, err := store.CreateTopic("orders", 1, options)
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	partition := partitions[0]
	const records = 200
	for index := range records {
		if _, err := partition.Append(context.Background(), api.AppendRequest{
			Topic: topic.ID, Partition: 0, Value: []byte(fmt.Sprintf("v-%d", index)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(partition.segments) < 10 {
		t.Fatalf("segment count = %d, want at least 10 to make the bound meaningful", len(partition.segments))
	}
	live := 0
	for _, segment := range partition.segments[:len(partition.segments)-1] {
		if segment.file != nil {
			live++
		}
	}
	if live != 0 {
		t.Fatalf("%d sealed segments kept a live writer handle open, want 0", live)
	}
	if partition.segments[len(partition.segments)-1].file == nil {
		t.Fatal("active segment lost its writer handle")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReopenReleasesSealedSegmentHandlesInsteadOfAccumulating(t *testing.T) {
	dir := t.TempDir()
	options := PartitionOptions{SegmentBytes: 200, BatchBytes: 128, BatchRecords: 1, RecordBytes: 64}
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	topic, err := store.CreateTopic("orders", 1, options)
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	for index := range 60 {
		if _, err := partitions[0].Append(context.Background(), api.AppendRequest{
			Topic: topic.ID, Partition: 0, Value: []byte(fmt.Sprintf("v-%d", index)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Recovery opens every historical segment once to rebuild its batch map.
	// Before the bounded cache, none of those descriptors were ever released
	// until the store closed again, so reopening a long-lived log reclaimed
	// zero descriptors and fd usage only grew across the process lifetime.
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := reopened.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(restored[0].segments) < 10 {
		t.Fatalf("recovered segment count = %d, want at least 10", len(restored[0].segments))
	}
	live := 0
	for _, segment := range restored[0].segments {
		if segment.file != nil {
			live++
		}
	}
	if live != 1 {
		t.Fatalf("recovery left %d live segment handles open, want exactly 1 (the active segment)", live)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFetchAcquiresSealedSegmentsThroughTheBoundedCache(t *testing.T) {
	dir := t.TempDir()
	options := PartitionOptions{SegmentBytes: 200, BatchBytes: 128, BatchRecords: 1, RecordBytes: 64}
	store, err := OpenWithOptions(dir, StoreOptions{MaxOpenSegmentFiles: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	topic, err := store.CreateTopic("orders", 1, options)
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	partition := partitions[0]
	for index := range 40 {
		if _, err := partition.Append(context.Background(), api.AppendRequest{
			Topic: topic.ID, Partition: 0, Value: []byte(fmt.Sprintf("v-%d", index)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(partition.segments) < 5 {
		t.Fatalf("segment count = %d, want at least 5", len(partition.segments))
	}
	// Fetching from the very first sealed segment must still work even though
	// its descriptor was released after recovery/roll; the cache reopens it.
	result, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 40, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 40 {
		t.Fatalf("fetched %d records, want 40", len(result.Records))
	}
	if cached := store.segmentFiles.size(); cached > 2 {
		t.Fatalf("segment file cache held %d descriptors, want at most the configured 2", cached)
	}
}

func TestSegmentFileCacheReadersDoNotRaceWithEviction(t *testing.T) {
	dir := t.TempDir()
	options := PartitionOptions{SegmentBytes: 200, BatchBytes: 128, BatchRecords: 1, RecordBytes: 64}
	store, err := OpenWithOptions(dir, StoreOptions{MaxOpenSegmentFiles: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	topic, err := store.CreateTopic("orders", 1, options)
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	partition := partitions[0]
	for index := range 80 {
		if _, err := partition.Append(context.Background(), api.AppendRequest{
			Topic: topic.ID, Partition: 0, Value: []byte(fmt.Sprintf("v-%d", index)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	var wait sync.WaitGroup
	for worker := range 8 {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			offset := uint64(worker % 80)
			for range 20 {
				if _, err := partition.Fetch(context.Background(), offset, api.FetchOptions{MaxRecords: 4, MaxBytes: 1 << 16}); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	wait.Wait()
}
