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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
)

func TestProjectionSnapshotsPublishAndCorruptCachesFallBack(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("payments", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshots(); err != nil {
		t.Fatal(err)
	}
	if diagnostics := store.SnapshotDiagnostics(); diagnostics.Catalog != nil || diagnostics.Offsets != nil {
		t.Fatalf("snapshot publication diagnostics = %#v", diagnostics)
	}
	catalogSnapshot := filepath.Join(dir, clusterMetadataDir, snapshotPathName)
	if _, err := os.Stat(catalogSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(catalogSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(catalogSnapshot, data, 0o644); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics := store.SnapshotDiagnostics(); diagnostics.Catalog == nil {
		t.Fatal("corrupt catalog snapshot was not reported in diagnostics")
	}
	if _, err := store.DescribeTopic("payments"); err != nil {
		t.Fatalf("catalog replay with corrupt snapshot: %v", err)
	}
	if _, err := store.OpenPartition(api.ClusterMetadataTopicID, 0, PartitionOptions{}); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("reserved partition error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// openTestSystemLog opens one reserved system log directly so prefix-digest
// behavior can be exercised without a full store.
func openTestSystemLog(t *testing.T, dir string, topic api.TopicID, options PartitionOptions, storeID StoreID) *Partition {
	t.Helper()
	options.normalize()
	if err := options.validateWriter(); err != nil {
		t.Fatal(err)
	}
	partition, err := openPartition(dir, topic, 0, options, storeID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := partition.Close(); err != nil {
			t.Error(err)
		}
	})
	return partition
}

func TestSystemLogPrefixDigestCacheMatchesFullScan(t *testing.T) {
	dir := t.TempDir()
	storeID := StoreID{1, 2, 3, 4}
	partition := openTestSystemLog(t, dir, api.ClusterMetadataTopicID, PartitionOptions{SegmentBytes: 512, BatchBytes: 1024}, storeID)
	for index := range 64 {
		if _, err := partition.AppendBatch(testBatch(api.ClusterMetadataTopicID, 0, uint64(index), fmt.Sprintf("event-%d", index))); err != nil {
			t.Fatal(err)
		}
		cached, err := projectionPrefixDigest(partition, storeID, partition.logEnd)
		if err != nil {
			t.Fatal(err)
		}
		if partition.prefixDigest == nil {
			t.Fatalf("system log did not cache the prefix digest at offset %d", partition.logEnd)
		}
		partition.prefixDigest = nil
		scanned, err := projectionPrefixDigest(partition, storeID, partition.logEnd)
		if err != nil {
			t.Fatal(err)
		}
		if cached != scanned {
			t.Fatalf("cached digest %x at offset %d differs from the scanned digest %x", cached, partition.logEnd, scanned)
		}
	}
	if len(partition.segments) < 2 {
		t.Fatalf("segment count = %d, want at least 2 so the digest crosses a roll", len(partition.segments))
	}
	other, err := projectionPrefixDigest(partition, StoreID{9}, partition.logEnd)
	if err != nil {
		t.Fatal(err)
	}
	same, err := projectionPrefixDigest(partition, storeID, partition.logEnd)
	if err != nil {
		t.Fatal(err)
	}
	if other == same {
		t.Fatal("prefix digest ignored the store identity")
	}
}

func TestSystemLogPrefixDigestServesWithoutReadingSegments(t *testing.T) {
	dir := t.TempDir()
	storeID := StoreID{5, 6, 7}
	partition := openTestSystemLog(t, dir, api.ConsumerOffsetsTopicID, PartitionOptions{SegmentBytes: 4096, BatchBytes: 1024}, storeID)
	for index := range 8 {
		if _, err := partition.AppendBatch(testBatch(api.ConsumerOffsetsTopicID, 0, uint64(index), fmt.Sprintf("commit-%d", index))); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := projectionPrefixDigest(partition, storeID, partition.logEnd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(api.ConsumerOffsetsTopicID, 0, partition.logEnd, "commit-8")); err != nil {
		t.Fatal(err)
	}
	extended, err := projectionPrefixDigest(partition, storeID, partition.logEnd)
	if err != nil {
		t.Fatal(err)
	}
	if extended == expected {
		t.Fatal("prefix digest did not advance with the appended batch")
	}
	// Hiding the segments makes any re-read fail, so an answer here proves the
	// running hash served the request without touching the log.
	segments := partition.segments
	partition.segments = nil
	served, err := projectionPrefixDigest(partition, storeID, partition.logEnd)
	partition.segments = segments
	if err != nil {
		t.Fatalf("cached prefix digest re-read the log: %v", err)
	}
	if served != extended {
		t.Fatalf("cached digest %x differs from %x", served, extended)
	}
}

func TestPrefixDigestCacheDropsNonContiguousBatch(t *testing.T) {
	dir := t.TempDir()
	storeID := StoreID{8}
	partition := openTestSystemLog(t, dir, api.ClusterMetadataTopicID, PartitionOptions{SegmentBytes: 4096, BatchBytes: 1024}, storeID)
	partition.prefixDigest = &prefixDigestState{hash: newPrefixDigest(storeID, partition.topic, 0), storeID: storeID, covered: 5}
	partition.extendPrefixDigestLocked(9, []byte("gap"))
	if partition.prefixDigest != nil {
		t.Fatal("cache kept a prefix it no longer covers")
	}
}

func TestSaveSnapshotsPublishesOutsideStoreMutex(t *testing.T) {
	release := make(chan struct{})
	publishing := make(chan struct{}, 1)
	base := currentFileSystem()
	blocking := base
	blocking.sync = func(file *os.File) error {
		if strings.Contains(filepath.Base(file.Name()), "projection-snapshot") {
			select {
			case publishing <- struct{}{}:
			default:
			}
			<-release
		}
		return base.sync(file)
	}
	fileSystemMu.Lock()
	previous := fileSystem
	fileSystem = blocking
	fileSystemMu.Unlock()
	t.Cleanup(func() {
		fileSystemMu.Lock()
		fileSystem = previous
		fileSystemMu.Unlock()
	})

	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("orders", 1, PartitionOptions{}); err != nil {
		t.Fatal(err)
	}
	saved := make(chan error, 1)
	go func() { saved <- store.SaveSnapshots() }()
	select {
	case <-publishing:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("snapshot publication never reached its sync")
	}
	observed := make(chan error, 1)
	go func() {
		_, statsErr := store.Stats()
		observed <- statsErr
	}()
	select {
	case statsErr := <-observed:
		if statsErr != nil {
			close(release)
			t.Fatal(statsErr)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("store mutex was held across snapshot publication")
	}
	close(release)
	if err := <-saved; err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
