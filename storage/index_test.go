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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func indexedBatch(topic api.TopicID, partition uint32, base uint64, timestamp int64, value string) api.RecordBatch {
	return api.RecordBatch{
		Topic: topic, Partition: partition, BaseOffset: base,
		Records: []api.Record{{
			Topic: topic, Partition: partition, Offset: base,
			Timestamp: timestamp, Value: []byte(value), Headers: []api.Header{},
		}},
	}
}

func TestSparseIndexesHaveIndependentEnvelopesAndRegressingTimeHints(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{IndexStride: 1})
	if err != nil {
		t.Fatal(err)
	}
	for index, timestamp := range []int64{-10, -20, -5, -30} {
		if _, err := partition.AppendBatch(indexedBatch(topic, 0, uint64(index), timestamp, string(rune('a'+index)))); err != nil {
			t.Fatal(err)
		}
	}

	segment := partition.segments[0]
	for _, time := range []bool{false, true} {
		if _, err := os.Stat(indexPath(segment.path, time)); !os.IsNotExist(err) {
			t.Fatalf("index sidecar exists before close (time=%t): %v", time, err)
		}
	}
	got, err := partition.Read(2, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []api.Record{
		{Topic: topic, Partition: 0, Offset: 2, Timestamp: -5, Value: []byte("c"), Headers: []api.Header{}},
		{Topic: topic, Partition: 0, Offset: 3, Timestamp: -30, Value: []byte("d"), Headers: []api.Header{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed read = %#v, want %#v", got, want)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	offsetData, err := os.ReadFile(indexPath(segment.path, false))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(offsetData), int(IndexHeaderBytes)+4*int(OffsetIndexEntryBytes); got != want {
		t.Fatalf("offset index bytes = %d, want %d", got, want)
	}
	if _, err := decodeIndexHeader(offsetData); err != nil {
		t.Fatalf("offset index header: %v", err)
	}
	for position := int(IndexHeaderBytes); position < len(offsetData); position += int(OffsetIndexEntryBytes) {
		if _, err := decodeOffsetIndexEntry(offsetData[position : position+int(OffsetIndexEntryBytes)]); err != nil {
			t.Fatalf("offset index entry at %d: %v", position, err)
		}
	}

	timeData, err := os.ReadFile(indexPath(segment.path, true))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(timeData), int(IndexHeaderBytes)+4*int(TimeIndexEntryBytes); got != want {
		t.Fatalf("time index bytes = %d, want %d", got, want)
	}
	var previous int64
	for index, position := 0, int(IndexHeaderBytes); position < len(timeData); index, position = index+1, position+int(TimeIndexEntryBytes) {
		entry, err := decodeTimeIndexEntry(timeData[position : position+int(TimeIndexEntryBytes)])
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 && entry.prefixMax != -10 {
			t.Fatalf("first time prefix maximum = %d, want -10", entry.prefixMax)
		}
		if index > 0 && entry.prefixMax < previous {
			t.Fatalf("time prefix maximum regressed from %d to %d", previous, entry.prefixMax)
		}
		previous = entry.prefixMax
	}

}

func TestIndexesPublishWhenSegmentBecomesInactive(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	first := indexedBatch(topic, 0, 0, 1, "value")
	encoded, err := EncodeBatch(first)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		SegmentBytes: uint64(SegmentHeaderBytes) + uint64(len(encoded)) + 1,
		BatchBytes:   4096,
		BatchRecords: 1,
		RecordBytes:  1024,
		IndexStride:  1,
	}
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(first); err != nil {
		t.Fatal(err)
	}
	priorPath := partition.segments[0].path
	for _, time := range []bool{false, true} {
		if _, err := os.Stat(indexPath(priorPath, time)); !os.IsNotExist(err) {
			t.Fatalf("index sidecar exists before roll (time=%t): %v", time, err)
		}
	}
	if _, err := partition.AppendBatch(indexedBatch(topic, 0, 1, 2, "value")); err != nil {
		t.Fatal(err)
	}
	if len(partition.segments) != 2 {
		t.Fatalf("segment count = %d, want 2", len(partition.segments))
	}
	for _, time := range []bool{false, true} {
		if _, err := os.Stat(indexPath(priorPath, time)); err != nil {
			t.Fatalf("closed segment index (time=%t) = %v", time, err)
		}
		if _, err := os.Stat(indexPath(partition.segments[1].path, time)); !os.IsNotExist(err) {
			t.Fatalf("active segment index exists before close (time=%t): %v", time, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, time := range []bool{false, true} {
		if _, err := os.Stat(indexPath(partition.segments[1].path, time)); err != nil {
			t.Fatalf("active segment index after close (time=%t) = %v", time, err)
		}
	}
}

func TestLaggingOrCorruptIndexesNeverHideLogRecords(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{IndexStride: 1})
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range []string{"one", "two", "three"} {
		if _, err := partition.AppendBatch(indexedBatch(topic, 0, uint64(index), int64(index), value)); err != nil {
			t.Fatal(err)
		}
	}
	segmentPath := partition.segments[0].path
	indexPathname := indexPath(segmentPath, false)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(indexPathname)
	if err != nil {
		t.Fatal(err)
	}
	lagging := data[:int(IndexHeaderBytes)+int(OffsetIndexEntryBytes)]
	if err := os.WriteFile(indexPathname, lagging, 0o644); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err = store.OpenPartition(topic, 0, PartitionOptions{IndexStride: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := partition.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || string(got[2].Value) != "three" {
		t.Fatalf("lagging-index read = %#v, want all three records", got)
	}
	unchanged, err := os.ReadFile(indexPathname)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(unchanged, lagging) {
		t.Fatal("startup rewrote a valid lagging index")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err = os.ReadFile(indexPathname)
	if err != nil {
		t.Fatal(err)
	}
	data[0] ^= 0xff
	if err := os.WriteFile(indexPathname, data, 0o644); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err = store.OpenPartition(topic, 0, PartitionOptions{IndexStride: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err = partition.Read(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Value) != "two" || string(got[1].Value) != "three" {
		t.Fatalf("rebuilt-index read = %#v, want records two and three", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	finalData, err := os.ReadFile(indexPathname)
	if err != nil {
		t.Fatal(err)
	}
	if string(finalData[:8]) != IndexMagic || binary.LittleEndian.Uint16(finalData[10:12]) != IndexHeaderBytes {
		t.Fatal("corrupt index was not rebuilt")
	}
}

func TestIndexHeaderOnlyCacheIsValidForEmptySegment(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, time := range []bool{false, true} {
		path := indexPath(partition.segments[0].path, time)
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("index sidecar exists before close (time=%t): %v", time, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, time := range []bool{false, true} {
		path := indexPath(partition.segments[0].path, time)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != int(IndexHeaderBytes) {
			t.Fatalf("empty index %q has %d bytes, want header-only", filepath.Base(path), len(data))
		}
	}
}

func TestCloseOnlyCheckpointsDirtySegmentIndexes(t *testing.T) {
	var indexWrites atomic.Int64
	base := currentFileSystem()
	counting := base
	// Count only user-partition sidecars; the two system logs checkpoint
	// their own active segments during the same close.
	counting.createTmp = func(directory, pattern string) (*os.File, error) {
		if strings.HasPrefix(pattern, ".index-") && strings.Contains(directory, string(os.PathSeparator)+"topics"+string(os.PathSeparator)) {
			indexWrites.Add(1)
		}
		return base.createTmp(directory, pattern)
	}
	fileSystemMu.Lock()
	previous := fileSystem
	fileSystem = counting
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
	topic, err := store.CreateTopic("orders", 1, PartitionOptions{SegmentBytes: 1024, BatchBytes: 512, RecordBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	partition := partitions[0]
	for index := range 60 {
		if _, err := partition.Append(context.Background(), api.AppendRequest{
			Topic: topic.ID, Partition: 0,
			Key: []byte(fmt.Sprintf("k-%d", index)), Value: make([]byte, 100),
		}); err != nil {
			t.Fatal(err)
		}
	}
	sealed := len(partition.segments) - 1
	if sealed < 4 {
		t.Fatalf("sealed segment count = %d, want at least 4", sealed)
	}
	indexWrites.Store(0)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Only the active segment's two sidecars may still be dirty at close.
	if written := indexWrites.Load(); written > 2 {
		t.Fatalf("close republished %d index sidecars across %d sealed segments, want at most 2", written, sealed)
	}

	// The sealed sidecars must still be the ones written when each segment
	// rolled, so a reopened partition keeps its seek hints.
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := reopened.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	for index, segment := range restored[0].segments[:sealed] {
		if len(segment.offsetIndex) == 0 {
			t.Fatalf("sealed segment %d lost its offset index", index)
		}
		if segment.indexDirty {
			t.Fatalf("sealed segment %d reloaded as dirty", index)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
