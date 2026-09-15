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
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
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
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != int(IndexHeaderBytes) {
			t.Fatalf("empty index %q has %d bytes, want header-only", filepath.Base(path), len(data))
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
