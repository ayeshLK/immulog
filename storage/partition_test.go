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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func testBatch(topic api.TopicID, partition uint32, base uint64, values ...string) api.RecordBatch {
	records := make([]api.Record, len(values))
	for index, value := range values {
		records[index] = api.Record{
			Topic: topic, Partition: partition, Offset: base + uint64(index),
			Timestamp: int64(index + 1), Value: []byte(value), Headers: []api.Header{},
		}
	}
	return api.RecordBatch{Topic: topic, Partition: partition, BaseOffset: base, Records: records}
}

func TestStorePartitionAppendReadAndReopen(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 2, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := partition.AppendBatch(testBatch(topic, 2, 0, "one", "two")); err != nil || got != 0 {
		t.Fatalf("first append = (%d, %v), want (0, nil)", got, err)
	}
	if got, err := partition.AppendBatch(testBatch(topic, 2, 2, "three")); err != nil || got != 2 {
		t.Fatalf("second append = (%d, %v), want (2, nil)", got, err)
	}
	if got, err := partition.EndOffset(); err != nil || got != 3 {
		t.Fatalf("end offset = (%d, %v), want (3, nil)", got, err)
	}
	want := append(testBatch(topic, 2, 0, "one", "two").Records, testBatch(topic, 2, 2, "three").Records...)
	got, err := partition.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("read records = %#v, want %#v", got, want)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(dir, "LOCK")
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("LOCK after close: %v", err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err = store.OpenPartition(topic, 2, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := partition.EndOffset(); err != nil || got != 3 {
		t.Fatalf("recovered end offset = (%d, %v), want (3, nil)", got, err)
	}
	got, err = partition.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered records = %#v, want %#v", got, want)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRejectsConcurrentOpenAndAllowsReopen(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, api.ErrDataDirLocked) {
		t.Fatalf("second open error = %v, want ErrDataDirLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionRollsBeforeWritingPastConfiguredSegmentSize(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{SegmentBytes: 161})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 0, "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 1, "b")); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "topics", topic.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	logCount := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			logCount++
		}
	}
	if logCount != 2 {
		t.Fatalf("segment count = %d, want 2", logCount)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryTruncatesOnlyVerifiedIncompleteFinalBatch(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 0, "durable")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "topics", topic.String(), "0", "00000000000000000000.log")
	unknown, err := EncodeBatch(testBatch(topic, 0, 1, "unknown"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(unknown[:BatchHeaderBytes]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err = store.OpenPartition(topic, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := partition.EndOffset(); err != nil || got != 1 {
		t.Fatalf("recovered end offset = (%d, %v), want (1, nil)", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryPreservesCompleteCorruptBatch(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 0, "durable")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "topics", topic.String(), "0", "00000000000000000000.log")
	unknown, err := EncodeBatch(testBatch(topic, 0, 1, "corrupt"))
	if err != nil {
		t.Fatal(err)
	}
	unknown[len(unknown)-5] ^= 1
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(unknown); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.OpenPartition(topic, 0, PartitionOptions{}); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		_ = reopened.Close()
		t.Fatalf("open corrupt log error = %v, want ErrCorruptLog", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatal("recovery modified a complete corrupt batch")
	}
}
