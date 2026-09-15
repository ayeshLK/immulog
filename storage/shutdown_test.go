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
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
)

func TestStoreClosePreservesReservedCommitAndFencesNewWork(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("shutdown-cutoff", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	consumer, err := store.OpenConsumer(context.Background(), "shutdown-cutoff", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Poll(context.Background(), api.FetchOptions{}); err != nil {
		t.Fatal(err)
	}

	appendOffsets := store.offsets.AppendBatch
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	store.offsetsAppend = func(batch api.RecordBatch) (uint64, error) {
		close(entered)
		<-release
		return appendOffsets(batch)
	}
	defer func() {
		if !released {
			close(release)
		}
		_ = store.Close()
	}()

	committed := make(chan error, 1)
	go func() { committed <- consumer.Commit(context.Background(), 1) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("commit did not reach the durable append barrier")
	}

	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	deadline := time.After(time.Second)
	for !store.closing.Load() {
		select {
		case <-deadline:
			t.Fatal("Close did not publish its cutoff")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := store.OpenConsumer(context.Background(), "after-cutoff", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest}); !errors.Is(err, api.ErrClosing) {
		t.Fatalf("consumer admission after cutoff = %v, want ErrClosing", err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("two")}); !errors.Is(err, api.ErrClosing) {
		t.Fatalf("append after cutoff = %v, want ErrClosing", err)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close completed before the reserved commit settled: %v", err)
	default:
	}

	close(release)
	released = true
	if err := <-committed; err != nil {
		t.Fatalf("reserved commit = %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close = %v", err)
	}
}

func TestStoreClosePreservesReservedCatalogCommandAndFencesNewMutation(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	appendCatalog := store.catalog.AppendBatch
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	store.catalogAppend = func(batch api.RecordBatch) (uint64, error) {
		close(entered)
		<-release
		return appendCatalog(batch)
	}
	defer func() {
		if !released {
			close(release)
		}
		_ = store.Close()
	}()

	created := make(chan error, 1)
	go func() {
		_, err := store.CreateTopic("reserved-catalog", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
		created <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("catalog command did not reach the durable append barrier")
	}

	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	deadline := time.After(time.Second)
	for !store.closing.Load() {
		select {
		case <-deadline:
			t.Fatal("Close did not publish its catalog cutoff")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := store.CreateTopic("after-cutoff", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192}); !errors.Is(err, api.ErrClosing) {
		t.Fatalf("catalog mutation after cutoff = %v, want ErrClosing", err)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close completed before the reserved catalog command settled: %v", err)
	default:
	}

	close(release)
	released = true
	if err := <-created; err != nil {
		t.Fatalf("reserved catalog command = %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close = %v", err)
	}
}
