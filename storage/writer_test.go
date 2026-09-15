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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
)

func waitForWriterState(t *testing.T, partition *Partition, ready func(queue int, admitted uint32) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		partition.queueMu.Lock()
		queue, admitted := len(partition.queue), partition.admittedRecords
		partition.queueMu.Unlock()
		if ready(queue, admitted) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for writer state")
}

func TestAppendAssignsOffsetsConcurrentlyAndCopiesInputs(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{
		BatchRecords:    3,
		BatchLinger:     2 * time.Millisecond,
		InFlightRecords: 32,
		InFlightBytes:   64 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	const count = 12
	results := make(chan api.Record, count)
	errorsCh := make(chan error, count)
	var group sync.WaitGroup
	for index := 0; index < count; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			value := []byte("value-" + string(rune('a'+index)))
			record, err := partition.Append(context.Background(), api.AppendRequest{
				Topic: topic, Partition: 0, Key: []byte("key"), Value: value,
				Headers: []api.Header{{Name: "x", Value: []byte("y")}},
			})
			value[0] = 'X'
			if err != nil {
				errorsCh <- err
				return
			}
			results <- record
		}(index)
	}
	group.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}

	records := make([]api.Record, 0, count)
	for record := range results {
		records = append(records, record)
	}
	if len(records) != count {
		t.Fatalf("append result count = %d, want %d", len(records), count)
	}
	offsets := make([]uint64, len(records))
	for index, record := range records {
		offsets[index] = record.Offset
		if len(record.Value) == 0 || record.Value[0] == 'X' {
			t.Fatalf("record %d retained caller buffer", record.Offset)
		}
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	for index, offset := range offsets {
		if offset != uint64(index) {
			t.Fatalf("assigned offsets = %v, want contiguous offset %d at position %d", offsets, index, index)
		}
	}
	got, err := partition.Read(0, count)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != count {
		t.Fatalf("durable record count = %d, want %d", len(got), count)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendCancellationBeforeClaimDoesNotPersist(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{
		BatchLinger:     time.Second,
		InFlightBytes:   165,
		InFlightRecords: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("x")})
		firstDone <- err
	}()
	waitForWriterState(t, partition, func(queue int, admitted uint32) bool { return queue == 0 && admitted == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = partition.Append(ctx, api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("x")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled append error = %v, want context.Canceled", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if got, err := partition.EndOffset(); err != nil || got != 1 {
		t.Fatalf("end offset after canceled append = (%d, %v), want (1, nil)", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseDrainsAdmittedAppendAndRejectsNewWork(t *testing.T) {
	dir := t.TempDir()
	topic := testTopic()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{BatchLinger: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("drain")})
		result <- err
	}()
	waitForWriterState(t, partition, func(queue int, admitted uint32) bool { return queue == 0 && admitted == 1 })
	if err := partition.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("admitted append during close = %v, want success", err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("late")}); !errors.Is(err, api.ErrClosed) {
		t.Fatalf("append after close = %v, want ErrClosed", err)
	}
	if err := partition.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
