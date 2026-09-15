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

type pollResult struct {
	result api.FetchResult
	err    error
}

func TestConsumerPollWaitsForDurableAppendAndAdvancesOnce(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("poll-wakeup", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192, TailSlots: 2, TailBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := store.OpenConsumer(context.Background(), "poll-wakeup", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan pollResult, 1)
	go func() {
		result, err := consumer.Poll(context.Background(), api.FetchOptions{MaxRecords: 1, MaxBytes: 1024, MaxWait: time.Second})
		completed <- pollResult{result: result, err: err}
	}()
	waitForFetchWaiter(t, partition)
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("arrived")}); err != nil {
		t.Fatal(err)
	}
	select {
	case completed := <-completed:
		if completed.err != nil || len(completed.result.Records) != 1 || completed.result.NextOffset != 1 || string(completed.result.Records[0].Value) != "arrived" {
			t.Fatalf("woken poll = (%#v, %v)", completed.result, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not wake after a durable append")
	}
	if next, err := consumer.NextOffset(); err != nil || next != 1 {
		t.Fatalf("next after woken poll = (%d, %v), want (1, nil)", next, err)
	}
}

func TestConsumerPollWaitTimeoutDoesNotAdvanceCursor(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("poll-timeout", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := store.OpenConsumer(context.Background(), "poll-timeout", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	result, err := consumer.Poll(context.Background(), api.FetchOptions{MaxRecords: 1, MaxBytes: 1024, MaxWait: 10 * time.Millisecond})
	if err != nil || len(result.Records) != 0 || result.NextOffset != 0 {
		t.Fatalf("timed poll = (%#v, %v)", result, err)
	}
	if next, err := consumer.NextOffset(); err != nil || next != 0 {
		t.Fatalf("next after timed poll = (%d, %v), want (0, nil)", next, err)
	}
}

func TestConsumerPollWaitWakesOnCloseWithoutAppend(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("poll-close", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := store.OpenConsumer(context.Background(), "poll-close", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan pollResult, 1)
	go func() {
		result, err := consumer.Poll(context.Background(), api.FetchOptions{MaxRecords: 1, MaxBytes: 1024, MaxWait: time.Second})
		completed <- pollResult{result: result, err: err}
	}()
	waitForFetchWaiter(t, partition)
	if err := partition.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case completed := <-completed:
		if !errors.Is(completed.err, api.ErrClosing) && !errors.Is(completed.err, api.ErrClosed) {
			t.Fatalf("poll woken by close = (%#v, %v), want closing error", completed.result, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not wake on partition close")
	}
}

func waitForFetchWaiter(t *testing.T, partition *Partition) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		partition.mu.RLock()
		waiting := partition.fetchWaiters != 0
		partition.mu.RUnlock()
		if waiting {
			return
		}
		select {
		case <-deadline:
			t.Fatal("poll did not register a durable-end waiter")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestGroupConsumerPollWaitsForDurableAppend(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("group-poll-wakeup", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	consumer, err := store.OpenConsumerGroup(context.Background(), "group-poll-wakeup", members, api.ConsumerGroupOptions{Start: api.GroupStartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan pollResult, 1)
	go func() {
		result, err := consumer.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024, MaxWait: time.Second})
		completed <- pollResult{result: result, err: err}
	}()
	waitForFetchWaiter(t, partition)
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("group-arrived")}); err != nil {
		t.Fatal(err)
	}
	select {
	case completed := <-completed:
		if completed.err != nil || len(completed.result.Records) != 1 || completed.result.NextOffset != 1 || string(completed.result.Records[0].Value) != "group-arrived" {
			t.Fatalf("woken group poll = (%#v, %v)", completed.result, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("group poll did not wake after a durable append")
	}
}
