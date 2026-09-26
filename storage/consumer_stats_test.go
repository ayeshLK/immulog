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
	"runtime"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestConsumerStatsReportsIndependentDeliveryAndCommitLag(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("consumer-stats", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"one", "two", "three"} {
		if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte(value)}); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := store.OpenConsumer(context.Background(), "consumer-stats", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest, Fetch: api.FetchOptions{MaxRecords: 2, MaxBytes: 1024}})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := consumer.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Active || stats.DurableEnd != 3 || stats.NextDelivery != 0 || stats.CommittedOrInitial != 0 || stats.DeliveryLag != 3 || stats.CommitLag != 3 || stats.ExpiredCommittedDistance != 0 {
		t.Fatalf("initial consumer stats = %#v", stats)
	}
	if _, err := consumer.Poll(context.Background(), api.FetchOptions{}); err != nil {
		t.Fatal(err)
	}
	stats, err = consumer.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.NextDelivery != 2 || stats.DeliveryLag != 1 || stats.CommittedOrInitial != 0 || stats.CommitLag != 3 {
		t.Fatalf("post-poll consumer stats = %#v", stats)
	}
	if err := consumer.Commit(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	stats, err = consumer.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.CommittedOrInitial != 2 || stats.DeliveryLag != 1 || stats.CommitLag != 1 {
		t.Fatalf("post-commit consumer stats = %#v", stats)
	}
}

func TestPartitionLatencyStatsAreBoundedAndCountDurableAppend(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("latency-stats", 1, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	stats := partition.Stats()
	if stats.AppendAcked != 1 || stats.AppendKnownUnwritten != 0 || stats.AppendUnknown != 0 {
		t.Fatalf("append outcomes = %#v", stats)
	}
	for name, latency := range map[string]LatencyStats{"write": stats.WriteLatency, "sync": stats.SyncLatency} {
		if latency.Operations != 1 {
			t.Fatalf("%s operations = %#v, want one", name, latency)
		}
		var buckets uint64
		for _, count := range latency.Buckets {
			buckets += count
		}
		if buckets != latency.Operations || runtime.GOOS != "windows" && latency.Nanos == 0 {
			t.Fatalf("%s histogram = %#v", name, latency)
		}
	}
}
