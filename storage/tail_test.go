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

func TestTailServesDurablePartialBatchWithCallerOwnedResults(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{TailSlots: 2, TailBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 0, "one", "two")); err != nil {
		t.Fatal(err)
	}
	if partition.tail == nil {
		t.Fatal("tail was not configured")
	}
	if err := partition.segments[0].file.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil || len(result.Records) != 1 || result.NextOffset != 1 || string(result.Records[0].Value) != "one" {
		t.Fatalf("first tail fetch = (%#v, %v)", result, err)
	}
	result.Records[0].Value[0] = 'X'
	result, err = partition.Fetch(context.Background(), 1, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil || len(result.Records) != 1 || result.NextOffset != 2 || string(result.Records[0].Value) != "two" {
		t.Fatalf("partial tail fetch = (%#v, %v)", result, err)
	}
	replayed, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil || string(replayed.Records[0].Value) != "one" {
		t.Fatalf("tail cache was mutated through a caller result: (%#v, %v)", replayed, err)
	}
}

func TestTailEvictionFallsBackToSegmentsAtRequestedOffset(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{TailSlots: 1, TailBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 0, "one")); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 1, "two")); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := partition.tail.fetch(topic, 0, 0, 2, api.FetchOptions{MaxRecords: 2, MaxBytes: 1024}); err != nil || hit {
		t.Fatalf("evicted offset cache lookup = (hit=%t, err=%v), want miss", hit, err)
	}
	result, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 2, MaxBytes: 1024})
	if err != nil || len(result.Records) != 2 || result.Records[0].Offset != 0 || result.Records[1].Offset != 1 || result.NextOffset != 2 {
		t.Fatalf("segment fallback = (%#v, %v)", result, err)
	}
}

func TestTailPublishesIngressWriterBatchesAfterDurability(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{TailSlots: 2, TailBytes: 4096, BatchRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("ingress")}); err != nil {
		t.Fatal(err)
	}
	if err := partition.segments[0].file.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil || len(result.Records) != 1 || result.Records[0].Offset != 0 || string(result.Records[0].Value) != "ingress" {
		t.Fatalf("ingress tail fetch = (%#v, %v)", result, err)
	}
}

func TestStoreTailBudgetDropsOffersAndReclaimsOnClose(t *testing.T) {
	topic := testTopic()
	first := testBatch(topic, 0, 0, "budget")
	probe, ok := newDeliveredBatch(topic, 0, first.Records, 1)
	if !ok {
		t.Fatal("derive tail budget")
	}
	store, err := OpenWithOptions(t.TempDir(), StoreOptions{TailBytes: probe.bytes})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	options := PartitionOptions{TailSlots: 2, TailBytes: probe.bytes}
	one, err := store.OpenPartition(topic, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	two, err := store.OpenPartition(topic, 1, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := one.AppendBatch(first); err != nil {
		t.Fatal(err)
	}
	if used := store.tailBudget.used(); used != probe.bytes {
		t.Fatalf("budget after first offer = %d, want %d", used, probe.bytes)
	}
	if _, err := two.AppendBatch(testBatch(topic, 1, 0, "budget")); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := two.tail.fetch(topic, 1, 0, 1, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil || hit {
		t.Fatalf("global-budget cache lookup = (hit=%t, err=%v), want miss", hit, err)
	}
	result, err := two.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil || len(result.Records) != 1 || string(result.Records[0].Value) != "budget" {
		t.Fatalf("segment fallback after global-budget drop = (%#v, %v)", result, err)
	}
	if err := one.Close(); err != nil {
		t.Fatal(err)
	}
	if used := store.tailBudget.used(); used != 0 {
		t.Fatalf("budget after partition close = %d, want 0", used)
	}
	three, err := store.OpenPartition(topic, 2, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := three.AppendBatch(testBatch(topic, 2, 0, "budget")); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := three.tail.fetch(topic, 2, 0, 1, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil || !hit {
		t.Fatalf("cache lookup after budget reclamation = (hit=%t, err=%v), want hit", hit, err)
	}
}

func TestOpenWithOptionsRejectsOversizedTailBudget(t *testing.T) {
	if _, err := OpenWithOptions(t.TempDir(), StoreOptions{TailBytes: maxTailBytes + 1}); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("oversized StoreOptions error = %v, want ErrResourceLimit", err)
	}
}

func TestTailByteLimitEvictsOldestBatch(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	first := testBatch(topic, 0, 0, "byte-limit")
	probe, ok := newDeliveredBatch(topic, 0, first.Records, 1)
	if !ok {
		t.Fatal("derive tail byte limit")
	}
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{TailSlots: 2, TailBytes: probe.bytes})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(first); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(topic, 0, 1, "byte-limit")); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := partition.tail.fetch(topic, 0, 0, 2, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil || hit {
		t.Fatalf("byte-evicted cache lookup = (hit=%t, err=%v), want miss", hit, err)
	}
	if _, hit, err := partition.tail.fetch(topic, 0, 1, 2, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil || !hit {
		t.Fatalf("newest byte-limited cache lookup = (hit=%t, err=%v), want hit", hit, err)
	}
	if used := store.tailBudget.used(); used != probe.bytes {
		t.Fatalf("budget after local eviction = %d, want %d", used, probe.bytes)
	}
}

func TestContendedStoreTailBudgetDropsOfferWithoutDelayingAppend(t *testing.T) {
	store, err := OpenWithOptions(t.TempDir(), StoreOptions{TailBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{TailSlots: 2, TailBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}

	store.tailBudget.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := partition.AppendBatch(testBatch(topic, 0, 0, "contended"))
		done <- err
	}()
	select {
	case err := <-done:
		store.tailBudget.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		store.tailBudget.mu.Unlock()
		t.Fatal("append waited for the contended tail budget")
	}
	if _, hit, err := partition.tail.fetch(topic, 0, 0, 1, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil || hit {
		t.Fatalf("contended-budget cache lookup = (hit=%t, err=%v), want miss", hit, err)
	}
	result, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil || len(result.Records) != 1 || string(result.Records[0].Value) != "contended" {
		t.Fatalf("segment fallback after contended budget = (%#v, %v)", result, err)
	}
}
