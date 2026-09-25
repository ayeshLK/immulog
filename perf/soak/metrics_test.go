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

package soak

import (
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

func TestSoakMetricsMeasurementAndSampleCap(t *testing.T) {
	metrics := newSoakMetrics(time.Millisecond, 2)
	start := time.Now().Add(-100 * time.Millisecond)
	metrics.startMeasurement(start)
	resources := soakResourceSample{Goroutines: 3}
	stats := []storage.ConsumerStats{{Partition: 0, DurableEnd: 10, NextDelivery: 8, CommittedOrInitial: 7, DeliveryLag: 2, CommitLag: 3}}
	metrics.recordSample(time.Now(), resources, stats)
	metrics.recordSample(time.Now(), resources, stats)
	metrics.recordSample(time.Now(), resources, stats)
	metrics.finishMeasurement(time.Now())

	samples, dropped := metrics.sampleSnapshot()
	if len(samples) != 2 || dropped != 1 {
		t.Fatalf("samples = (%d, %d), want (2, 1)", len(samples), dropped)
	}
	if samples[0].Partitions["soak-stable/0"].DurableEnd != 10 {
		t.Fatalf("sample partition = %#v", samples[0].Partitions)
	}
	if duration := metrics.measurementDuration(); duration <= 0 || duration > time.Second {
		t.Fatalf("measurement duration = %s", duration)
	}
}

func TestSoakOracleObserversDoNotBlock(t *testing.T) {
	oracle := &soakOracle{}
	oracle.mu.Lock()
	if err := oracle.commit(1); !errors.Is(err, api.ErrConcurrentOperation) {
		oracle.mu.Unlock()
		t.Fatalf("oracle commit while locked = %v, want ErrConcurrentOperation", err)
	}
	if err := oracle.delivery([]byte("key"), 0); !errors.Is(err, api.ErrConcurrentOperation) {
		oracle.mu.Unlock()
		t.Fatalf("oracle delivery while locked = %v, want ErrConcurrentOperation", err)
	}
	oracle.mu.Unlock()
}

func TestSoakOracleStoresPayloadDigest(t *testing.T) {
	key := []byte("key")
	value := []byte("payload")
	oracle := &soakOracle{pending: make(map[string]*soakExpected)}
	if err := oracle.offer(key, value); err != nil {
		t.Fatal(err)
	}
	pending := oracle.pending[string(key)]
	if pending == nil {
		t.Fatal("oracle expectation was not stored")
	}
	if pending.valueBytes != len(value) || pending.valueDigest != sha256.Sum256(value) {
		t.Fatalf("stored payload identity = (%d, %x)", pending.valueBytes, pending.valueDigest)
	}
}

func TestSoakOracleUpdateBoundsTracksSkippedByRetentionSeparatelyFromExpired(t *testing.T) {
	oracle := &soakOracle{pending: make(map[string]*soakExpected)}
	if err := oracle.offer([]byte("acked-and-purged"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := oracle.acknowledge([]byte("acked-and-purged"), []byte("v"), 2); err != nil {
		t.Fatal(err)
	}
	// Retention advances the log start to 5, past the oracle's unverified next
	// offset of 0. Offset 2 is both pending and acknowledged below the new log
	// start, so it counts as expired; offsets 0, 1, 3, and 4 were never even
	// fetched, so they only count as skipped, not expired.
	if err := oracle.updateBounds(5, 10); err != nil {
		t.Fatal(err)
	}
	if got := oracle.expiredCount(); got != 1 {
		t.Fatalf("expired = %d, want 1", got)
	}
	if got := oracle.skippedByRetentionCount(); got != 5 {
		t.Fatalf("skipped_by_retention = %d, want 5 (log start 5 - prior next 0)", got)
	}
	if got := oracle.nextOffset(); got != 5 {
		t.Fatalf("next = %d, want 5", got)
	}

	// A second advance with nothing pending still counts as skipped, not
	// expired, confirming the two counters track distinct events end to end.
	if err := oracle.updateBounds(9, 12); err != nil {
		t.Fatal(err)
	}
	if got := oracle.expiredCount(); got != 1 {
		t.Fatalf("expired after second advance = %d, want unchanged 1", got)
	}
	if got := oracle.skippedByRetentionCount(); got != 9 {
		t.Fatalf("skipped_by_retention after second advance = %d, want 9 (5 + (9 - 5))", got)
	}
}

func TestSoakOracleOnlyConsumedTopicsRetainDeliveryTiming(t *testing.T) {
	consumed := &soakOracle{consumed: true, pending: make(map[string]*soakExpected), deliveryTiming: make(map[uint64]soakTiming)}
	unconsumed := &soakOracle{consumed: false, pending: make(map[string]*soakExpected), deliveryTiming: make(map[uint64]soakTiming)}
	for _, oracle := range []*soakOracle{consumed, unconsumed} {
		if err := oracle.offer([]byte("key"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := oracle.acknowledge([]byte("key"), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	// Both oracles acknowledged a record that has not been observed by the
	// scanner yet, so acknowledge() stages a delivery timing entry for later.
	// A partition nothing ever polls has no delivery() call to drain it, so it
	// must never have been staged in the first place.
	if len(consumed.deliveryTiming) != 1 {
		t.Fatalf("consumed oracle deliveryTiming length = %d, want 1", len(consumed.deliveryTiming))
	}
	if len(unconsumed.deliveryTiming) != 0 {
		t.Fatalf("unconsumed oracle deliveryTiming length = %d, want 0 (never populated, so it cannot leak)", len(unconsumed.deliveryTiming))
	}
}

func TestSoakLatencyBucketsIncludeLongTails(t *testing.T) {
	var operations, nanos atomic.Uint64
	var buckets [soakLatencyBucketCount]atomic.Uint64
	recordSoakLatency(&operations, &nanos, &buckets, 20*time.Second)
	values := make([]uint64, len(buckets))
	for index := range buckets {
		values[index] = buckets[index].Load()
	}
	if got := latencyBucketQuantile(values, 0.99); got != 30_000_000_000 {
		t.Fatalf("20s bucket upper bound = %d, want 30s", got)
	}
}
