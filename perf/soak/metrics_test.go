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
