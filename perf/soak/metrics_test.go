package soak

import (
	"sync/atomic"
	"testing"
	"time"

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
