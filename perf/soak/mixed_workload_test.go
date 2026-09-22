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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

const (
	soakRetainedTopic      = "soak-retained"
	soakStableTopic        = "soak-stable"
	soakPartitionCount     = 2
	soakDefaultDuration    = 24 * time.Hour
	soakDefaultSeed        = uint64(0x5eed5eed)
	soakDefaultReopen      = 10 * time.Minute
	soakDefaultInterval    = 20 * time.Millisecond
	soakDefaultChurn       = 750 * time.Millisecond
	soakCheckpointName     = ".immulog-soak-checkpoint.json"
	soakProducerCount      = 2
	soakProducerWorkers    = soakPartitionCount * soakProducerCount * 2
	soakLatencyBucketCount = 12
	soakDefaultSampleEvery = 250 * time.Millisecond
	soakDefaultSampleLimit = 100_000
)

type soakCheckpoint struct {
	Version    uint32                            `json:"version"`
	Seed       uint64                            `json:"seed"`
	SeedSet    bool                              `json:"seed_set"`
	Run        uint64                            `json:"run"`
	Partitions map[string]soakCheckpointPosition `json:"partitions"`
}

type soakCheckpointPosition struct {
	Next uint64 `json:"next"`
}

type soakFixture struct {
	retained      storage.TopicDescriptor
	stable        storage.TopicDescriptor
	retainedParts []*storage.Partition
	stableParts   []*storage.Partition
}

type soakExpected struct {
	value                   []byte
	offeredAt               time.Time
	acknowledgedAt          time.Time
	observedAt              time.Time
	deliveredAt             time.Time
	acked                   bool
	observed                bool
	observedOffset          uint64
	deliveryLatencyRecorded bool
	offset                  uint64
}

type soakTiming struct {
	offeredAt      time.Time
	acknowledgedAt time.Time
}

type soakOracle struct {
	mu             sync.Mutex
	topic          api.TopicID
	partition      uint32
	seed           uint64
	run            uint64
	next           uint64
	lastL          uint64
	lastH          uint64
	pending        map[string]*soakExpected
	lastSequences  map[uint32]uint64
	deliveryTiming map[uint64]soakTiming
	digest         hash.Hash
	metrics        *soakMetrics
	verified       uint64
	expired        uint64
	committed      uint64
	err            error
}

type soakPartitionMetrics struct {
	deliveredRecords atomic.Uint64
	deliveredBytes   atomic.Uint64
	pollOps          atomic.Uint64
	pollRecords      atomic.Uint64
	emptyPolls       atomic.Uint64
	commitOps        atomic.Uint64
	commitRecords    atomic.Uint64
	assignmentLost   atomic.Uint64
	lagSamples       atomic.Uint64
	deliveryLagTotal atomic.Uint64
	commitLagTotal   atomic.Uint64
	maxDeliveryLag   atomic.Uint64
	maxCommitLag     atomic.Uint64
}

type soakSamplePartition struct {
	DurableEnd   uint64 `json:"durable_end"`
	NextDelivery uint64 `json:"next_delivery"`
	Committed    uint64 `json:"committed"`
	DeliveryLag  uint64 `json:"delivery_lag"`
	CommitLag    uint64 `json:"commit_lag"`
}

type soakResourceSample struct {
	Goroutines  uint64 `json:"goroutines"`
	OpenFiles   uint64 `json:"open_files"`
	HeapBytes   uint64 `json:"heap_bytes"`
	RSSBytes    uint64 `json:"rss_bytes"`
	GCCycles    uint64 `json:"gc_cycles"`
	ReadBytes   uint64 `json:"read_bytes"`
	WriteBytes  uint64 `json:"write_bytes"`
	UserTicks   uint64 `json:"user_ticks"`
	SystemTicks uint64 `json:"system_ticks"`
}

type soakSample struct {
	ElapsedNanos uint64                         `json:"elapsed_nanos"`
	Phase        string                         `json:"phase"`
	Offered      uint64                         `json:"offered"`
	Acknowledged uint64                         `json:"acknowledged"`
	Delivered    uint64                         `json:"delivered"`
	Committed    uint64                         `json:"committed"`
	Partitions   map[string]soakSamplePartition `json:"partitions"`
	Resources    soakResourceSample             `json:"resources"`
}

type soakPhaseReport struct {
	Name          string `json:"name"`
	DurationNanos uint64 `json:"duration_nanos"`
}

type soakConsumerProgress struct {
	lastPollStarted    [soakPartitionCount]atomic.Int64
	lastPollCompleted  [soakPartitionCount]atomic.Int64
	lastCommitStarted  [soakPartitionCount]atomic.Int64
	lastCommitFinished [soakPartitionCount]atomic.Int64
}

type soakMetrics struct {
	stableTopic            api.TopicID
	stableOffered          atomic.Uint64
	stableAcknowledged     atomic.Uint64
	warmupNanos            uint64
	partitions             [soakPartitionCount]soakPartitionMetrics
	measurementStarted     atomic.Int64
	measurementFinished    atomic.Int64
	measurementAccumulated atomic.Int64
	sampleIntervalNanos    int64
	sampleLimit            uint64
	sampleMu               sync.Mutex
	samples                []soakSample
	samplesDropped         uint64
	phaseMu                sync.Mutex
	phaseDurations         map[string]time.Duration
	schedulerLate          atomic.Uint64
	maxSchedulerLateness   atomic.Uint64
	consumerStatsSkipped   atomic.Uint64
	offered                atomic.Uint64
	acknowledged           atomic.Uint64
	unknown                atomic.Uint64
	knownRejected          atomic.Uint64
	cancelled              atomic.Uint64
	overloadCalls          atomic.Uint64
	acknowledgedBytes      atomic.Uint64
	deliveredBytes         atomic.Uint64
	maxGoroutines          atomic.Uint64
	maxOpenFiles           atomic.Uint64
	maxHeapBytes           atomic.Uint64
	maxRSSBytes            atomic.Uint64
	gcCycles               atomic.Uint64
	processReadBytes       atomic.Uint64
	processWriteBytes      atomic.Uint64
	processUserTicks       atomic.Uint64
	processSystemTicks     atomic.Uint64
	lagSamples             atomic.Uint64
	deliveryLagTotal       atomic.Uint64
	commitLagTotal         atomic.Uint64
	maxDeliveryLag         atomic.Uint64
	maxCommitLag           atomic.Uint64
	appendOps              atomic.Uint64
	appendNanos            atomic.Uint64
	appendBuckets          [soakLatencyBucketCount]atomic.Uint64
	pollOps                atomic.Uint64
	pollNanos              atomic.Uint64
	pollBuckets            [soakLatencyBucketCount]atomic.Uint64
	commitOps              atomic.Uint64
	commitNanos            atomic.Uint64
	commitBuckets          [soakLatencyBucketCount]atomic.Uint64
	verifyOps              atomic.Uint64
	verifyNanos            atomic.Uint64
	verifyBuckets          [soakLatencyBucketCount]atomic.Uint64
	offerToScanOps         atomic.Uint64
	offerToScanNanos       atomic.Uint64
	offerToScanBuckets     [soakLatencyBucketCount]atomic.Uint64
	ackToScanOps           atomic.Uint64
	ackToScanNanos         atomic.Uint64
	ackToScanBuckets       [soakLatencyBucketCount]atomic.Uint64
	offerToDeliveryOps     atomic.Uint64
	offerToDeliveryNanos   atomic.Uint64
	offerToDeliveryBuckets [soakLatencyBucketCount]atomic.Uint64
	ackToDeliveryOps       atomic.Uint64
	ackToDeliveryNanos     atomic.Uint64
	ackToDeliveryBuckets   [soakLatencyBucketCount]atomic.Uint64
}

type soakGroupHandle struct {
	mu       sync.Mutex
	consumer *storage.GroupConsumer
}

func TestMixedWorkloadSoak(t *testing.T) {
	if os.Getenv("IMMULOG_SOAK") != "1" {
		t.Skip("set IMMULOG_SOAK=1 to run the mixed-workload soak")
	}

	duration, err := soakDuration()
	if err != nil {
		t.Fatal(err)
	}
	warmup, err := soakWarmup()
	if err != nil {
		t.Fatal(err)
	}
	profile, err := soakProfile()
	if err != nil {
		t.Fatal(err)
	}
	reopenInterval, err := soakReopenInterval(duration)
	if err != nil {
		t.Fatal(err)
	}
	appendInterval, err := soakAppendInterval()
	if err != nil {
		t.Fatal(err)
	}
	churnInterval, err := soakChurnInterval()
	if err != nil {
		t.Fatal(err)
	}
	sampleInterval, err := soakSampleInterval()
	if err != nil {
		t.Fatal(err)
	}
	sampleLimit, err := soakSampleLimit()
	if err != nil {
		t.Fatal(err)
	}
	producerRate, err := soakProducerRate()
	if err != nil {
		t.Fatal(err)
	}
	overloadPeriod, overloadWindow := time.Minute, 5*time.Second
	stressCancellation := true
	if profile == "sustained" {
		overloadPeriod, overloadWindow = duration+time.Minute, 0
		appendInterval = 0
		stressCancellation = false
	}
	if duration <= 30*time.Second && profile == "mixed" {
		overloadPeriod, overloadWindow = 5*time.Second, time.Second
	}
	dir, cleanup := soakDirectory(t)
	defer cleanup()

	checkpoint, err := loadSoakCheckpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := soakSeed(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Run == ^uint64(0) {
		t.Fatal("soak run counter is exhausted")
	}
	checkpoint.Version = 1
	checkpoint.Seed = seed
	checkpoint.SeedSet = true
	checkpoint.Run++
	if checkpoint.Partitions == nil {
		checkpoint.Partitions = make(map[string]soakCheckpointPosition)
	}

	store, fixture, err := openSoakStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	metrics := newSoakMetrics(sampleInterval, sampleLimit)
	metrics.stableTopic = fixture.stable.ID
	metrics.warmupNanos = uint64(warmup)
	var sequenceCounter atomic.Uint64
	oracles, err := newSoakOracles(fixture, checkpoint, seed, checkpoint.Run)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	for _, oracle := range oracles {
		oracle.metrics = metrics
	}
	if warmup > 0 {
		warmupMetrics := metrics
		warmupMetrics.startMeasurement(time.Now())
		warmupContext, warmupCancel := context.WithTimeout(context.Background(), warmup)
		warmupErr := runSoakCycle(warmupContext, store, fixture, oracles, warmupMetrics, checkpoint.Run, seed, &sequenceCounter, appendInterval, producerRate, churnInterval, reopenInterval, overloadPeriod, overloadWindow, stressCancellation)
		warmupCancel()
		if warmupErr != nil {
			t.Fatal(warmupErr)
		}
		store, fixture, err = openSoakStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		metrics = newSoakMetrics(sampleInterval, sampleLimit)
		metrics.stableTopic = fixture.stable.ID
		metrics.warmupNanos = uint64(warmup)
		oracles, err = newSoakOracles(fixture, checkpoint, seed, checkpoint.Run)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		for _, oracle := range oracles {
			oracle.metrics = metrics
		}
	}
	if err := writeSoakCheckpoint(dir, checkpoint); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}

	started := time.Now()
	metrics.startMeasurement(started)
	metricsFile := os.Getenv("IMMULOG_SOAK_METRICS_FILE")
	metricsWritten := false
	var failure error
	defer func() {
		if metricsFile != "" && !metricsWritten {
			metrics.finishMeasurement(time.Now())
			if err := writeSoakMetrics(metricsFile, metrics, oracles, time.Since(started), false, failure); err != nil {
				t.Logf("write partial soak metrics: %v", err)
			}
		}
	}()
	soakContext, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	for {
		if err := runSoakCycle(soakContext, store, fixture, oracles, metrics, checkpoint.Run, seed, &sequenceCounter, appendInterval, producerRate, churnInterval, reopenInterval, overloadPeriod, overloadWindow, stressCancellation); err != nil {
			failure = err
			t.Fatal(err)
		}
		for key, oracle := range oracles {
			checkpoint.Partitions[key] = soakCheckpointPosition{Next: oracle.nextOffset()}
		}
		if err := writeSoakCheckpoint(dir, checkpoint); err != nil {
			t.Fatal(err)
		}
		if soakContext.Err() != nil {
			break
		}
		store, fixture, err = openSoakStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(started) >= duration {
			break
		}
	}

	metrics.finishMeasurement(time.Now())
	for key, oracle := range oracles {
		checkpoint.Partitions[key] = soakCheckpointPosition{Next: oracle.nextOffset()}
	}
	if err := writeSoakCheckpoint(dir, checkpoint); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("mixed soak completed profile=%s duration=%s run=%d seed=0x%x %s", profile, duration, checkpoint.Run, seed, metrics.summary())
	for key, oracle := range oracles {
		t.Logf("oracle %s verified=%d expired=%d next=%d digest=%s", key, oracle.verifiedCount(), oracle.expiredCount(), oracle.nextOffset(), oracle.digestHex())
	}
	if metricsFile != "" {
		if err := writeSoakMetrics(metricsFile, metrics, oracles, time.Since(started), true, nil); err != nil {
			t.Fatal(err)
		}
		metricsWritten = true
	}
}

type soakLatencyReport struct {
	Operations      uint64   `json:"operations"`
	TotalNanos      uint64   `json:"total_nanos"`
	AverageNanos    uint64   `json:"average_nanos"`
	P50BucketNanos  uint64   `json:"p50_bucket_nanos"`
	P90BucketNanos  uint64   `json:"p90_bucket_nanos"`
	P95BucketNanos  uint64   `json:"p95_bucket_nanos"`
	P99BucketNanos  uint64   `json:"p99_bucket_nanos"`
	P999BucketNanos uint64   `json:"p999_bucket_nanos"`
	Buckets         []uint64 `json:"buckets"`
}

type soakOracleReport struct {
	Verified uint64 `json:"verified"`
	Expired  uint64 `json:"expired"`
	Next     uint64 `json:"next"`
	Digest   string `json:"digest"`
}

type soakPartitionReport struct {
	DeliveredRecords      uint64  `json:"delivered_records"`
	DeliveredPayloadBytes uint64  `json:"delivered_payload_bytes"`
	DeliveryRate          float64 `json:"delivery_rate_records_per_second"`
	DeliveryByteRate      float64 `json:"delivery_rate_payload_bytes_per_second"`
	Polls                 uint64  `json:"polls"`
	PolledRecords         uint64  `json:"polled_records"`
	AveragePollBatch      float64 `json:"average_poll_batch"`
	EmptyPolls            uint64  `json:"empty_polls"`
	Commits               uint64  `json:"commits"`
	CommittedRecords      uint64  `json:"committed_records"`
	AverageCommitBatch    float64 `json:"average_commit_batch"`
	AssignmentLost        uint64  `json:"assignment_lost"`
	LagSamples            uint64  `json:"lag_samples"`
	AverageDeliveryLag    uint64  `json:"average_delivery_lag"`
	MaxDeliveryLag        uint64  `json:"max_delivery_lag"`
	AverageCommitLag      uint64  `json:"average_commit_lag"`
	MaxCommitLag          uint64  `json:"max_commit_lag"`
}

type soakMetricsReport struct {
	Version              uint32                         `json:"version"`
	Completed            bool                           `json:"completed"`
	Failure              string                         `json:"failure,omitempty"`
	WarmupNanos          uint64                         `json:"warmup_nanos"`
	MeasurementNanos     uint64                         `json:"measurement_nanos"`
	TotalNanos           uint64                         `json:"total_nanos"`
	Phases               []soakPhaseReport              `json:"phases"`
	SampleIntervalNanos  uint64                         `json:"sample_interval_nanos"`
	SamplesDropped       uint64                         `json:"samples_dropped"`
	SchedulerLate        uint64                         `json:"scheduler_late"`
	MaxSchedulerLateness uint64                         `json:"max_scheduler_lateness_nanos"`
	ConsumerStatsSkipped uint64                         `json:"consumer_stats_skipped"`
	Samples              []soakSample                   `json:"samples"`
	Profile              string                         `json:"profile"`
	Offered              uint64                         `json:"offered"`
	Acknowledged         uint64                         `json:"acknowledged"`
	Unknown              uint64                         `json:"unknown"`
	KnownRejected        uint64                         `json:"known_rejected"`
	Cancelled            uint64                         `json:"cancelled"`
	OverloadCalls        uint64                         `json:"overload_calls"`
	AcknowledgedBytes    uint64                         `json:"acknowledged_bytes"`
	DeliveredBytes       uint64                         `json:"delivered_payload_bytes"`
	MaxGoroutines        uint64                         `json:"max_goroutines"`
	MaxOpenFiles         uint64                         `json:"max_open_files"`
	MaxHeapBytes         uint64                         `json:"max_heap_bytes"`
	MaxRSSBytes          uint64                         `json:"max_rss_bytes"`
	GCycles              uint64                         `json:"gc_cycles"`
	ProcessReadBytes     uint64                         `json:"process_read_bytes"`
	ProcessWriteBytes    uint64                         `json:"process_write_bytes"`
	ProcessUserTicks     uint64                         `json:"process_user_ticks"`
	ProcessSystemTicks   uint64                         `json:"process_system_ticks"`
	LagSamples           uint64                         `json:"lag_samples"`
	AverageDeliveryLag   uint64                         `json:"average_delivery_lag"`
	MaxDeliveryLag       uint64                         `json:"max_delivery_lag"`
	AverageCommitLag     uint64                         `json:"average_commit_lag"`
	MaxCommitLag         uint64                         `json:"max_commit_lag"`
	Latency              map[string]soakLatencyReport   `json:"latency"`
	Partitions           map[string]soakPartitionReport `json:"partitions"`
	Oracles              map[string]soakOracleReport    `json:"oracles"`
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeSoakMetrics(path string, metrics *soakMetrics, oracles map[string]*soakOracle, elapsed time.Duration, completed bool, failure error) error {
	lagSamples := metrics.lagSamples.Load()
	averageDeliveryLag, averageCommitLag := uint64(0), uint64(0)
	if lagSamples != 0 {
		averageDeliveryLag = metrics.deliveryLagTotal.Load() / lagSamples
		averageCommitLag = metrics.commitLagTotal.Load() / lagSamples
	}
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	measurement := metrics.measurementDuration()
	if measurement <= 0 {
		measurement = elapsed
	}
	samples, samplesDropped := metrics.sampleSnapshot()
	drain := metrics.phaseDuration("drain")
	verify := metrics.phaseDuration("verify")
	cleanup := elapsed - measurement - drain - verify
	if cleanup < 0 {
		cleanup = 0
	}
	report := soakMetricsReport{
		Version:              2,
		Completed:            completed,
		Failure:              errorString(failure),
		WarmupNanos:          metrics.warmupNanos,
		MeasurementNanos:     uint64(measurement),
		TotalNanos:           uint64(elapsed),
		Phases:               []soakPhaseReport{{Name: "measure", DurationNanos: uint64(measurement)}, {Name: "drain", DurationNanos: uint64(drain)}, {Name: "verify", DurationNanos: uint64(verify)}, {Name: "cleanup", DurationNanos: uint64(cleanup)}},
		SampleIntervalNanos:  uint64(metrics.sampleIntervalNanos),
		SamplesDropped:       samplesDropped,
		SchedulerLate:        metrics.schedulerLate.Load(),
		MaxSchedulerLateness: metrics.maxSchedulerLateness.Load(),
		ConsumerStatsSkipped: metrics.consumerStatsSkipped.Load(),
		Samples:              samples,
		Profile:              os.Getenv("IMMULOG_SOAK_PROFILE"),
		Offered:              metrics.offered.Load(),
		Acknowledged:         metrics.acknowledged.Load(),
		Unknown:              metrics.unknown.Load(),
		KnownRejected:        metrics.knownRejected.Load(),
		Cancelled:            metrics.cancelled.Load(),
		OverloadCalls:        metrics.overloadCalls.Load(),
		AcknowledgedBytes:    metrics.acknowledgedBytes.Load(),
		DeliveredBytes:       metrics.deliveredBytes.Load(),
		MaxGoroutines:        metrics.maxGoroutines.Load(),
		MaxOpenFiles:         metrics.maxOpenFiles.Load(),
		MaxHeapBytes:         metrics.maxHeapBytes.Load(),
		MaxRSSBytes:          metrics.maxRSSBytes.Load(),
		GCycles:              metrics.gcCycles.Load(),
		ProcessReadBytes:     metrics.processReadBytes.Load(),
		ProcessWriteBytes:    metrics.processWriteBytes.Load(),
		ProcessUserTicks:     metrics.processUserTicks.Load(),
		ProcessSystemTicks:   metrics.processSystemTicks.Load(),
		LagSamples:           lagSamples,
		AverageDeliveryLag:   averageDeliveryLag,
		MaxDeliveryLag:       metrics.maxDeliveryLag.Load(),
		AverageCommitLag:     averageCommitLag,
		MaxCommitLag:         metrics.maxCommitLag.Load(),
		Latency:              make(map[string]soakLatencyReport),
		Partitions:           make(map[string]soakPartitionReport, soakPartitionCount),
		Oracles:              make(map[string]soakOracleReport, len(oracles)),
	}
	latencies := []struct {
		name       string
		operations *atomic.Uint64
		nanos      *atomic.Uint64
		buckets    *[soakLatencyBucketCount]atomic.Uint64
	}{
		{"append", &metrics.appendOps, &metrics.appendNanos, &metrics.appendBuckets},
		{"poll", &metrics.pollOps, &metrics.pollNanos, &metrics.pollBuckets},
		{"commit", &metrics.commitOps, &metrics.commitNanos, &metrics.commitBuckets},
		{"verify", &metrics.verifyOps, &metrics.verifyNanos, &metrics.verifyBuckets},
		{"offer_to_scan", &metrics.offerToScanOps, &metrics.offerToScanNanos, &metrics.offerToScanBuckets},
		{"ack_to_scan", &metrics.ackToScanOps, &metrics.ackToScanNanos, &metrics.ackToScanBuckets},
		{"offer_to_delivery", &metrics.offerToDeliveryOps, &metrics.offerToDeliveryNanos, &metrics.offerToDeliveryBuckets},
		{"ack_to_delivery", &metrics.ackToDeliveryOps, &metrics.ackToDeliveryNanos, &metrics.ackToDeliveryBuckets},
	}
	for _, latency := range latencies {
		values := make([]uint64, len(latency.buckets))
		for index := range latency.buckets {
			values[index] = latency.buckets[index].Load()
		}
		latencyReport := soakLatencyReport{
			Operations:      latency.operations.Load(),
			TotalNanos:      latency.nanos.Load(),
			AverageNanos:    averageLatencyNanos(latency.operations.Load(), latency.nanos.Load()),
			P50BucketNanos:  latencyBucketQuantile(values, 0.50),
			P90BucketNanos:  latencyBucketQuantile(values, 0.90),
			P95BucketNanos:  latencyBucketQuantile(values, 0.95),
			P99BucketNanos:  latencyBucketQuantile(values, 0.99),
			P999BucketNanos: latencyBucketQuantile(values, 0.999),
			Buckets:         values,
		}
		report.Latency[latency.name] = latencyReport
	}
	for partition := range metrics.partitions {
		report.Partitions[fmt.Sprintf("%s/%d", soakStableTopic, partition)] = metrics.partitionReport(partition, measurement)
	}
	for key, oracle := range oracles {
		report.Oracles[key] = soakOracleReport{
			Verified: oracle.verifiedCount(),
			Expired:  oracle.expiredCount(),
			Next:     oracle.nextOffset(),
			Digest:   oracle.digestHex(),
		}
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode soak metrics: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create soak metrics directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write soak metrics: %w", err)
	}
	return nil
}

func soakProfile() (string, error) {
	profile := os.Getenv("IMMULOG_SOAK_PROFILE")
	if profile == "" {
		return "mixed", nil
	}
	if profile != "mixed" && profile != "sustained" {
		return "", fmt.Errorf("IMMULOG_SOAK_PROFILE must be mixed or sustained, got %q", profile)
	}
	return profile, nil
}

func soakDuration() (time.Duration, error) {
	value := os.Getenv("IMMULOG_SOAK_DURATION")
	if value == "" {
		return soakDefaultDuration, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 || duration > soakDefaultDuration {
		return 0, fmt.Errorf("IMMULOG_SOAK_DURATION must be in (0,%s], got %q", soakDefaultDuration, value)
	}
	return duration, nil
}

func soakWarmup() (time.Duration, error) {
	value := os.Getenv("IMMULOG_SOAK_WARMUP")
	if value == "" {
		return 0, nil
	}
	warmup, err := time.ParseDuration(value)
	if err != nil || warmup < 0 {
		return 0, fmt.Errorf("IMMULOG_SOAK_WARMUP must be nonnegative, got %q", value)
	}
	return warmup, nil
}

func soakReopenInterval(duration time.Duration) (time.Duration, error) {
	value := os.Getenv("IMMULOG_SOAK_REOPEN_INTERVAL")
	if value == "" {
		if duration <= 30*time.Second {
			return maxDuration(time.Millisecond, duration/2), nil
		}
		return soakDefaultReopen, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		return 0, fmt.Errorf("IMMULOG_SOAK_REOPEN_INTERVAL must be positive, got %q", value)
	}
	return interval, nil
}

func soakAppendInterval() (time.Duration, error) {
	value := os.Getenv("IMMULOG_SOAK_APPEND_INTERVAL")
	if value == "" {
		return soakDefaultInterval, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval < 0 {
		return 0, fmt.Errorf("IMMULOG_SOAK_APPEND_INTERVAL must be nonnegative, got %q", value)
	}
	return interval, nil
}

func soakSampleInterval() (time.Duration, error) {
	value := os.Getenv("IMMULOG_SOAK_SAMPLE_INTERVAL")
	if value == "" {
		return soakDefaultSampleEvery, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		return 0, fmt.Errorf("IMMULOG_SOAK_SAMPLE_INTERVAL must be positive, got %q", value)
	}
	return interval, nil
}

func soakSampleLimit() (uint64, error) {
	value := os.Getenv("IMMULOG_SOAK_SAMPLE_LIMIT")
	if value == "" {
		return soakDefaultSampleLimit, nil
	}
	limit, err := strconv.ParseUint(value, 10, 64)
	if err != nil || limit == 0 {
		return 0, fmt.Errorf("IMMULOG_SOAK_SAMPLE_LIMIT must be positive, got %q", value)
	}
	return limit, nil
}

func soakProducerRate() (float64, error) {
	value := os.Getenv("IMMULOG_SOAK_PRODUCER_RATE")
	if value == "" {
		return 0, nil
	}
	rate, err := strconv.ParseFloat(value, 64)
	if err != nil || rate < 0 {
		return 0, fmt.Errorf("IMMULOG_SOAK_PRODUCER_RATE must be nonnegative, got %q", value)
	}
	return rate, nil
}

func soakChurnInterval() (time.Duration, error) {
	value := os.Getenv("IMMULOG_SOAK_CHURN_INTERVAL")
	if value == "" {
		return soakDefaultChurn, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval < 0 {
		return 0, fmt.Errorf("IMMULOG_SOAK_CHURN_INTERVAL must be nonnegative, got %q", value)
	}
	return interval, nil
}

func soakDirectory(t *testing.T) (string, func()) {
	if value := os.Getenv("IMMULOG_SOAK_DIR"); value != "" {
		return value, func() {}
	}
	dir := t.TempDir()
	return dir, func() {}
}

func loadSoakCheckpoint(dir string) (soakCheckpoint, error) {
	data, err := os.ReadFile(filepath.Join(dir, soakCheckpointName))
	if errors.Is(err, os.ErrNotExist) {
		return soakCheckpoint{Version: 1, Partitions: make(map[string]soakCheckpointPosition)}, nil
	}
	if err != nil {
		return soakCheckpoint{}, fmt.Errorf("read soak checkpoint: %w", err)
	}
	var checkpoint soakCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return soakCheckpoint{}, fmt.Errorf("decode soak checkpoint: %w", err)
	}
	if checkpoint.Version != 1 || checkpoint.Partitions == nil {
		return soakCheckpoint{}, errors.New("soak checkpoint has an unsupported version")
	}
	return checkpoint, nil
}

func writeSoakCheckpoint(dir string, checkpoint soakCheckpoint) error {
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("encode soak checkpoint: %w", err)
	}
	temporary := filepath.Join(dir, soakCheckpointName+".tmp")
	path := filepath.Join(dir, soakCheckpointName)
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return fmt.Errorf("write soak checkpoint: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("publish soak checkpoint: %w", err)
	}
	return nil
}

func soakSeed(checkpoint soakCheckpoint) (uint64, error) {
	value := os.Getenv("IMMULOG_SOAK_SEED")
	if value == "" {
		if checkpoint.SeedSet {
			return checkpoint.Seed, nil
		}
		return soakDefaultSeed, nil
	}
	seed, err := strconv.ParseUint(value, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("IMMULOG_SOAK_SEED must be an unsigned integer, got %q", value)
	}
	if checkpoint.SeedSet && checkpoint.Seed != seed {
		return 0, fmt.Errorf("soak checkpoint seed 0x%x differs from requested seed 0x%x", checkpoint.Seed, seed)
	}
	return seed, nil
}

func openSoakStore(dir string) (*storage.Store, soakFixture, error) {
	options := storage.StoreOptions{
		TailBytes:               512 << 10,
		MaxTopics:               8,
		MaxUserPartitions:       8,
		MaxOpenPartitions:       8,
		MaxCatalogHistoryBytes:  1 << 30,
		MaxOffsetsHistoryBytes:  1 << 30,
		MaxConsumerGroups:       4,
		MaxConsumerProgressKeys: 4,
	}
	store, err := storage.OpenWithOptions(dir, options)
	if err != nil {
		return nil, soakFixture{}, err
	}
	retainedOptions := soakPartitionOptions()
	retainedOptions.RetentionTimeEnabled = true
	retainedOptions.RetentionSizeEnabled = true
	retainedOptions.RetentionDuration = 0
	retainedOptions.RetentionBytes = 1 << 20
	retainedOptions.MaxSegmentAge = 250 * time.Millisecond
	retainedOptions.RetentionCheck = 50 * time.Millisecond
	stableOptions := soakPartitionOptions()
	retained, err := describeOrCreateSoakTopic(store, soakRetainedTopic, retainedOptions)
	if err != nil {
		_ = store.Close()
		return nil, soakFixture{}, err
	}
	stable, err := describeOrCreateSoakTopic(store, soakStableTopic, stableOptions)
	if err != nil {
		_ = store.Close()
		return nil, soakFixture{}, err
	}
	fixture := soakFixture{retained: retained, stable: stable}
	for partition := uint32(0); partition < soakPartitionCount; partition++ {
		opened, err := store.OpenPartition(retained.ID, partition, soakPartitionOptions())
		if err != nil {
			_ = store.Close()
			return nil, soakFixture{}, err
		}
		fixture.retainedParts = append(fixture.retainedParts, opened)
		opened, err = store.OpenPartition(stable.ID, partition, soakPartitionOptions())
		if err != nil {
			_ = store.Close()
			return nil, soakFixture{}, err
		}
		fixture.stableParts = append(fixture.stableParts, opened)
	}
	return store, fixture, nil
}

func soakPartitionOptions() storage.PartitionOptions {
	return storage.PartitionOptions{
		SegmentBytes:     512 << 10,
		BatchBytes:       128 << 10,
		BatchRecords:     8,
		RecordBytes:      96 << 10,
		InFlightBytes:    256 << 10,
		InFlightRecords:  16,
		AdmissionWaiters: 32,
		BatchLinger:      time.Millisecond,
		TailSlots:        8,
		TailBytes:        128 << 10,
	}
}

func describeOrCreateSoakTopic(store *storage.Store, name string, options storage.PartitionOptions) (storage.TopicDescriptor, error) {
	descriptor, err := store.DescribeTopic(name)
	if err == nil {
		if len(descriptor.Partitions) != soakPartitionCount {
			return storage.TopicDescriptor{}, fmt.Errorf("soak topic %q has %d partitions, want %d", name, len(descriptor.Partitions), soakPartitionCount)
		}
		return descriptor, nil
	}
	if !errors.Is(err, api.ErrUnknownTopic) {
		return storage.TopicDescriptor{}, err
	}
	return store.CreateTopic(name, soakPartitionCount, options)
}

func newSoakOracles(fixture soakFixture, checkpoint soakCheckpoint, seed, run uint64) (map[string]*soakOracle, error) {
	oracles := make(map[string]*soakOracle, soakPartitionCount*2)
	for partition := uint32(0); partition < soakPartitionCount; partition++ {
		for _, topic := range []struct {
			name string
			part *storage.Partition
			id   api.TopicID
		}{{soakRetainedTopic, fixture.retainedParts[partition], fixture.retained.ID}, {soakStableTopic, fixture.stableParts[partition], fixture.stable.ID}} {
			stats := topic.part.Stats()
			key := soakPartitionKey(topic.name, partition)
			position := checkpoint.Partitions[key]
			if position.Next > stats.DurableEnd {
				return nil, fmt.Errorf("checkpoint %s next offset %d exceeds durable end %d", key, position.Next, stats.DurableEnd)
			}
			next := position.Next
			if next < stats.LogStartOffset {
				next = stats.LogStartOffset
			}
			oracles[key] = &soakOracle{
				topic: topic.id, partition: partition, seed: seed, run: run, next: next,
				lastL: stats.LogStartOffset, lastH: stats.DurableEnd,
				pending: make(map[string]*soakExpected), lastSequences: make(map[uint32]uint64), deliveryTiming: make(map[uint64]soakTiming), digest: sha256.New(),
			}
		}
	}
	return oracles, nil
}

func runSoakCycle(parent context.Context, store *storage.Store, fixture soakFixture, oracles map[string]*soakOracle, metrics *soakMetrics, run, seed uint64, sequenceCounter *atomic.Uint64, appendInterval time.Duration, producerRate float64, churnInterval, reopenInterval, overloadPeriod, overloadWindow time.Duration, stressCancellation bool) error {
	members := soakGroupMembers(fixture.stable.ID, false)
	consumer, err := store.OpenConsumerGroup(parent, "soak-workers", members, soakGroupOptions())
	if err != nil {
		return fmt.Errorf("open soak consumer group: %w", err)
	}
	handle := &soakGroupHandle{consumer: consumer}
	cycleContext, cancel := context.WithCancel(parent)
	var workerWait sync.WaitGroup
	var firstErr error
	var errorOnce sync.Once
	report := func(err error) {
		if err == nil {
			return
		}
		errorOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}
	cycleStart := time.Now()
	for partition := uint32(0); partition < soakPartitionCount; partition++ {
		for producer := uint32(0); producer < soakProducerCount; producer++ {
			workerWait.Add(1)
			go func(partition, producer uint32) {
				defer workerWait.Done()
				producerLoop(cycleContext, cycleStart, fixture.retainedParts[partition], seed, run, partition*soakProducerCount+producer, sequenceCounter, appendInterval, producerRate, overloadPeriod, overloadWindow, stressCancellation, metrics, oracles[soakPartitionKey(soakRetainedTopic, partition)], report)
			}(partition, producer)
			workerWait.Add(1)
			go func(partition, producer uint32) {
				defer workerWait.Done()
				producerLoop(cycleContext, cycleStart, fixture.stableParts[partition], seed, run, 100+partition*soakProducerCount+producer, sequenceCounter, appendInterval, producerRate, overloadPeriod, overloadWindow, stressCancellation, metrics, oracles[soakPartitionKey(soakStableTopic, partition)], report)
			}(partition, producer)
		}
	}
	for partition := uint32(0); partition < soakPartitionCount; partition++ {
		for _, topic := range []struct {
			name string
			part *storage.Partition
		}{
			{soakRetainedTopic, fixture.retainedParts[partition]},
			{soakStableTopic, fixture.stableParts[partition]},
		} {
			oracle := oracles[soakPartitionKey(topic.name, partition)]
			workerWait.Add(1)
			go func(part *storage.Partition, oracle *soakOracle) {
				defer workerWait.Done()
				scannerLoop(cycleContext, part, oracle, metrics, report)
			}(topic.part, oracle)
		}
	}
	workerWait.Add(1)
	go func() {
		defer workerWait.Done()
		groupLoop(cycleContext, handle, fixture.stable.ID, fixture.stableParts, oracles, metrics, report)
	}()
	if churnInterval != 0 {
		workerWait.Add(1)
		go func() {
			defer workerWait.Done()
			churnLoop(cycleContext, store, handle, fixture.stable.ID, churnInterval, report)
		}()
	}
	workerWait.Add(1)
	go func() {
		defer workerWait.Done()
		maintenanceLoop(cycleContext, store, report)
	}()
	workerWait.Add(1)
	go func() {
		defer workerWait.Done()
		statsLoop(cycleContext, store, handle, fixture.stable.ID, metrics, report)
	}()

	timer := time.NewTimer(reopenInterval)
	select {
	case <-timer.C:
	case <-parent.Done():
		metrics.finishMeasurement(time.Now())
	case <-cycleContext.Done():
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	metrics.addMeasurement(time.Since(cycleStart))
	cancel()
	workerWait.Wait()
	drainStarted := time.Now()
	current := handle.currentConsumer()
	if current != nil {
		if closeErr := current.Close(); closeErr != nil && firstErr == nil {
			firstErr = fmt.Errorf("close soak consumer group: %w", closeErr)
		}
	}
	if firstErr == nil {
		drainContext, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
		firstErr = waitForSoakAdmissionDrain(drainContext, fixture)
		drainCancel()
	}
	metrics.recordPhase("drain", time.Since(drainStarted))
	verifyStarted := time.Now()
	if firstErr == nil {
		if retentionErr := store.RunRetention(context.Background()); retentionErr != nil {
			firstErr = fmt.Errorf("force final soak retention pass: %w", retentionErr)
		}
	}
	if firstErr == nil {
		drainContext, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
		firstErr = verifySoakOracles(drainContext, fixture, oracles)
		drainCancel()
	}
	if firstErr == nil {
		firstErr = checkSoakAdmissionDrained(fixture)
	}
	metrics.recordPhase("verify", time.Since(verifyStarted))
	closeErr := store.Close()
	if closeErr != nil && firstErr == nil {
		firstErr = fmt.Errorf("close soak store: %w", closeErr)
	}
	return firstErr
}

func producerLoop(ctx context.Context, started time.Time, partition *storage.Partition, seed, run uint64, producer uint32, sequenceCounter *atomic.Uint64, appendInterval time.Duration, producerRate float64, overloadPeriod, overloadWindow time.Duration, stressCancellation bool, metrics *soakMetrics, oracle *soakOracle, report func(error)) {
	random := rand.New(rand.NewSource(int64(seed + uint64(producer))))
	var nextDeadline time.Time
	var targetInterval time.Duration
	if producerRate > 0 {
		targetInterval = time.Duration(float64(time.Second) * float64(soakProducerWorkers) / producerRate)
		if targetInterval < time.Nanosecond {
			targetInterval = time.Nanosecond
		}
		nextDeadline = time.Now()
	}
	for {
		if ctx.Err() != nil {
			return
		}
		sequence := sequenceCounter.Add(1) - 1
		key := soakRecordKey(seed, run, producer, sequence)
		size, empty := soakPayloadSize(sequence, random)
		value := soakRecordValue(key, size, empty)
		if err := oracle.offer(key, value); err != nil {
			report(err)
			return
		}
		metrics.offered.Add(1)
		if oracle.topic == metrics.stableTopic {
			metrics.stableOffered.Add(1)
		}
		elapsed := time.Since(started) % overloadPeriod
		overload := elapsed >= overloadPeriod-overloadWindow
		callContext := ctx
		var cancel context.CancelFunc
		if stressCancellation && (overload || sequence%19 == 0) {
			callContext, cancel = context.WithTimeout(ctx, 250*time.Microsecond)
		}
		callStarted := time.Now()
		record, err := partition.Append(callContext, api.AppendRequest{Topic: oracle.topic, Partition: oracle.partition, Key: key, Value: value})
		if cancel != nil {
			cancel()
		}
		recordSoakLatency(&metrics.appendOps, &metrics.appendNanos, &metrics.appendBuckets, time.Since(callStarted))
		if err == nil {
			if !bytes.Equal(record.Key, key) || !bytes.Equal(record.Value, value) || record.Topic != oracle.topic || record.Partition != oracle.partition {
				report(fmt.Errorf("append result does not preserve request identity on %s/%d", oracle.topic, oracle.partition))
				return
			}
			if appendErr := oracle.acknowledge(key, value, record.Offset); appendErr != nil {
				report(appendErr)
				return
			}
			metrics.acknowledged.Add(1)
			metrics.acknowledgedBytes.Add(uint64(len(value)))
			if oracle.topic == metrics.stableTopic {
				metrics.stableAcknowledged.Add(1)
			}
		} else {
			if appendErr := oracle.reject(key); appendErr != nil {
				report(appendErr)
				return
			}
			if errors.Is(err, api.ErrAppendOutcomeUnknown) {
				metrics.unknown.Add(1)
			} else {
				metrics.knownRejected.Add(1)
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				metrics.cancelled.Add(1)
			}
			if !isExpectedSoakAppendError(err) {
				report(fmt.Errorf("unexpected soak append error: %w", err))
				return
			}
		}
		if overload {
			metrics.overloadCalls.Add(1)
		} else if producerRate > 0 {
			nextDeadline = nextDeadline.Add(targetInterval)
			now := time.Now()
			if now.Before(nextDeadline) {
				timer := time.NewTimer(nextDeadline.Sub(now))
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			} else {
				metrics.recordSchedulerLate(now.Sub(nextDeadline))
				nextDeadline = now
			}
		} else if appendInterval != 0 {
			timer := time.NewTimer(appendInterval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}
}

func soakPayloadSize(sequence uint64, random *rand.Rand) (int, bool) {
	switch sequence % 64 {
	case 0:
		return 64 << 10, false
	case 1:
		return 0, false
	case 2:
		return 0, true
	default:
		if random.Intn(8) == 0 {
			return 1024 + random.Intn(128), false
		}
		return 1024, false
	}
}

func soakRecordKey(seed, run uint64, producer uint32, sequence uint64) []byte {
	return []byte(fmt.Sprintf("soak-%016x-%016x-%08x-%016x", seed, run, producer, sequence))
}

func soakRecordValue(key []byte, size int, empty bool) []byte {
	if size == 0 {
		if empty {
			return []byte{}
		}
		return nil
	}
	value := make([]byte, size)
	copy(value, key)
	for index := len(key); index < len(value); index++ {
		value[index] = byte(index)
	}
	return value
}

func isExpectedSoakAppendError(err error) bool {
	return errors.Is(err, api.ErrBackpressure) || errors.Is(err, api.ErrDiskPressure) || errors.Is(err, api.ErrResourceLimit) || errors.Is(err, api.ErrClosing) || errors.Is(err, api.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func scannerLoop(ctx context.Context, partition *storage.Partition, oracle *soakOracle, metrics *soakMetrics, report func(error)) {
	for {
		if ctx.Err() != nil {
			return
		}
		verifyStarted := time.Now()
		err := oracle.verify(ctx, partition)
		recordSoakLatency(&metrics.verifyOps, &metrics.verifyNanos, &metrics.verifyBuckets, time.Since(verifyStarted))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			report(err)
			return
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (progress *soakConsumerProgress) diagnostic(partition uint32) string {
	index := int(partition)
	return fmt.Sprintf("last_poll_started_ago=%s last_poll_completed_ago=%s last_commit_started_ago=%s last_commit_finished_ago=%s", elapsedSince(progress.lastPollStarted[index].Load()), elapsedSince(progress.lastPollCompleted[index].Load()), elapsedSince(progress.lastCommitStarted[index].Load()), elapsedSince(progress.lastCommitFinished[index].Load()))
}

func elapsedSince(timestamp int64) string {
	if timestamp == 0 {
		return "never"
	}
	return time.Since(time.Unix(0, timestamp)).Round(time.Millisecond).String()
}

func groupLoop(ctx context.Context, handle *soakGroupHandle, topic api.TopicID, partitions []*storage.Partition, oracles map[string]*soakOracle, metrics *soakMetrics, report func(error)) {
	var progress soakConsumerProgress
	for {
		if ctx.Err() != nil {
			return
		}
		consumer := handle.currentConsumer()
		if consumer == nil {
			return
		}
		for partition := uint32(0); partition < uint32(len(partitions)); partition++ {
			partitionMetrics := &metrics.partitions[partition]
			partitionMetrics.pollOps.Add(1)
			progress.lastPollStarted[partition].Store(time.Now().UnixNano())
			pollContext, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
			pollStarted := time.Now()
			result, err := consumer.Poll(pollContext, topic, partition, api.FetchOptions{MaxRecords: 16, MaxBytes: 128 << 10, MaxWait: 5 * time.Millisecond})
			recordSoakLatency(&metrics.pollOps, &metrics.pollNanos, &metrics.pollBuckets, time.Since(pollStarted))
			cancel()
			if err != nil {
				if isExpectedSoakConsumerError(err) {
					if errors.Is(err, api.ErrAssignmentLost) {
						partitionMetrics.assignmentLost.Add(1)
						replacement, waitErr := waitForSoakConsumerReplacement(ctx, handle, consumer)
						if waitErr != nil {
							report(fmt.Errorf("soak group assignment lost on partition %d: %v; replacement was not installed: %w (%s)", partition, err, waitErr, progress.diagnostic(partition)))
							return
						}
						consumer = replacement
					}
					continue
				}
				report(fmt.Errorf("soak group poll partition %d: %w", partition, err))
				return
			}
			progress.lastPollCompleted[partition].Store(time.Now().UnixNano())
			partitionMetrics.pollRecords.Add(uint64(len(result.Records)))
			if err := validateSoakDelivery(result, topic, partition, oracles[soakPartitionKey(soakStableTopic, partition)]); err != nil {
				report(err)
				return
			}
			deliveredBytes := soakRecordPayloadBytes(result.Records)
			partitionMetrics.deliveredRecords.Add(uint64(len(result.Records)))
			partitionMetrics.deliveredBytes.Add(deliveredBytes)
			metrics.deliveredBytes.Add(deliveredBytes)
			if len(result.Records) == 0 {
				partitionMetrics.emptyPolls.Add(1)
				continue
			}
			partitionMetrics.commitOps.Add(1)
			progress.lastCommitStarted[partition].Store(time.Now().UnixNano())
			commitStarted := time.Now()
			commitErr := consumer.Commit(ctx, topic, partition, result.NextOffset)
			recordSoakLatency(&metrics.commitOps, &metrics.commitNanos, &metrics.commitBuckets, time.Since(commitStarted))
			if commitErr != nil {
				if isExpectedSoakConsumerError(commitErr) {
					if errors.Is(commitErr, api.ErrAssignmentLost) {
						partitionMetrics.assignmentLost.Add(1)
						replacement, waitErr := waitForSoakConsumerReplacement(ctx, handle, consumer)
						if waitErr != nil {
							report(fmt.Errorf("soak group assignment lost during commit on partition %d: %v; replacement was not installed: %w (%s)", partition, commitErr, waitErr, progress.diagnostic(partition)))
							return
						}
						consumer = replacement
					}
					continue
				}
				report(fmt.Errorf("soak group commit partition %d: %w", partition, commitErr))
				return
			}
			progress.lastCommitFinished[partition].Store(time.Now().UnixNano())
			partitionMetrics.commitRecords.Add(uint64(len(result.Records)))
			oracle := oracles[soakPartitionKey(soakStableTopic, partition)]
			if err := oracle.commit(result.NextOffset); err != nil {
				report(err)
				return
			}
			stats, statsErr := consumer.Stats(topic, partition)
			if statsErr != nil && !isExpectedSoakConsumerError(statsErr) {
				report(fmt.Errorf("soak group stats partition %d: %w", partition, statsErr))
				return
			}
			if statsErr == nil && stats.Active && stats.CommittedOrInitial < result.NextOffset {
				report(fmt.Errorf("successful commit partition %d is not visible at next offset %d", partition, result.NextOffset))
				return
			}
		}
	}
}

func soakRecordPayloadBytes(records []api.Record) uint64 {
	var total uint64
	for _, record := range records {
		total += uint64(len(record.Value))
	}
	return total
}

func validateSoakDelivery(result api.FetchResult, topic api.TopicID, partition uint32, oracle *soakOracle) error {
	for index, record := range result.Records {
		want := result.Records[0].Offset + uint64(index)
		if record.Offset != want || record.Topic != topic || record.Partition != partition {
			return fmt.Errorf("group delivery has a noncontiguous record at partition %d", partition)
		}
		if err := oracle.delivery(record.Key, record.Offset); err != nil {
			return err
		}
	}
	if len(result.Records) != 0 && result.NextOffset != result.Records[len(result.Records)-1].Offset+1 {
		return fmt.Errorf("group delivery next offset %d does not follow the returned records", result.NextOffset)
	}
	return nil
}

func isExpectedSoakConsumerError(err error) bool {
	return errors.Is(err, api.ErrAssignmentLost) || errors.Is(err, api.ErrConcurrentOperation) || errors.Is(err, api.ErrClosing) || errors.Is(err, api.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func waitForSoakConsumerReplacement(ctx context.Context, handle *soakGroupHandle, previous *storage.GroupConsumer) (*storage.GroupConsumer, error) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		if current := handle.currentConsumer(); current != previous {
			if current == nil {
				return nil, api.ErrClosed
			}
			return current, nil
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			return nil, errors.New("replacement was not installed within 5s")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func churnLoop(ctx context.Context, store *storage.Store, handle *soakGroupHandle, topic api.TopicID, interval time.Duration, report func(error)) {
	toggle := false
	timer := time.NewTicker(interval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			toggle = !toggle
			consumer, err := store.OpenConsumerGroup(ctx, "soak-workers", soakGroupMembers(topic, toggle), soakGroupOptions())
			if err != nil {
				if ctx.Err() != nil || isExpectedSoakConsumerError(err) {
					continue
				}
				report(fmt.Errorf("replace soak membership: %w", err))
				return
			}
			handle.replaceConsumer(consumer)
		case <-ctx.Done():
			return
		}
	}
}

func soakGroupMembers(topic api.TopicID, split bool) []api.ConsumerGroupMember {
	if !split {
		return []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: topic, Partition: 0}, {Topic: topic, Partition: 1}}}}
	}
	return []api.ConsumerGroupMember{
		{Subscriptions: []api.TopicPartition{{Topic: topic, Partition: 0}}},
		{Subscriptions: []api.TopicPartition{{Topic: topic, Partition: 1}}},
	}
}

func soakGroupOptions() api.ConsumerGroupOptions {
	return api.ConsumerGroupOptions{Start: api.GroupStartEarliest, ProgressTimeout: 5 * time.Second, Fetch: api.FetchOptions{MaxRecords: 16, MaxBytes: 128 << 10}}
}

func maintenanceLoop(ctx context.Context, store *storage.Store, report func(error)) {
	retentionTicker := time.NewTicker(250 * time.Millisecond)
	snapshotTicker := time.NewTicker(2 * time.Second)
	defer retentionTicker.Stop()
	defer snapshotTicker.Stop()
	for {
		select {
		case <-retentionTicker.C:
			retentionContext, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			err := store.RunRetention(retentionContext)
			cancel()
			if err != nil && ctx.Err() == nil && !errors.Is(err, api.ErrClosing) && !errors.Is(err, api.ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				report(fmt.Errorf("run soak retention: %w", err))
				return
			}
		case <-snapshotTicker.C:
			if err := store.SaveSnapshots(); err != nil && ctx.Err() == nil && !errors.Is(err, api.ErrClosing) && !errors.Is(err, api.ErrClosed) {
				report(fmt.Errorf("save soak snapshots: %w", err))
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func statsLoop(ctx context.Context, store *storage.Store, handle *soakGroupHandle, topic api.TopicID, metrics *soakMetrics, report func(error)) {
	ticker := time.NewTicker(time.Duration(metrics.sampleIntervalNanos))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			stats, err := store.Stats()
			if err != nil && ctx.Err() == nil && !errors.Is(err, api.ErrClosing) && !errors.Is(err, api.ErrClosed) {
				report(fmt.Errorf("sample soak stats: %w", err))
				return
			}
			resources := metrics.sampleRuntime()
			consumerStats := make([]storage.ConsumerStats, 0, soakPartitionCount)
			if consumer := handle.currentConsumer(); consumer != nil {
				for partition := uint32(0); partition < soakPartitionCount; partition++ {
					stats, statsErr := consumer.Stats(topic, partition)
					if errors.Is(statsErr, api.ErrConcurrentOperation) {
						metrics.consumerStatsSkipped.Add(1)
						continue
					}
					if statsErr == nil && stats.Active {
						consumerStats = append(consumerStats, stats)
						metrics.recordLag(partition, stats.DeliveryLag, stats.CommitLag)
					}
				}
			}
			metrics.recordSample(time.Now(), resources, consumerStats)
			if err == nil && stats.OffsetsHistoryRemaining == 0 {
				report(errors.New("soak exhausted consumer-offset history capacity"))
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func verifySoakOracles(ctx context.Context, fixture soakFixture, oracles map[string]*soakOracle) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("verify soak oracles: %w", err)
		}
		progress := false
		for partition := uint32(0); partition < soakPartitionCount; partition++ {
			for _, topic := range []struct {
				name string
				part *storage.Partition
			}{
				{soakRetainedTopic, fixture.retainedParts[partition]},
				{soakStableTopic, fixture.stableParts[partition]},
			} {
				oracle := oracles[soakPartitionKey(topic.name, partition)]
				before := oracle.nextOffset()
				if err := oracle.verify(ctx, topic.part); err != nil {
					return err
				}
				if oracle.nextOffset() != before {
					progress = true
				}
			}
		}
		allDrained := true
		for _, oracle := range oracles {
			if oracle.hasUnverifiedAcknowledgement() {
				allDrained = false
				break
			}
		}
		if allDrained {
			return nil
		}
		if !progress {
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("verify soak oracles: %w", ctx.Err())
			}
		}
	}
}

func waitForSoakAdmissionDrain(ctx context.Context, fixture soakFixture) error {
	for {
		if err := checkSoakAdmissionDrained(fixture); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for soak admission drain: %w", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func checkSoakAdmissionDrained(fixture soakFixture) error {
	for _, partition := range append(append([]*storage.Partition{}, fixture.retainedParts...), fixture.stableParts...) {
		stats := partition.Stats()
		if stats.InFlightRecords != 0 || stats.InFlightBytes != 0 || stats.WaitingAppends != 0 {
			return fmt.Errorf("partition %s/%d retained admission state after drain: records=%d bytes=%d waiting=%d", stats.Topic, stats.Partition, stats.InFlightRecords, stats.InFlightBytes, stats.WaitingAppends)
		}
	}
	return nil
}

func (oracle *soakOracle) offer(key, value []byte) error {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if oracle.err != nil {
		return oracle.err
	}
	id := string(key)
	if _, exists := oracle.pending[id]; exists {
		return oracle.setErrorLocked(fmt.Errorf("duplicate offered soak record ID %q", id))
	}
	oracle.pending[id] = &soakExpected{value: append([]byte(nil), value...), offeredAt: time.Now()}
	return nil
}

func (oracle *soakOracle) acknowledge(key, value []byte, offset uint64) error {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if oracle.err != nil {
		return oracle.err
	}
	pending := oracle.pending[string(key)]
	if pending == nil {
		return oracle.setErrorLocked(fmt.Errorf("acknowledged soak record %q was not pending", key))
	}
	if !bytes.Equal(pending.value, value) {
		return oracle.setErrorLocked(fmt.Errorf("acknowledged soak record %q changed payload", key))
	}
	pending.acked = true
	pending.acknowledgedAt = time.Now()
	pending.offset = offset
	if pending.deliveredAt.IsZero() {
		oracle.deliveryTiming[offset] = soakTiming{offeredAt: pending.offeredAt, acknowledgedAt: pending.acknowledgedAt}
	} else if !pending.deliveryLatencyRecorded {
		oracle.recordDeliveryLatencyLocked(pending.offeredAt, pending.acknowledgedAt, pending.deliveredAt)
		pending.deliveryLatencyRecorded = true
	}
	if pending.observed {
		oracle.recordScanLatencyLocked(pending.offeredAt, pending.acknowledgedAt, pending.observedAt)
		if pending.observedOffset != offset {
			return oracle.setErrorLocked(fmt.Errorf("soak record %q observed at offset %d and acknowledged at %d", key, pending.observedOffset, offset))
		}
		delete(oracle.pending, string(key))
	}
	return nil
}

func (oracle *soakOracle) reject(key []byte) error {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if oracle.err != nil {
		return oracle.err
	}
	delete(oracle.pending, string(key))
	return nil
}

func (oracle *soakOracle) verify(ctx context.Context, partition *storage.Partition) error {
	for attempt := 0; attempt < 16; attempt++ {
		stats := partition.Stats()
		if err := oracle.updateBounds(stats.LogStartOffset, stats.DurableEnd); err != nil {
			return err
		}
		if oracle.nextOffset() >= stats.DurableEnd {
			return nil
		}
		result, err := partition.Fetch(ctx, oracle.nextOffset(), api.FetchOptions{MaxRecords: 32, MaxBytes: 128 << 10})
		if errors.Is(err, api.ErrOffsetOutOfRange) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("verify partition %s/%d: %w", oracle.topic, oracle.partition, err)
		}
		if len(result.Records) == 0 {
			return nil
		}
		next := oracle.nextOffset()
		for _, record := range result.Records {
			if record.Offset != next || record.Topic != oracle.topic || record.Partition != oracle.partition {
				return fmt.Errorf("oracle saw noncontiguous record on %s/%d at offset %d, expected %d", oracle.topic, oracle.partition, record.Offset, next)
			}
			if err := oracle.observe(record); err != nil {
				return err
			}
			_, _ = oracle.digest.Write(record.Key)
			_, _ = oracle.digest.Write([]byte{0})
			_, _ = oracle.digest.Write(record.Value)
			_, _ = oracle.digest.Write([]byte{0})
			next++
		}
		oracle.mu.Lock()
		oracle.next = result.NextOffset
		oracle.verified += uint64(len(result.Records))
		oracle.mu.Unlock()
	}
	return nil
}

func (oracle *soakOracle) updateBounds(logStart, durableEnd uint64) error {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if oracle.err != nil {
		return oracle.err
	}
	if logStart < oracle.lastL || durableEnd < oracle.lastH {
		return oracle.setErrorLocked(fmt.Errorf("oracle bounds regressed from L/H=%d/%d to %d/%d", oracle.lastL, oracle.lastH, logStart, durableEnd))
	}
	oracle.lastL, oracle.lastH = logStart, durableEnd
	if oracle.next < logStart {
		for id, pending := range oracle.pending {
			if pending.acked && pending.offset < logStart {
				delete(oracle.pending, id)
				oracle.expired++
			}
		}
		oracle.next = logStart
	}
	if oracle.next > durableEnd {
		return oracle.setErrorLocked(fmt.Errorf("oracle next offset %d exceeds durable end %d", oracle.next, durableEnd))
	}
	return nil
}

func (oracle *soakOracle) observe(record api.Record) error {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if oracle.err != nil {
		return oracle.err
	}
	if pending := oracle.pending[string(record.Key)]; pending != nil {
		if !bytes.Equal(pending.value, record.Value) {
			return oracle.setErrorLocked(fmt.Errorf("oracle payload mismatch at offset %d", record.Offset))
		}
		pending.observed = true
		pending.observedAt = time.Now()
		pending.observedOffset = record.Offset
		if pending.acked {
			oracle.recordScanLatencyLocked(pending.offeredAt, pending.acknowledgedAt, pending.observedAt)
			if pending.offset != record.Offset {
				return oracle.setErrorLocked(fmt.Errorf("oracle acknowledged offset %d differs from observed %d", pending.offset, record.Offset))
			}
			delete(oracle.pending, string(record.Key))
		}
	}
	if seed, run, producer, sequence, ok := parseSoakRecordKey(record.Key); ok && seed == oracle.seed && run == oracle.run {
		last, exists := oracle.lastSequences[producer]
		if exists && sequence <= last {
			return oracle.setErrorLocked(fmt.Errorf("oracle observed reused or reordered record ID for producer %d at sequence %d", producer, sequence))
		}
		oracle.lastSequences[producer] = sequence
	}
	return nil
}

func (oracle *soakOracle) hasUnverifiedAcknowledgement() bool {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	for _, pending := range oracle.pending {
		if pending.acked {
			return true
		}
	}
	return false
}

func (oracle *soakOracle) commit(next uint64) error {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if oracle.err != nil {
		return oracle.err
	}
	if next < oracle.committed {
		return oracle.setErrorLocked(fmt.Errorf("consumer commit regressed from %d to %d", oracle.committed, next))
	}
	oracle.committed = next
	return nil
}

func (oracle *soakOracle) delivery(key []byte, offset uint64) error {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if oracle.err != nil {
		return oracle.err
	}
	if offset < oracle.committed {
		return oracle.setErrorLocked(fmt.Errorf("consumer redelivered committed offset %d below %d", offset, oracle.committed))
	}
	deliveredAt := time.Now()
	deliveryRecorded := false
	if timing, ok := oracle.deliveryTiming[offset]; ok {
		oracle.recordDeliveryLatencyLocked(timing.offeredAt, timing.acknowledgedAt, deliveredAt)
		delete(oracle.deliveryTiming, offset)
		deliveryRecorded = true
	}
	if pending := oracle.pending[string(key)]; pending != nil {
		pending.deliveredAt = deliveredAt
		if pending.acked && !deliveryRecorded && !pending.deliveryLatencyRecorded {
			oracle.recordDeliveryLatencyLocked(pending.offeredAt, pending.acknowledgedAt, deliveredAt)
			pending.deliveryLatencyRecorded = true
		}
	}
	return nil
}

func (oracle *soakOracle) recordScanLatencyLocked(offeredAt, acknowledgedAt, observedAt time.Time) {
	if oracle.metrics == nil || offeredAt.IsZero() || acknowledgedAt.IsZero() || observedAt.IsZero() {
		return
	}
	recordSoakLatency(&oracle.metrics.offerToScanOps, &oracle.metrics.offerToScanNanos, &oracle.metrics.offerToScanBuckets, observedAt.Sub(offeredAt))
	recordSoakLatency(&oracle.metrics.ackToScanOps, &oracle.metrics.ackToScanNanos, &oracle.metrics.ackToScanBuckets, observedAt.Sub(acknowledgedAt))
}

func (oracle *soakOracle) recordDeliveryLatencyLocked(offeredAt, acknowledgedAt, deliveredAt time.Time) {
	if oracle.metrics == nil || offeredAt.IsZero() || acknowledgedAt.IsZero() || deliveredAt.IsZero() {
		return
	}
	recordSoakLatency(&oracle.metrics.offerToDeliveryOps, &oracle.metrics.offerToDeliveryNanos, &oracle.metrics.offerToDeliveryBuckets, deliveredAt.Sub(offeredAt))
	recordSoakLatency(&oracle.metrics.ackToDeliveryOps, &oracle.metrics.ackToDeliveryNanos, &oracle.metrics.ackToDeliveryBuckets, deliveredAt.Sub(acknowledgedAt))
}

func (oracle *soakOracle) setErrorLocked(err error) error {
	if oracle.err == nil {
		oracle.err = err
	}
	return oracle.err
}

func (oracle *soakOracle) nextOffset() uint64 {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	return oracle.next
}

func (oracle *soakOracle) verifiedCount() uint64 {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	return oracle.verified
}

func (oracle *soakOracle) expiredCount() uint64 {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	return oracle.expired
}

func (oracle *soakOracle) digestHex() string {
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	return hex.EncodeToString(oracle.digest.Sum(nil))
}

func parseSoakRecordKey(key []byte) (uint64, uint64, uint32, uint64, bool) {
	parts := strings.Split(string(key), "-")
	if len(parts) != 5 || parts[0] != "soak" {
		return 0, 0, 0, 0, false
	}
	seed, err1 := strconv.ParseUint(parts[1], 16, 64)
	run, err2 := strconv.ParseUint(parts[2], 16, 64)
	producer, err3 := strconv.ParseUint(parts[3], 16, 32)
	sequence, err4 := strconv.ParseUint(parts[4], 16, 64)
	return seed, run, uint32(producer), sequence, err1 == nil && err2 == nil && err3 == nil && err4 == nil
}

func soakPartitionKey(name string, partition uint32) string {
	return name + "/" + strconv.FormatUint(uint64(partition), 10)
}

func (handle *soakGroupHandle) currentConsumer() *storage.GroupConsumer {
	handle.mu.Lock()
	defer handle.mu.Unlock()
	return handle.consumer
}

func (handle *soakGroupHandle) replaceConsumer(consumer *storage.GroupConsumer) {
	handle.mu.Lock()
	handle.consumer = consumer
	handle.mu.Unlock()
}

func newSoakMetrics(sampleInterval time.Duration, sampleLimit uint64) *soakMetrics {
	if sampleInterval <= 0 {
		sampleInterval = soakDefaultSampleEvery
	}
	if sampleLimit == 0 {
		sampleLimit = soakDefaultSampleLimit
	}
	return &soakMetrics{sampleIntervalNanos: int64(sampleInterval), sampleLimit: sampleLimit, samples: make([]soakSample, 0, minUint64(sampleLimit, 1024)), phaseDurations: make(map[string]time.Duration)}
}

func minUint64(left, right uint64) int {
	if left < right {
		return int(left)
	}
	return int(right)
}

func (metrics *soakMetrics) startMeasurement(now time.Time) {
	metrics.measurementStarted.Store(now.UnixNano())
	metrics.measurementFinished.Store(0)
	metrics.measurementAccumulated.Store(0)
}

func (metrics *soakMetrics) finishMeasurement(now time.Time) {
	metrics.measurementFinished.CompareAndSwap(0, now.UnixNano())
}

func (metrics *soakMetrics) addMeasurement(duration time.Duration) {
	if duration > 0 {
		metrics.measurementAccumulated.Add(int64(duration))
	}
}

func (metrics *soakMetrics) measurementDuration() time.Duration {
	if accumulated := metrics.measurementAccumulated.Load(); accumulated > 0 {
		return time.Duration(accumulated)
	}
	started := metrics.measurementStarted.Load()
	finished := metrics.measurementFinished.Load()
	if started == 0 {
		return 0
	}
	if finished == 0 {
		finished = time.Now().UnixNano()
	}
	if finished <= started {
		return 0
	}
	return time.Duration(finished - started)
}

func (metrics *soakMetrics) recordPhase(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	metrics.phaseMu.Lock()
	metrics.phaseDurations[name] += duration
	metrics.phaseMu.Unlock()
}

func (metrics *soakMetrics) phaseDuration(name string) time.Duration {
	metrics.phaseMu.Lock()
	defer metrics.phaseMu.Unlock()
	return metrics.phaseDurations[name]
}

func (metrics *soakMetrics) recordSchedulerLate(lateness time.Duration) {
	if lateness <= 0 {
		return
	}
	metrics.schedulerLate.Add(1)
	metrics.updateMax(&metrics.maxSchedulerLateness, uint64(lateness))
}

func (metrics *soakMetrics) recordSample(now time.Time, resources soakResourceSample, consumerStats []storage.ConsumerStats) {
	started := metrics.measurementStarted.Load()
	if started == 0 {
		return
	}
	elapsed := now.UnixNano() - started
	if elapsed < 0 {
		return
	}
	sample := soakSample{
		ElapsedNanos: uint64(elapsed),
		Phase:        "measure",
		Offered:      metrics.stableOffered.Load(),
		Acknowledged: metrics.stableAcknowledged.Load(),
		Resources:    resources,
		Partitions:   make(map[string]soakSamplePartition, len(consumerStats)),
	}
	for _, stats := range consumerStats {
		key := soakPartitionKey(soakStableTopic, stats.Partition)
		sample.Partitions[key] = soakSamplePartition{
			DurableEnd:   stats.DurableEnd,
			NextDelivery: stats.NextDelivery,
			Committed:    stats.CommittedOrInitial,
			DeliveryLag:  stats.DeliveryLag,
			CommitLag:    stats.CommitLag,
		}
	}
	for partition := range metrics.partitions {
		sample.Delivered += metrics.partitions[partition].deliveredRecords.Load()
		sample.Committed += metrics.partitions[partition].commitRecords.Load()
	}
	metrics.sampleMu.Lock()
	defer metrics.sampleMu.Unlock()
	if uint64(len(metrics.samples)) >= metrics.sampleLimit {
		metrics.samplesDropped++
		return
	}
	metrics.samples = append(metrics.samples, sample)
}

func (metrics *soakMetrics) sampleSnapshot() ([]soakSample, uint64) {
	metrics.sampleMu.Lock()
	defer metrics.sampleMu.Unlock()
	return append([]soakSample(nil), metrics.samples...), metrics.samplesDropped
}

func (metrics *soakMetrics) sampleRuntime() soakResourceSample {
	goroutines := uint64(runtime.NumGoroutine())
	openFiles := uint64(0)
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		openFiles = uint64(len(entries))
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	metrics.updateMax(&metrics.maxGoroutines, goroutines)
	metrics.updateMax(&metrics.maxOpenFiles, openFiles)
	metrics.updateMax(&metrics.maxHeapBytes, memory.HeapAlloc)
	metrics.updateMax(&metrics.gcCycles, uint64(memory.NumGC))
	var rss, readBytes, writeBytes, userTicks, systemTicks uint64
	if sampledRSS, sampledRead, sampledWrite, sampledUser, sampledSystem, ok := processMetrics(); ok {
		rss, readBytes, writeBytes, userTicks, systemTicks = sampledRSS, sampledRead, sampledWrite, sampledUser, sampledSystem
		metrics.updateMax(&metrics.maxRSSBytes, rss)
		metrics.processReadBytes.Store(readBytes)
		metrics.processWriteBytes.Store(writeBytes)
		metrics.processUserTicks.Store(userTicks)
		metrics.processSystemTicks.Store(systemTicks)
	}
	return soakResourceSample{
		Goroutines: goroutines, OpenFiles: openFiles, HeapBytes: memory.HeapAlloc,
		RSSBytes: rss, GCCycles: uint64(memory.NumGC), ReadBytes: readBytes,
		WriteBytes: writeBytes, UserTicks: userTicks, SystemTicks: systemTicks,
	}
}

func (metrics *soakMetrics) recordLag(partition uint32, delivery, commit uint64) {
	metrics.lagSamples.Add(1)
	metrics.deliveryLagTotal.Add(delivery)
	metrics.commitLagTotal.Add(commit)
	metrics.updateMax(&metrics.maxDeliveryLag, delivery)
	metrics.updateMax(&metrics.maxCommitLag, commit)
	partitionMetrics := &metrics.partitions[partition]
	partitionMetrics.lagSamples.Add(1)
	partitionMetrics.deliveryLagTotal.Add(delivery)
	partitionMetrics.commitLagTotal.Add(commit)
	metrics.updateMax(&partitionMetrics.maxDeliveryLag, delivery)
	metrics.updateMax(&partitionMetrics.maxCommitLag, commit)
}

func (metrics *soakMetrics) partitionReport(partition int, elapsed time.Duration) soakPartitionReport {
	partitionMetrics := &metrics.partitions[partition]
	polls := partitionMetrics.pollOps.Load()
	commits := partitionMetrics.commitOps.Load()
	lagSamples := partitionMetrics.lagSamples.Load()
	averageDeliveryLag, averageCommitLag := uint64(0), uint64(0)
	if lagSamples != 0 {
		averageDeliveryLag = partitionMetrics.deliveryLagTotal.Load() / lagSamples
		averageCommitLag = partitionMetrics.commitLagTotal.Load() / lagSamples
	}
	return soakPartitionReport{
		DeliveredRecords:      partitionMetrics.deliveredRecords.Load(),
		DeliveredPayloadBytes: partitionMetrics.deliveredBytes.Load(),
		DeliveryRate:          float64(partitionMetrics.deliveredRecords.Load()) / elapsed.Seconds(),
		DeliveryByteRate:      float64(partitionMetrics.deliveredBytes.Load()) / elapsed.Seconds(),
		Polls:                 polls,
		PolledRecords:         partitionMetrics.pollRecords.Load(),
		AveragePollBatch:      averageBatchSize(partitionMetrics.pollRecords.Load(), polls),
		EmptyPolls:            partitionMetrics.emptyPolls.Load(),
		Commits:               commits,
		CommittedRecords:      partitionMetrics.commitRecords.Load(),
		AverageCommitBatch:    averageBatchSize(partitionMetrics.commitRecords.Load(), commits),
		AssignmentLost:        partitionMetrics.assignmentLost.Load(),
		LagSamples:            lagSamples,
		AverageDeliveryLag:    averageDeliveryLag,
		MaxDeliveryLag:        partitionMetrics.maxDeliveryLag.Load(),
		AverageCommitLag:      averageCommitLag,
		MaxCommitLag:          partitionMetrics.maxCommitLag.Load(),
	}
}

func averageBatchSize(records, operations uint64) float64 {
	if operations == 0 {
		return 0
	}
	return float64(records) / float64(operations)
}

func processMetrics() (rss, readBytes, writeBytes, userTicks, systemTicks uint64, ok bool) {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, 0, 0, 0, false
	}
	for _, line := range strings.Split(string(status), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[2] != "kB" {
			return 0, 0, 0, 0, 0, false
		}
		rss, err = strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, 0, 0, 0, 0, false
		}
		rss *= 1024
		break
	}
	ioData, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, 0, 0, 0, 0, false
	}
	for _, line := range strings.Split(string(ioData), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			return 0, 0, 0, 0, 0, false
		}
		switch fields[0] {
		case "read_bytes:":
			readBytes = value
		case "write_bytes:":
			writeBytes = value
		}
	}
	statData, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, 0, 0, 0, 0, false
	}
	closeParen := strings.LastIndex(string(statData), ")")
	if closeParen < 0 {
		return 0, 0, 0, 0, 0, false
	}
	fields := strings.Fields(string(statData)[closeParen+2:])
	if len(fields) <= 12 {
		return 0, 0, 0, 0, 0, false
	}
	userTicks, err = strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, 0, false
	}
	systemTicks, err = strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, 0, false
	}
	return rss, readBytes, writeBytes, userTicks, systemTicks, true
}

func (metrics *soakMetrics) updateMax(target *atomic.Uint64, value uint64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func (metrics *soakMetrics) summary() string {
	lagSamples := metrics.lagSamples.Load()
	averageDeliveryLag, averageCommitLag := uint64(0), uint64(0)
	if lagSamples != 0 {
		averageDeliveryLag = metrics.deliveryLagTotal.Load() / lagSamples
		averageCommitLag = metrics.commitLagTotal.Load() / lagSamples
	}
	latencies := []string{
		metrics.latencySummary("append", &metrics.appendOps, &metrics.appendNanos, &metrics.appendBuckets),
		metrics.latencySummary("poll", &metrics.pollOps, &metrics.pollNanos, &metrics.pollBuckets),
		metrics.latencySummary("commit", &metrics.commitOps, &metrics.commitNanos, &metrics.commitBuckets),
		metrics.latencySummary("verify", &metrics.verifyOps, &metrics.verifyNanos, &metrics.verifyBuckets),
		metrics.latencySummary("offer_to_scan", &metrics.offerToScanOps, &metrics.offerToScanNanos, &metrics.offerToScanBuckets),
		metrics.latencySummary("ack_to_scan", &metrics.ackToScanOps, &metrics.ackToScanNanos, &metrics.ackToScanBuckets),
		metrics.latencySummary("offer_to_delivery", &metrics.offerToDeliveryOps, &metrics.offerToDeliveryNanos, &metrics.offerToDeliveryBuckets),
		metrics.latencySummary("ack_to_delivery", &metrics.ackToDeliveryOps, &metrics.ackToDeliveryNanos, &metrics.ackToDeliveryBuckets),
	}
	return fmt.Sprintf("offered=%d acknowledged=%d unknown=%d known_rejected=%d cancelled=%d overload_calls=%d acknowledged_bytes=%d delivered_payload_bytes=%d max_goroutines=%d max_open_files=%d max_heap_bytes=%d max_rss_bytes=%d gc_cycles=%d process_read_bytes=%d process_write_bytes=%d process_user_ticks=%d process_system_ticks=%d lag_samples=%d average_delivery_lag=%d max_delivery_lag=%d average_commit_lag=%d max_commit_lag=%d %s", metrics.offered.Load(), metrics.acknowledged.Load(), metrics.unknown.Load(), metrics.knownRejected.Load(), metrics.cancelled.Load(), metrics.overloadCalls.Load(), metrics.acknowledgedBytes.Load(), metrics.deliveredBytes.Load(), metrics.maxGoroutines.Load(), metrics.maxOpenFiles.Load(), metrics.maxHeapBytes.Load(), metrics.maxRSSBytes.Load(), metrics.gcCycles.Load(), metrics.processReadBytes.Load(), metrics.processWriteBytes.Load(), metrics.processUserTicks.Load(), metrics.processSystemTicks.Load(), lagSamples, averageDeliveryLag, metrics.maxDeliveryLag.Load(), averageCommitLag, metrics.maxCommitLag.Load(), strings.Join(latencies, " "))
}

func (metrics *soakMetrics) latencySummary(name string, operations, nanos *atomic.Uint64, buckets *[soakLatencyBucketCount]atomic.Uint64) string {
	values := make([]uint64, len(buckets))
	for index := range buckets {
		values[index] = buckets[index].Load()
	}
	return fmt.Sprintf("%s_ops=%d_%s_nanos=%d_%s_avg_nanos=%d_%s_p50_bucket_nanos=%d_%s_p95_bucket_nanos=%d_%s_p99_bucket_nanos=%d_%s_buckets=%v", name, operations.Load(), name, nanos.Load(), name, averageLatencyNanos(operations.Load(), nanos.Load()), name, latencyBucketQuantile(values, 0.50), name, latencyBucketQuantile(values, 0.95), name, latencyBucketQuantile(values, 0.99), name, values)
}

func averageLatencyNanos(operations, nanos uint64) uint64 {
	if operations == 0 {
		return 0
	}
	return nanos / operations
}

func latencyBucketQuantile(values []uint64, fraction float64) uint64 {
	var total uint64
	for _, value := range values {
		total += value
	}
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * fraction)
	if target == 0 {
		target = 1
	}
	boundaries := [...]uint64{1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 5_000_000_000, 10_000_000_000, 30_000_000_000, 60_000_000_000}
	var cumulative uint64
	for index, value := range values {
		cumulative += value
		if cumulative >= target {
			if index < len(boundaries) {
				return boundaries[index]
			}
			return 0
		}
	}
	return 0
}

func recordSoakLatency(operations, nanos *atomic.Uint64, buckets *[soakLatencyBucketCount]atomic.Uint64, elapsed time.Duration) {
	operations.Add(1)
	nanos.Add(uint64(elapsed))
	boundaries := [...]time.Duration{time.Microsecond, 10 * time.Microsecond, 100 * time.Microsecond, time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute}
	bucket := len(boundaries)
	for index, boundary := range boundaries {
		if elapsed < boundary {
			bucket = index
			break
		}
	}
	buckets[bucket].Add(1)
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
