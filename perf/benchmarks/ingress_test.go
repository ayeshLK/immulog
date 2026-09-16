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

package benchmarks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

const (
	benchmarkSegmentBytes = uint64(64 << 20)
	benchmarkBatchBytes   = uint32(4 << 20)
	benchmarkTotalRecords = uint64(4096)
)

// BenchmarkIngressAppend measures one-record durable append latency. It is a
// baseline for the public Append path, not a multi-producer throughput test.
func BenchmarkIngressAppend(b *testing.B) {
	benchmarkIngressAppend(b, 1, 256, benchmarkSegmentBytes)
}

// BenchmarkIngressAppendParallel measures the bounded ingress path with an
// explicit producer count and enough batch capacity for coalescing.
func BenchmarkIngressAppendParallel(b *testing.B) {
	for _, producers := range []int{1, 2, 4, 8, 16, 32, 64} {
		for _, payloadSize := range []int{64, 256, 1024, 4096} {
			b.Run(fmt.Sprintf("producers-%d/payload-%d", producers, payloadSize), func(b *testing.B) {
				payload := benchmarkPayload(payloadSize)
				partition, topic, store := benchmarkPartition(b, storage.PartitionOptions{
					BatchBytes:       benchmarkBatchBytes,
					BatchRecords:     256,
					SegmentBytes:     benchmarkSegmentBytes,
					InFlightBytes:    32 << 20,
					InFlightRecords:  4096,
					AdmissionWaiters: 256,
				})
				defer store.Close()
				b.SetBytes(int64(payloadSize))
				b.ReportAllocs()
				b.ResetTimer()
				err := runConcurrentAppends(b, partition, topic, payload, producers)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkCheckDurableEnd(b, partition, uint64(b.N))
				benchmarkReportThroughput(b, uint64(b.N))
			})
		}
	}
}

// BenchmarkDirectAppendBatch measures synchronous durable batches without
// adding an EndOffset call to each timed iteration.
func BenchmarkIngressBatchLinger(b *testing.B) {
	for _, producers := range []int{8, 32, 64} {
		for _, linger := range []time.Duration{0, 100 * time.Microsecond, time.Millisecond, 5 * time.Millisecond} {
			b.Run(fmt.Sprintf("producers-%d/linger-%s", producers, linger), func(b *testing.B) {
				payload := benchmarkPayload(1024)
				partition, topic, store := benchmarkPartition(b, storage.PartitionOptions{
					BatchBytes:       benchmarkBatchBytes,
					BatchRecords:     256,
					SegmentBytes:     benchmarkSegmentBytes,
					InFlightBytes:    32 << 20,
					InFlightRecords:  4096,
					AdmissionWaiters: 256,
					BatchLinger:      linger,
				})
				defer store.Close()
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				b.ResetTimer()
				err := runConcurrentAppends(b, partition, topic, payload, producers)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkCheckDurableEnd(b, partition, uint64(b.N))
				benchmarkReportThroughput(b, uint64(b.N))
			})
		}
	}
}

func BenchmarkDirectAppendBatch(b *testing.B) {
	for _, batchRecords := range []uint32{1, 8, 64, 256} {
		for _, payloadSize := range []int{256, 1024, 4096} {
			b.Run(fmt.Sprintf("records-%d/payload-%d", batchRecords, payloadSize), func(b *testing.B) {
				payload := benchmarkPayload(payloadSize)
				partition, topic, store := benchmarkPartition(b, storage.PartitionOptions{
					BatchBytes:   benchmarkBatchBytes,
					BatchRecords: batchRecords,
					SegmentBytes: benchmarkSegmentBytes,
				})
				defer store.Close()
				records := benchmarkRecords(topic, 0, batchRecords, payload)
				b.SetBytes(int64(batchRecords) * int64(payloadSize))
				b.ReportAllocs()
				b.ResetTimer()
				var nextOffset uint64
				for index := 0; index < b.N; index++ {
					for recordIndex := range records {
						records[recordIndex].Offset = nextOffset + uint64(recordIndex)
					}
					if _, err := partition.AppendBatch(api.RecordBatch{
						Topic: topic, Partition: 0, BaseOffset: nextOffset, Records: records,
					}); err != nil {
						b.Fatal(err)
					}
					nextOffset += uint64(batchRecords)
				}
				b.StopTimer()
				benchmarkCheckDurableEnd(b, partition, nextOffset)
				benchmarkReportThroughput(b, nextOffset)
			})
		}
	}
}

func benchmarkIngressAppend(b *testing.B, batchRecords uint32, payloadSize int, segmentBytes uint64) {
	payload := benchmarkPayload(payloadSize)
	partition, topic, store := benchmarkPartition(b, storage.PartitionOptions{
		BatchBytes:   benchmarkBatchBytes,
		BatchRecords: batchRecords,
		SegmentBytes: segmentBytes,
	})
	defer store.Close()
	request := api.AppendRequest{Topic: topic, Partition: 0, Value: payload}
	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := partition.Append(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkCheckDurableEnd(b, partition, uint64(b.N))
	benchmarkReportThroughput(b, uint64(b.N))
}

func runConcurrentAppends(b *testing.B, partition *storage.Partition, topic api.TopicID, payload []byte, producers int) error {
	var next atomic.Uint64
	var firstErr error
	var errOnce sync.Once
	var workers sync.WaitGroup
	workers.Add(producers)
	for range producers {
		go func() {
			defer workers.Done()
			request := api.AppendRequest{Topic: topic, Partition: 0, Value: payload}
			for {
				index := next.Add(1)
				if index > uint64(b.N) {
					return
				}
				if _, err := partition.Append(context.Background(), request); err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}()
	}
	workers.Wait()
	return firstErr
}

func benchmarkPartition(b *testing.B, options storage.PartitionOptions) (*storage.Partition, api.TopicID, *storage.Store) {
	b.Helper()
	store, err := storage.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	topic := benchmarkTopic()
	partition, err := store.OpenPartition(topic, 0, options)
	if err != nil {
		store.Close()
		b.Fatal(err)
	}
	return partition, topic, store
}

func benchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte(index)
	}
	return payload
}

func benchmarkRecords(topic api.TopicID, partition uint32, count uint32, payload []byte) []api.Record {
	records := make([]api.Record, count)
	for index := range records {
		records[index] = api.Record{
			Topic: topic, Partition: partition, Offset: uint64(index), Value: payload,
		}
	}
	return records
}

func benchmarkCheckDurableEnd(b *testing.B, partition *storage.Partition, expected uint64) {
	b.Helper()
	stats := partition.Stats()
	if stats.DurableEnd != expected {
		b.Fatalf("durable end = %d, want %d", stats.DurableEnd, expected)
	}
	if stats.AppendUnknown != 0 || stats.AppendKnownUnwritten != 0 {
		b.Fatalf("append outcomes include unknown=%d known_unwritten=%d", stats.AppendUnknown, stats.AppendKnownUnwritten)
	}
	if expected != 0 {
		b.ReportMetric(float64(stats.WriteLatency.Operations)/float64(expected), "writes/record")
		b.ReportMetric(float64(stats.SyncLatency.Operations)/float64(expected), "syncs/record")
	}
}

func benchmarkReportThroughput(b *testing.B, records uint64) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(records)/elapsed, "records/s")
	}
}

func benchmarkTopic() api.TopicID {
	return api.TopicID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}
}
