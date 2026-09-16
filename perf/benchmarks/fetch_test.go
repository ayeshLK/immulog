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

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

const benchmarkFetchBatchRecords = uint32(128)

func BenchmarkFetch(b *testing.B) {
	for _, mode := range []string{"segment", "tail"} {
		b.Run(mode, func(b *testing.B) {
			options := storage.PartitionOptions{
				BatchBytes:   benchmarkBatchBytes,
				BatchRecords: benchmarkFetchBatchRecords,
				SegmentBytes: benchmarkSegmentBytes,
			}
			if mode == "tail" {
				options.TailSlots = 32
				options.TailBytes = 8 << 20
			}
			partition, topic, store := benchmarkPartition(b, options)
			defer store.Close()
			benchmarkPopulate(b, partition, topic, benchmarkTotalRecords, benchmarkFetchBatchRecords, 256)
			fetchOptions := api.FetchOptions{MaxRecords: benchmarkFetchBatchRecords, MaxBytes: 1 << 20}
			b.SetBytes(int64(benchmarkFetchBatchRecords) * 256)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				offset := uint64(index%int(benchmarkTotalRecords/uint64(benchmarkFetchBatchRecords))) * uint64(benchmarkFetchBatchRecords)
				if mode == "tail" {
					offset = benchmarkTotalRecords - uint64(benchmarkFetchBatchRecords)
				}
				result, err := partition.Fetch(context.Background(), offset, fetchOptions)
				if err != nil {
					b.Fatal(err)
				}
				if uint32(len(result.Records)) != benchmarkFetchBatchRecords {
					b.Fatalf("fetched %d records, want %d", len(result.Records), benchmarkFetchBatchRecords)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)*float64(benchmarkFetchBatchRecords)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

func BenchmarkFetchParallel(b *testing.B) {
	partition, topic, store := benchmarkPartition(b, storage.PartitionOptions{
		BatchBytes:   benchmarkBatchBytes,
		BatchRecords: benchmarkFetchBatchRecords,
		SegmentBytes: benchmarkSegmentBytes,
	})
	defer store.Close()
	benchmarkPopulate(b, partition, topic, benchmarkTotalRecords, benchmarkFetchBatchRecords, 256)
	fetchOptions := api.FetchOptions{MaxRecords: benchmarkFetchBatchRecords, MaxBytes: 1 << 20}
	b.SetBytes(int64(benchmarkFetchBatchRecords) * 256)
	b.ReportAllocs()
	b.ResetTimer()
	err := runConcurrentFetch(b, partition, fetchOptions, 8)
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N)*float64(benchmarkFetchBatchRecords)/b.Elapsed().Seconds(), "records/s")
}

func benchmarkPopulate(b *testing.B, partition *storage.Partition, topic api.TopicID, total uint64, batchRecords uint32, payloadSize int) {
	b.Helper()
	payload := benchmarkPayload(payloadSize)
	for base := uint64(0); base < total; base += uint64(batchRecords) {
		records := benchmarkRecords(topic, 0, batchRecords, payload)
		for index := range records {
			records[index].Offset = base + uint64(index)
		}
		if _, err := partition.AppendBatch(api.RecordBatch{
			Topic: topic, Partition: 0, BaseOffset: base, Records: records,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func runConcurrentFetch(b *testing.B, partition *storage.Partition, options api.FetchOptions, readers int) error {
	var next atomic.Uint64
	var firstErr error
	var errOnce sync.Once
	var workers sync.WaitGroup
	workers.Add(readers)
	for range readers {
		go func() {
			defer workers.Done()
			reader, err := partition.NewReader(0)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
			for {
				if next.Add(1) > uint64(b.N) {
					return
				}
				result, err := reader.Fetch(context.Background(), options)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				if len(result.Records) != int(benchmarkFetchBatchRecords) {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("fetched %d records, want %d", len(result.Records), benchmarkFetchBatchRecords)
					})
					return
				}
				if result.NextOffset == benchmarkTotalRecords {
					if err := reader.Seek(0); err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
				}
			}
		}()
	}
	workers.Wait()
	return firstErr
}
