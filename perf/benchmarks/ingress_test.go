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
	"testing"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

// BenchmarkIngressAppend is the selected multi-producer terminal-writer path.
// Compare it with BenchmarkDirectAppendBatch when qualifying dependency changes
// or operating profiles; neither benchmark substitutes for crash/recovery tests.
func BenchmarkIngressAppend(b *testing.B) {
	store, err := storage.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	topic := benchmarkTopic()
	partition, err := store.OpenPartition(topic, 0, storage.PartitionOptions{BatchRecords: 1, BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		b.Fatal(err)
	}
	request := api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("benchmark")}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := partition.Append(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDirectAppendBatch retains a direct synchronous writer reference for
// like-for-like one-record durability comparisons with the ingress adapter.
func BenchmarkDirectAppendBatch(b *testing.B) {
	store, err := storage.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	topic := benchmarkTopic()
	partition, err := store.OpenPartition(topic, 0, storage.PartitionOptions{BatchRecords: 1, BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		base, err := partition.EndOffset()
		if err != nil {
			b.Fatal(err)
		}
		batch := api.RecordBatch{Topic: topic, Partition: 0, BaseOffset: base, Records: []api.Record{{Topic: topic, Partition: 0, Offset: base, Value: []byte("benchmark")}}}
		if _, err := partition.AppendBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTopic() api.TopicID {
	return api.TopicID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}
}
