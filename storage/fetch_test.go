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

	"github.com/ayeshLK/immulog/api"
)

func TestFetchEnforcesHardResultLimitsAndCopiesRecords(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range [][]byte{[]byte("first"), []byte("second"), []byte("third")} {
		if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: value, Headers: []api.Header{{Name: "source", Value: []byte("test")}}}); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := partition.Read(0, 3)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := recordEncodedBytes(stored[0])
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := recordEncodedBytes(stored[1])
	if err != nil {
		t.Fatal(err)
	}
	result, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 3, MaxBytes: firstBytes + secondBytes - 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || result.NextOffset != 1 {
		t.Fatalf("bounded fetch = %#v, want one record and next offset 1", result)
	}
	result.Records[0].Value[0] = 'X'
	result.Records[0].Headers[0].Value[0] = 'X'
	replayed, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: firstBytes})
	if err != nil {
		t.Fatal(err)
	}
	if string(replayed.Records[0].Value) != "first" || string(replayed.Records[0].Headers[0].Value) != "test" {
		t.Fatalf("fetch result mutated durable or shared record: %#v", replayed.Records[0])
	}
	tooSmall, err := partition.Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1, MaxBytes: firstBytes - 1})
	if !errors.Is(err, api.ErrFetchLimitTooSmall) {
		t.Fatalf("small fetch error = %v, want ErrFetchLimitTooSmall", err)
	}
	if len(tooSmall.Records) != 0 || tooSmall.NextOffset != 0 {
		t.Fatalf("small fetch result = %#v, want unchanged empty result", tooSmall)
	}
}

func TestRawReaderSeekCursorAndConcurrentOperationFence(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"zero", "one", "two"} {
		if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte(value)}); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := partition.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	reader.operation.Lock()
	if _, err := reader.Fetch(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrConcurrentOperation) {
		t.Fatalf("overlapping fetch error = %v, want ErrConcurrentOperation", err)
	}
	reader.operation.Unlock()
	result, err := reader.Fetch(context.Background(), api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || result.Records[0].Offset != 0 || result.NextOffset != 1 {
		t.Fatalf("reader first fetch = %#v", result)
	}
	if next, err := reader.NextOffset(); err != nil || next != 1 {
		t.Fatalf("reader next = (%d, %v), want (1, nil)", next, err)
	}
	if err := reader.Seek(2); err != nil {
		t.Fatal(err)
	}
	result, err = reader.Fetch(context.Background(), api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
	if err != nil || len(result.Records) != 1 || result.Records[0].Offset != 2 {
		t.Fatalf("reader seek fetch = (%#v, %v)", result, err)
	}
	if err := reader.Seek(4); !errors.Is(err, api.ErrOffsetOutOfRange) {
		t.Fatalf("seek beyond end error = %v, want ErrOffsetOutOfRange", err)
	}
	if next, err := reader.NextOffset(); err != nil || next != 3 {
		t.Fatalf("reader next after failed seek = (%d, %v), want (3, nil)", next, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Fetch(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrClosed) {
		t.Fatalf("fetch after reader close = %v, want ErrClosed", err)
	}
}

func TestFetchCancellationBeforeReadDoesNotAdvanceReader(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	reader, err := partition.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.Fetch(ctx, api.FetchOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled fetch error = %v, want context.Canceled", err)
	}
	if next, err := reader.NextOffset(); err != nil || next != 0 {
		t.Fatalf("reader advanced after canceled fetch = (%d, %v)", next, err)
	}
}

func TestFetchFailureFencesPartition(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := partition.segments[0].file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Fetch(context.Background(), 0, api.FetchOptions{}); err == nil {
		t.Fatal("fetch from a closed segment unexpectedly succeeded")
	}
	if _, err := partition.Fetch(context.Background(), 0, api.FetchOptions{}); !errors.Is(err, api.ErrPartitionUnavailable) {
		t.Fatalf("fetch after read failure = %v, want ErrPartitionUnavailable", err)
	}
}
