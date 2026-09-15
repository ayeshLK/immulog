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

func TestConsumerUnknownCommitFencesAllDependentGroupWork(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("payments", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	consumer, err := store.OpenConsumer(context.Background(), "payments", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Poll(context.Background(), api.FetchOptions{}); err != nil {
		t.Fatal(err)
	}
	store.offsetsAppend = func(api.RecordBatch) (uint64, error) {
		return 0, errors.Join(api.ErrAppendOutcomeUnknown, errors.New("injected uncertain offsets append"))
	}
	if err := consumer.Commit(context.Background(), 1); !errors.Is(err, api.ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown commit error = %v, want ErrCommitOutcomeUnknown", err)
	}
	if _, err := consumer.Poll(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrGroupUnavailable) {
		t.Fatalf("poll after unknown commit = %v, want ErrGroupUnavailable", err)
	}
	if _, err := store.OpenConsumer(context.Background(), "other", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest}); !errors.Is(err, api.ErrGroupUnavailable) {
		t.Fatalf("open after unknown commit = %v, want ErrGroupUnavailable", err)
	}
}
