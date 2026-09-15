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

func TestConsumerCommitsResumeAndFencesReassignment(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("orders", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"zero", "one", "two"} {
		if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte(value)}); err != nil {
			t.Fatal(err)
		}
	}
	options := api.ConsumerOptions{Start: api.GroupStartEarliest, Fetch: api.FetchOptions{MaxRecords: 2, MaxBytes: 1024}}
	first, err := store.OpenConsumer(context.Background(), "orders", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := first.Poll(context.Background(), api.FetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 2 || result.NextOffset != 2 {
		t.Fatalf("first poll = %#v, want offsets [0,2)", result)
	}
	if err := first.Commit(context.Background(), 3); !errors.Is(err, api.ErrInvalidCommit) {
		t.Fatalf("commit beyond delivery = %v, want ErrInvalidCommit", err)
	}
	if err := first.Commit(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	second, err := store.OpenConsumer(context.Background(), "orders", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Poll(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("stale consumer poll = %v, want ErrAssignmentLost", err)
	}
	result, err = second.Poll(context.Background(), api.FetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || result.Records[0].Offset != 2 || result.NextOffset != 3 {
		t.Fatalf("replacement poll = %#v, want offset 2", result)
	}
	if err := second.Commit(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resumed, err := store.OpenConsumer(context.Background(), "orders", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	result, err = resumed.Poll(context.Background(), api.FetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 0 || result.NextOffset != 3 {
		t.Fatalf("restart poll = %#v, want empty cursor at 3", result)
	}
	if err := resumed.Commit(context.Background(), 2); !errors.Is(err, api.ErrCommitRegression) {
		t.Fatalf("regressing commit = %v, want ErrCommitRegression", err)
	}
}

func TestConsumerLatestStartPersistsBaselineBeforeFirstCommit(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("audit", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"before", "also-before"} {
		if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte(value)}); err != nil {
			t.Fatal(err)
		}
	}
	options := api.ConsumerOptions{Start: api.GroupStartLatest, Fetch: api.FetchOptions{MaxRecords: 10, MaxBytes: 1024}}
	first, err := store.OpenConsumer(context.Background(), "audit", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	second, err := store.OpenConsumer(context.Background(), "audit", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Poll(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("stale latest consumer poll = %v, want ErrAssignmentLost", err)
	}
	result, err := second.Poll(context.Background(), api.FetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || result.Records[0].Offset != 2 {
		t.Fatalf("persisted latest baseline was not reused: %#v", result)
	}
}

func TestConsumerClosePersistsGenerationFence(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("close-fence", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	options := api.ConsumerOptions{Start: api.GroupStartEarliest}
	consumer, err := store.OpenConsumer(context.Background(), "close-fence", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	before := store.offsetsState.groups["close-fence"].Generation
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	group := store.offsetsState.groups["close-fence"]
	if group.Generation != before+1 || len(group.AssignmentOwner) != 0 {
		t.Fatalf("close assignment = %#v, want empty generation %d", group, before+1)
	}
	if _, err := consumer.Poll(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrClosed) {
		t.Fatalf("poll after close = %v, want ErrClosed", err)
	}
	replacement, err := store.OpenConsumer(context.Background(), "close-fence", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.generation != before+2 {
		t.Fatalf("replacement generation = %d, want %d", replacement.generation, before+2)
	}
}

func TestConsumerProgressDeadlineFencesPollAndCommit(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("deadline", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
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
	consumer, err := store.OpenConsumer(context.Background(), "deadline", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest, ProgressTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := consumer.Poll(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired poll = %v, want ErrAssignmentLost", err)
	}
	if err := consumer.Commit(context.Background(), 0); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired commit = %v, want ErrAssignmentLost", err)
	}
}

func TestConsumerAssignmentReasonTracksLifecycle(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("assignment-cause", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	options := api.ConsumerOptions{Start: api.GroupStartEarliest}
	first, err := store.OpenConsumer(context.Background(), "assignment-cause", descriptor.ID, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if reason := assignmentReason(t, store.offsetsState.groups["assignment-cause"].AssignmentBody); reason != 1 {
		t.Fatalf("initial assignment reason = %d, want 1", reason)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if reason := assignmentReason(t, store.offsetsState.groups["assignment-cause"].AssignmentBody); reason != 2 {
		t.Fatalf("close assignment reason = %d, want 2", reason)
	}
	if _, err := store.OpenConsumer(context.Background(), "assignment-cause", descriptor.ID, 0, options); err != nil {
		t.Fatal(err)
	}
	if reason := assignmentReason(t, store.offsetsState.groups["assignment-cause"].AssignmentBody); reason != 5 {
		t.Fatalf("reopen assignment reason = %d, want 5", reason)
	}

	expiring, err := store.OpenConsumer(context.Background(), "timeout-cause", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest, ProgressTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := expiring.Poll(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired poll = %v, want ErrAssignmentLost", err)
	}
	if _, err := store.OpenConsumer(context.Background(), "timeout-cause", descriptor.ID, 0, options); err != nil {
		t.Fatal(err)
	}
	if reason := assignmentReason(t, store.offsetsState.groups["timeout-cause"].AssignmentBody); reason != 4 {
		t.Fatalf("timeout replacement reason = %d, want 4", reason)
	}
}

func assignmentReason(t *testing.T, body []byte) byte {
	t.Helper()
	cursor := eventCursor{data: body}
	if _, err := cursor.id(); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.stringValue(maxGroupIDBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.id(); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.u64(); err != nil {
		t.Fatal(err)
	}
	reason, err := cursor.u8()
	if err != nil {
		t.Fatal(err)
	}
	return reason
}
