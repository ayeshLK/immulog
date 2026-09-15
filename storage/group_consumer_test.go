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

func TestGroupConsumerPersistsMultiMemberAssignmentsAndFencesSnapshots(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("group-orders", 2, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partitions := make([]*Partition, 2)
	for index := range partitions {
		partitions[index], err = store.OpenPartition(descriptor.ID, uint32(index), PartitionOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := partitions[index].Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: uint32(index), Value: []byte{byte('a' + index)}}); err != nil {
			t.Fatal(err)
		}
	}
	members := []api.ConsumerGroupMember{
		{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 1}}},
		{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}},
	}
	options := api.ConsumerGroupOptions{Start: api.GroupStartEarliest, Fetch: api.FetchOptions{MaxRecords: 4, MaxBytes: 1024}}
	first, err := store.OpenConsumerGroup(context.Background(), "workers", members, options)
	if err != nil {
		t.Fatal(err)
	}
	if subscriptions := first.Subscriptions(); len(subscriptions) != 2 || subscriptions[0].Partition != 0 || subscriptions[1].Partition != 1 {
		t.Fatalf("subscriptions = %#v, want canonical two-partition assignment", subscriptions)
	}
	for partition := uint32(0); partition < 2; partition++ {
		result, err := first.Poll(context.Background(), descriptor.ID, partition, api.FetchOptions{})
		if err != nil || len(result.Records) != 1 || result.Records[0].Partition != partition || result.NextOffset != 1 {
			t.Fatalf("first poll partition %d = (%#v, %v)", partition, result, err)
		}
		if err := first.Commit(context.Background(), descriptor.ID, partition, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := partitions[partition].Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: partition, Value: []byte{byte('c' + partition)}}); err != nil {
			t.Fatal(err)
		}
	}
	second, err := store.OpenConsumerGroup(context.Background(), "workers", members, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("stale group poll = %v, want ErrAssignmentLost", err)
	}
	for partition := uint32(0); partition < 2; partition++ {
		result, err := second.Poll(context.Background(), descriptor.ID, partition, api.FetchOptions{})
		if err != nil || len(result.Records) != 1 || result.Records[0].Offset != 1 {
			t.Fatalf("replacement poll partition %d = (%#v, %v)", partition, result, err)
		}
		if err := second.Commit(context.Background(), descriptor.ID, partition, 2); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resumed, err := store.OpenConsumerGroup(context.Background(), "workers", members, options)
	if err != nil {
		t.Fatal(err)
	}
	for partition := uint32(0); partition < 2; partition++ {
		result, err := resumed.Poll(context.Background(), descriptor.ID, partition, api.FetchOptions{})
		if err != nil || len(result.Records) != 0 || result.NextOffset != 2 {
			t.Fatalf("restart poll partition %d = (%#v, %v)", partition, result, err)
		}
	}
}

func TestGroupConsumerRejectsOutOfRangeExplicitStartBeforeCreatingGroup(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("explicit-group", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	options := api.ConsumerGroupOptions{Start: api.GroupStartExplicit, ExplicitStarts: []api.ExplicitStart{{Topic: descriptor.ID, Partition: 0, Next: 1}}}
	if _, err := store.OpenConsumerGroup(context.Background(), "explicit", members, options); !errors.Is(err, api.ErrOffsetOutOfRange) {
		t.Fatalf("explicit open error = %v, want ErrOffsetOutOfRange", err)
	}
	if store.offsetsState.groups["explicit"] != nil {
		t.Fatal("invalid explicit start created durable group state")
	}
}

func TestGroupConsumerDeadlineFencesEveryMember(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("deadline-group", 2, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{
		{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}},
		{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 1}}},
	}
	consumer, err := store.OpenConsumerGroup(context.Background(), "deadline-group", members, api.ConsumerGroupOptions{Start: api.GroupStartEarliest, ProgressTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := consumer.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired first-member poll = %v, want ErrAssignmentLost", err)
	}
	if _, err := consumer.Poll(context.Background(), descriptor.ID, 1, api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired second-member poll = %v, want ErrAssignmentLost", err)
	}
	if err := consumer.Commit(context.Background(), descriptor.ID, 0, 0); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired member commit = %v, want ErrAssignmentLost", err)
	}
}

func TestGroupConsumerPersistsTimeoutCauseOnReplacement(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("timeout-transition", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	options := api.ConsumerGroupOptions{Start: api.GroupStartEarliest, ProgressTimeout: time.Millisecond}
	first, err := store.OpenConsumerGroup(context.Background(), "timeout-transition", members, options)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := first.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired poll = %v, want ErrAssignmentLost", err)
	}
	if !store.expiredGroups["timeout-transition"] {
		t.Fatal("expired group was not marked for a durable timeout transition")
	}
	replacement, err := store.OpenConsumerGroup(context.Background(), "timeout-transition", members, options)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.generation != first.generation+1 {
		t.Fatalf("timeout replacement generation = %d, want %d", replacement.generation, first.generation+1)
	}
	if store.expiredGroups["timeout-transition"] {
		t.Fatal("timeout transition marker was not consumed")
	}
}
