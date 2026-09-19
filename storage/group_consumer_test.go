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
	"sync"
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
	same, err := store.OpenConsumerGroup(context.Background(), "workers", members, options)
	if err != nil {
		t.Fatal(err)
	}
	if same != first {
		t.Fatal("identical group open did not reuse the live consumer")
	}
	if first.generation != 1 {
		t.Fatalf("coalesced generation = %d, want 1", first.generation)
	}
	replacementOptions := options
	replacementOptions.Fetch.MaxRecords++
	second, err := store.OpenConsumerGroup(context.Background(), "workers", members, replacementOptions)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("changed group open reused the live consumer")
	}
	if _, err := first.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("stale group poll = %v, want ErrAssignmentLost", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close stale group = %v", err)
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

func TestGroupConsumerCoalescesEquivalentSnapshots(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("coalesced-group", 2, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	for partition := uint32(0); partition < 2; partition++ {
		part, err := store.OpenPartition(descriptor.ID, partition, PartitionOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: partition, Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	members := []api.ConsumerGroupMember{
		{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 1}, {Topic: descriptor.ID, Partition: 0}}},
		{},
	}
	first, err := store.OpenConsumerGroup(context.Background(), "workers", members, api.ConsumerGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := store.offsetsState.groups["workers"]
	beforeGeneration, beforeAssignment := before.Generation, before.AssignmentOffset
	equivalentMembers := []api.ConsumerGroupMember{
		{},
		{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}, {Topic: descriptor.ID, Partition: 1}}},
	}
	equivalentOptions := api.ConsumerGroupOptions{
		Start:           api.GroupStartEarliest,
		Fetch:           api.FetchOptions{MaxRecords: 1024, MaxBytes: 4 << 20},
		ProgressTimeout: 30 * time.Second,
	}
	second, err := store.OpenConsumerGroup(context.Background(), "workers", equivalentMembers, equivalentOptions)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatal("equivalent group open did not reuse the live consumer")
	}
	after := store.offsetsState.groups["workers"]
	if after.Generation != beforeGeneration || after.AssignmentOffset != beforeAssignment {
		t.Fatalf("coalesced group changed durable assignment: generation %d/%d, assignment %d/%d", after.Generation, beforeGeneration, after.AssignmentOffset, beforeAssignment)
	}
	changedOptions := equivalentOptions
	changedOptions.Fetch.MaxWait = time.Millisecond
	third, err := store.OpenConsumerGroup(context.Background(), "workers", equivalentMembers, changedOptions)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("changed group options reused the live consumer")
	}
	if _, err := first.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("coalesced alias poll = %v, want ErrAssignmentLost after replacement", err)
	}
}

func TestGroupConsumerCoalescedAliasesShareLifecycle(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("coalesced-alias-group", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	part, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	first, err := store.OpenConsumerGroup(context.Background(), "workers", members, api.ConsumerGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.OpenConsumerGroup(context.Background(), "workers", members, api.ConsumerGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatal("equivalent opens did not return an alias")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{}); !errors.Is(err, api.ErrClosed) {
		t.Fatalf("poll through closed alias = %v, want ErrClosed", err)
	}
}

func TestGroupConsumerCoalescesExplicitStartOrdering(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("explicit-coalesced-group", 2, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	for partition := uint32(0); partition < 2; partition++ {
		part, err := store.OpenPartition(descriptor.ID, partition, PartitionOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: partition, Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}, {Topic: descriptor.ID, Partition: 1}}}}
	firstOptions := api.ConsumerGroupOptions{
		Start: api.GroupStartExplicit,
		ExplicitStarts: []api.ExplicitStart{
			{Topic: descriptor.ID, Partition: 0, Next: 0},
			{Topic: descriptor.ID, Partition: 1, Next: 0},
		},
	}
	first, err := store.OpenConsumerGroup(context.Background(), "workers", members, firstOptions)
	if err != nil {
		t.Fatal(err)
	}
	secondOptions := firstOptions
	secondOptions.ExplicitStarts = []api.ExplicitStart{firstOptions.ExplicitStarts[1], firstOptions.ExplicitStarts[0]}
	second, err := store.OpenConsumerGroup(context.Background(), "workers", members, secondOptions)
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Fatal("explicit-start ordering change did not coalesce")
	}
	changedOptions := firstOptions
	changedOptions.ExplicitStarts = []api.ExplicitStart{
		{Topic: descriptor.ID, Partition: 0, Next: 1},
		{Topic: descriptor.ID, Partition: 1, Next: 0},
	}
	if _, err := store.OpenConsumerGroup(context.Background(), "workers", members, changedOptions); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("changed explicit start error = %v, want ErrInvalidArgument", err)
	}
}

func TestGroupConsumerCoalescesConcurrentEquivalentOpens(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("concurrent-coalesced-group", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	part, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	first, err := store.OpenConsumerGroup(context.Background(), "workers", members, api.ConsumerGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := store.offsetsState.groups["workers"]
	beforeGeneration, beforeAssignment := before.Generation, before.AssignmentOffset
	results := make(chan *GroupConsumer, 8)
	errs := make(chan error, 8)
	var waitGroup sync.WaitGroup
	for index := 0; index < 8; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			consumer, openErr := store.OpenConsumerGroup(context.Background(), "workers", members, api.ConsumerGroupOptions{})
			if openErr != nil {
				errs <- openErr
				return
			}
			results <- consumer
		}()
	}
	waitGroup.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for consumer := range results {
		if consumer != first {
			t.Fatal("concurrent equivalent open returned a different consumer")
		}
	}
	after := store.offsetsState.groups["workers"]
	if after.Generation != beforeGeneration || after.AssignmentOffset != beforeAssignment {
		t.Fatalf("concurrent coalescing changed durable assignment: generation %d/%d, assignment %d/%d", after.Generation, beforeGeneration, after.AssignmentOffset, beforeAssignment)
	}
}

func TestGroupConsumerDoesNotCoalesceExpiredSnapshot(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("expired-coalesced-group", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	part, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	options := api.ConsumerGroupOptions{ProgressTimeout: time.Millisecond}
	first, err := store.OpenConsumerGroup(context.Background(), "workers", members, options)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	second, err := store.OpenConsumerGroup(context.Background(), "workers", members, options)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("expired group open reused the expired consumer")
	}
	if second.generation != first.generation+1 {
		t.Fatalf("expired replacement generation = %d, want %d", second.generation, first.generation+1)
	}
	if _, err := first.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("expired consumer poll = %v, want ErrAssignmentLost", err)
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

func TestGroupConsumerPollKeepsLeaseDuringSlowFetch(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("slow-group-fetch", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	consumer, err := store.OpenConsumerGroup(context.Background(), "slow-group-fetch", members, api.ConsumerGroupOptions{Start: api.GroupStartEarliest, ProgressTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cursor := consumer.cursors[topicKey{topic: descriptor.ID, partition: 0}]

	partition.mu.Lock()
	pollResult := make(chan error, 1)
	go func() {
		_, err := consumer.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024})
		pollResult <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		store.mu.Lock()
		active := consumer.members[cursor.owner].inFlight != 0
		store.mu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			partition.mu.Unlock()
			t.Fatal("group poll did not begin")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	partition.mu.Unlock()

	select {
	case err := <-pollResult:
		if err != nil {
			t.Fatalf("slow group poll = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow group poll did not complete")
	}
	if _, err := consumer.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil {
		t.Fatalf("group poll after slow fetch = %v, want nil", err)
	}
}

func TestGroupConsumerCommitKeepsLeaseDuringSlowDurableAppend(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("slow-group-commit", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
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
	members := []api.ConsumerGroupMember{{Subscriptions: []api.TopicPartition{{Topic: descriptor.ID, Partition: 0}}}}
	consumer, err := store.OpenConsumerGroup(context.Background(), "slow-group-commit", members, api.ConsumerGroupOptions{Start: api.GroupStartEarliest, ProgressTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := consumer.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil || len(result.Records) != 1 {
		t.Fatalf("initial group poll = (%#v, %v)", result, err)
	}

	appendOffsets := store.offsets.AppendBatch
	entered := make(chan struct{})
	release := make(chan struct{})
	store.offsetsAppend = func(batch api.RecordBatch) (uint64, error) {
		close(entered)
		<-release
		return appendOffsets(batch)
	}
	committed := make(chan error, 1)
	go func() { committed <- consumer.Commit(context.Background(), descriptor.ID, 0, 1) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("group commit did not reach the durable append barrier")
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	select {
	case err := <-committed:
		if err != nil {
			t.Fatalf("slow group commit = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow group commit did not complete")
	}
	if _, err := consumer.Poll(context.Background(), descriptor.ID, 0, api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}); err != nil {
		t.Fatalf("group poll after slow commit = %v, want nil", err)
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
