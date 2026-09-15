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
	"time"

	"github.com/ayeshLK/immulog/api"
)

// ConsumerStats is a pull-based snapshot of one managed assignment. Lag fields
// are offset distances, not processing time or a guarantee about external
// effects. An inactive assignment reports no lag values.
type ConsumerStats struct {
	CapturedAt               time.Time
	GroupID                  string
	Topic                    api.TopicID
	Partition                uint32
	Active                   bool
	LogStartOffset           uint64
	DurableEnd               uint64
	NextDelivery             uint64
	CommittedOrInitial       uint64
	DeliveryLag              uint64
	CommitLag                uint64
	ExpiredCommittedDistance uint64
}

// Stats returns a bounded snapshot for this single-key assignment.
func (consumer *Consumer) Stats() (ConsumerStats, error) {
	if consumer == nil || consumer.store == nil {
		return ConsumerStats{}, api.ErrClosed
	}
	consumer.operation.Lock()
	defer consumer.operation.Unlock()
	return consumer.assignmentStats(consumer.key, consumer.next, consumer.session, consumer.generation)
}

// Stats returns a bounded snapshot for one partition in this membership
// snapshot. The caller selects the partition, so the method never allocates an
// unbounded all-group diagnostic payload.
func (consumer *GroupConsumer) Stats(topic api.TopicID, partition uint32) (ConsumerStats, error) {
	if consumer == nil || consumer.store == nil {
		return ConsumerStats{}, api.ErrClosed
	}
	key := topicKey{topic: topic, partition: partition}
	consumer.store.mu.Lock()
	cursor := consumer.cursors[key]
	consumer.store.mu.Unlock()
	if cursor == nil {
		return ConsumerStats{CapturedAt: time.Now(), GroupID: consumer.groupID, Topic: topic, Partition: partition}, api.ErrInvalidArgument
	}
	cursor.operation.Lock()
	defer cursor.operation.Unlock()
	return consumer.assignmentStats(key, cursor.next, cursor.owner, consumer.generation)
}

func (consumer *Consumer) assignmentStats(key topicKey, next uint64, owner [16]byte, generation uint64) (ConsumerStats, error) {
	capturedAt := time.Now()
	stats := ConsumerStats{CapturedAt: capturedAt, GroupID: consumer.groupID, Topic: key.topic, Partition: key.partition, NextDelivery: next}
	store := consumer.store
	store.mu.Lock()
	if consumer.closed || store.closed {
		store.mu.Unlock()
		return stats, api.ErrClosed
	}
	if store.closing.Load() {
		store.mu.Unlock()
		return stats, api.ErrClosing
	}
	group := store.offsetsState.groups[consumer.groupID]
	partition := store.partitions[partitionKey{topic: key.topic, partition: key.partition}]
	active := group != nil && partition != nil && store.consumers[consumerMapKey(consumer.groupID, key)] == consumer &&
		group.Generation == generation && group.AssignmentOwner[key] == owner &&
		!consumer.expired && (consumer.deadline.IsZero() || capturedAt.Before(consumer.deadline))
	committed := uint64(0)
	if active {
		committed = latestNext(group.Progress[key])
	}
	store.mu.Unlock()
	if !active {
		return stats, nil
	}
	logStart, durableEnd, partitionActive := partitionBounds(partition)
	if !partitionActive || !assignmentStillActive(store, consumer.groupID, key, consumer, generation, owner) {
		return stats, nil
	}
	stats.LogStartOffset = logStart
	stats.DurableEnd = durableEnd
	stats.CommittedOrInitial = committed
	stats.Active = true
	stats.DeliveryLag = offsetDistance(durableEnd, next)
	stats.CommitLag = offsetDistance(durableEnd, committed)
	stats.ExpiredCommittedDistance = offsetDistance(logStart, committed)
	return stats, nil
}

func (consumer *GroupConsumer) assignmentStats(key topicKey, next uint64, owner [16]byte, generation uint64) (ConsumerStats, error) {
	capturedAt := time.Now()
	stats := ConsumerStats{CapturedAt: capturedAt, GroupID: consumer.groupID, Topic: key.topic, Partition: key.partition, NextDelivery: next}
	store := consumer.store
	store.mu.Lock()
	if consumer.closed || store.closed {
		store.mu.Unlock()
		return stats, api.ErrClosed
	}
	if store.closing.Load() {
		store.mu.Unlock()
		return stats, api.ErrClosing
	}
	group := store.offsetsState.groups[consumer.groupID]
	partition := store.partitions[partitionKey{topic: key.topic, partition: key.partition}]
	member := consumer.members[owner]
	active := group != nil && partition != nil && member != nil && store.groupConsumers[consumer.groupID] == consumer &&
		group.Generation == generation && group.AssignmentOwner[key] == owner && capturedAt.Before(member.deadline)
	committed := uint64(0)
	if active {
		committed = latestNext(group.Progress[key])
	}
	store.mu.Unlock()
	if !active {
		return stats, nil
	}
	logStart, durableEnd, partitionActive := partitionBounds(partition)
	if !partitionActive || !groupAssignmentStillActive(store, consumer.groupID, key, consumer, generation, owner) {
		return stats, nil
	}
	stats.LogStartOffset = logStart
	stats.DurableEnd = durableEnd
	stats.CommittedOrInitial = committed
	stats.Active = true
	stats.DeliveryLag = offsetDistance(durableEnd, next)
	stats.CommitLag = offsetDistance(durableEnd, committed)
	stats.ExpiredCommittedDistance = offsetDistance(logStart, committed)
	return stats, nil
}

func partitionBounds(partition *Partition) (logStart, durableEnd uint64, active bool) {
	partition.mu.RLock()
	defer partition.mu.RUnlock()
	if partition.closed || partition.closing || partition.unavailable || len(partition.segments) == 0 {
		return 0, 0, false
	}
	return partition.segments[0].header.BaseOffset, partition.logEnd, true
}

func assignmentStillActive(store *Store, groupID string, key topicKey, consumer *Consumer, generation uint64, owner [16]byte) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	group := store.offsetsState.groups[groupID]
	return !store.closed && !store.closing.Load() && !store.offsetsUnavailable && group != nil && store.consumers[consumerMapKey(groupID, key)] == consumer &&
		group.Generation == generation && group.AssignmentOwner[key] == owner && !consumer.expired &&
		(consumer.deadline.IsZero() || time.Now().Before(consumer.deadline))
}

func groupAssignmentStillActive(store *Store, groupID string, key topicKey, consumer *GroupConsumer, generation uint64, owner [16]byte) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	group := store.offsetsState.groups[groupID]
	member := consumer.members[owner]
	return !store.closed && !store.closing.Load() && !store.offsetsUnavailable && group != nil && member != nil && store.groupConsumers[groupID] == consumer &&
		group.Generation == generation && group.AssignmentOwner[key] == owner && time.Now().Before(member.deadline)
}

func offsetDistance(high, low uint64) uint64 {
	if low >= high {
		return 0
	}
	return high - low
}
