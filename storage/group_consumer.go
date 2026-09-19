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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ayeshLK/immulog/api"
)

// GroupConsumer owns one atomically installed local membership snapshot. Its
// callers supply the complete member/subscription set for every replacement;
// earlier handles are fenced instead of being silently retokened.
type GroupConsumer struct {
	store           *Store
	groupID         string
	generation      uint64
	fetch           api.FetchOptions
	progressTimeout time.Duration
	membership      canonicalGroupMembership
	members         map[[16]byte]*groupMember
	cursors         map[topicKey]*groupCursor
	closed          bool
}

type groupMember struct {
	session       [16]byte
	subscriptions []topicKey
	deadline      time.Time
}

type canonicalGroupMember struct {
	subscriptions []topicKey
}

type canonicalGroupMembership []canonicalGroupMember

type groupCursor struct {
	key       topicKey
	owner     [16]byte
	operation sync.Mutex
	next      uint64
	delivered uint64
}

type groupAssignment struct {
	key            topicKey
	owner          [16]byte
	resume         uint64
	createBaseline bool
}

// OpenConsumerGroup installs a group's complete local membership snapshot.
// An equivalent request reuses the current live snapshot without advancing its
// durable generation; a changed request creates fresh member sessions and
// fences the previous handle.
func (s *Store) OpenConsumerGroup(ctx context.Context, groupID string, members []api.ConsumerGroupMember, options api.ConsumerGroupOptions) (*GroupConsumer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := appendString(nil, groupID, maxGroupIDBytes); err != nil {
		return nil, err
	}
	if err := normalizeConsumerGroupOptions(&options); err != nil {
		return nil, err
	}
	canonical, keys, err := canonicalGroupMembers(members)
	if err != nil {
		return nil, err
	}
	starts, err := explicitStartMap(options.ExplicitStarts)
	if err != nil {
		return nil, err
	}
	if options.Start == api.GroupStartExplicit && !sameKeySet(keys, starts) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("explicit starts must cover exactly the group subscriptions"))
	}
	if options.Start != api.GroupStartExplicit && len(starts) != 0 {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("explicit starts require GroupStartExplicit"))
	}
	if err := s.acquireOffsetsAdmission(ctx); err != nil {
		return nil, err
	}
	defer s.releaseOffsetsAdmission()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, api.ErrClosed
	}
	if s.closing.Load() {
		return nil, api.ErrClosing
	}
	if s.offsetsUnavailable {
		return nil, api.ErrGroupUnavailable
	}
	group := s.offsetsState.groups[groupID]
	if current := s.groupConsumers[groupID]; current != nil {
		live, expired := s.liveGroupConsumerLocked(groupID, current, group)
		if expired {
			delete(s.groupConsumers, groupID)
			s.expiredGroups[groupID] = true
		}
		if live && groupConsumerMatches(current, group, canonical, options, starts) {
			return current, nil
		}
	}
	addGroups := uint64(0)
	if group == nil {
		addGroups = 1
	}
	addProgressKeys := uint64(0)
	for key := range keys {
		if group == nil {
			addProgressKeys++
			continue
		}
		if !groupHasProjectionKey(group, key) {
			addProgressKeys++
		}
	}
	if err := admitOffsetsProjectionGrowth(s.offsetsState, s.options, addGroups, addProgressKeys); err != nil {
		return nil, err
	}
	if group == nil && options.Start == api.GroupStartExplicit {
		for key, next := range starts {
			partition, err := s.openConsumerPartitionLocked(key)
			if err != nil {
				return nil, err
			}
			if _, err := resolveGroupStart(partition, options.Start, next); err != nil {
				return nil, err
			}
		}
	}
	if group == nil {
		body, err := encodeGroupCreatedGroupBody(s.storeID, s.catalogState.revision, groupID, options.Start, starts)
		if err != nil {
			return nil, err
		}
		if err := s.appendOffsetsEventLocked(EventGroupCreated, body); err != nil {
			return nil, s.groupAppendError(err)
		}
		group = s.offsetsState.groups[groupID]
	}
	if group.CreationStartMode != uint8(options.Start) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer start policy differs from the durable group policy"))
	}
	if options.Start == api.GroupStartExplicit && !explicitStartsMatchKeys(group.ExplicitStarts, starts) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("explicit group starts differ from the durable group policy"))
	}
	if group.Generation == ^uint64(0) {
		return nil, errors.Join(api.ErrResourceLimit, errors.New("consumer group generation is exhausted"))
	}
	prepared, err := prepareGroupMembers(canonical)
	if err != nil {
		return nil, err
	}
	assignments := make([]groupAssignment, 0, len(keys))
	cursors := make(map[topicKey]*groupCursor, len(keys))
	for _, member := range prepared {
		for _, key := range member.subscriptions {
			partition, err := s.openConsumerPartitionLocked(key)
			if err != nil {
				return nil, err
			}
			progress, exists := group.Progress[key]
			var resume uint64
			if exists {
				resume = latestNext(progress)
				if err := partition.validateReadOffset(resume); err != nil {
					return nil, err
				}
			} else if options.Start == api.GroupStartExplicit {
				resume, err = resolveGroupStart(partition, options.Start, group.ExplicitStarts[key])
				if err != nil {
					return nil, err
				}
			} else {
				resume, err = resolveGroupStart(partition, options.Start, 0)
				if err != nil {
					return nil, err
				}
			}
			assignments = append(assignments, groupAssignment{key: key, owner: member.session, resume: resume, createBaseline: !exists})
			cursors[key] = &groupCursor{key: key, owner: member.session, next: resume, delivered: resume}
		}
	}
	reason := byte(1)
	if s.expiredGroups[groupID] {
		reason = 4
		delete(s.expiredGroups, groupID)
	} else if s.groupConsumers[groupID] != nil {
		reason = 3
	} else if group.Generation != 0 {
		reason = 5
	}
	body, err := encodeGroupAssignmentBody(s.storeID, s.catalogState.revision, groupID, group.Generation, reason, prepared, assignments)
	if err != nil {
		return nil, err
	}
	if err := s.appendOffsetsEventLocked(EventLocalAssignmentChanged, body); err != nil {
		return nil, s.groupAppendError(err)
	}
	group = s.offsetsState.groups[groupID]
	memberState := make(map[[16]byte]*groupMember, len(prepared))
	deadline := time.Now().Add(options.ProgressTimeout)
	for _, member := range prepared {
		member.deadline = deadline
		memberState[member.session] = &member
	}
	consumer := &GroupConsumer{store: s, groupID: groupID, generation: group.Generation, fetch: options.Fetch, progressTimeout: options.ProgressTimeout, membership: canonical, members: memberState, cursors: cursors}
	s.groupConsumers[groupID] = consumer
	return consumer, nil
}

func (s *Store) liveGroupConsumerLocked(groupID string, consumer *GroupConsumer, group *offsetGroup) (live, expired bool) {
	if consumer == nil || consumer.closed || s.groupConsumers[groupID] != consumer || group == nil || group.Generation != consumer.generation || group.AssignmentInstance != [16]byte(s.storeID) {
		return false, false
	}
	now := time.Now()
	for _, member := range consumer.members {
		if !now.Before(member.deadline) {
			return false, true
		}
	}
	if len(group.AssignmentMember) != len(consumer.members) || len(group.AssignmentOwner) != len(consumer.cursors) {
		return false, false
	}
	for session := range consumer.members {
		if _, exists := group.AssignmentMember[session]; !exists {
			return false, false
		}
	}
	for key, cursor := range consumer.cursors {
		if owner, exists := group.AssignmentOwner[key]; !exists || owner != cursor.owner {
			return false, false
		}
	}
	return true, false
}

func groupConsumerMatches(consumer *GroupConsumer, group *offsetGroup, membership canonicalGroupMembership, options api.ConsumerGroupOptions, starts map[topicKey]uint64) bool {
	return equalCanonicalGroupMembers(consumer.membership, membership) &&
		consumer.fetch == options.Fetch &&
		consumer.progressTimeout == options.ProgressTimeout &&
		group.CreationStartMode == uint8(options.Start) &&
		equalExplicitStarts(group.ExplicitStarts, starts)
}

func equalExplicitStarts(left, right map[topicKey]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, exists := right[key]; !exists || other != value {
			return false
		}
	}
	return true
}

// Poll fetches one explicitly subscribed partition under this group snapshot.
func (consumer *GroupConsumer) Poll(ctx context.Context, topic api.TopicID, partitionID uint32, options api.FetchOptions) (api.FetchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := topicKey{topic: topic, partition: partitionID}
	cursor, err := consumer.cursor(key)
	if err != nil {
		return api.FetchResult{}, err
	}
	if !cursor.operation.TryLock() {
		return api.FetchResult{}, api.ErrConcurrentOperation
	}
	defer cursor.operation.Unlock()
	if err := ctx.Err(); err != nil {
		return api.FetchResult{NextOffset: cursor.next}, err
	}
	if options.MaxRecords == 0 && options.MaxBytes == 0 {
		if options.MaxWait != 0 {
			wait := options.MaxWait
			options = consumer.fetch
			options.MaxWait = wait
		} else {
			options = consumer.fetch
		}
	}
	options, err = normalizeFetchOptions(options)
	if err != nil {
		return api.FetchResult{NextOffset: cursor.next}, err
	}
	if options.MaxWait != 0 && options.MaxWait >= consumer.progressTimeout {
		return api.FetchResult{NextOffset: cursor.next}, errors.Join(api.ErrInvalidArgument, fmt.Errorf("poll wait %s must be below the progress timeout %s", options.MaxWait, consumer.progressTimeout))
	}
	consumer.store.mu.Lock()
	partition, err := consumer.activeCursorLocked(cursor)
	if err == nil {
		consumer.members[cursor.owner].deadline = time.Now().Add(consumer.progressTimeout)
	}
	consumer.store.mu.Unlock()
	if err != nil {
		return api.FetchResult{NextOffset: cursor.next}, err
	}
	result, err := partition.Fetch(ctx, cursor.next, options)
	if err != nil {
		return result, err
	}
	if len(result.Records) == 0 && options.MaxWait != 0 {
		if err := partition.waitForData(ctx, cursor.next, options.MaxWait); err != nil {
			return api.FetchResult{NextOffset: cursor.next}, err
		}
		result, err = partition.Fetch(ctx, cursor.next, options)
		if err != nil {
			return result, err
		}
	}
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	if _, err := consumer.activeCursorLocked(cursor); err != nil {
		return api.FetchResult{NextOffset: cursor.next}, err
	}
	cursor.next = result.NextOffset
	cursor.delivered = result.NextOffset
	return result, nil
}

// Commit persists a next offset for one delivered partition prefix.
func (consumer *GroupConsumer) Commit(ctx context.Context, topic api.TopicID, partitionID uint32, next uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	key := topicKey{topic: topic, partition: partitionID}
	cursor, err := consumer.cursor(key)
	if err != nil {
		return err
	}
	if !cursor.operation.TryLock() {
		return api.ErrConcurrentOperation
	}
	defer cursor.operation.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := consumer.store.acquireOffsetsAdmission(ctx); err != nil {
		return err
	}
	defer consumer.store.releaseOffsetsAdmission()
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	partition, err := consumer.activeCursorLocked(cursor)
	if err != nil {
		return err
	}
	group := consumer.store.offsetsState.groups[consumer.groupID]
	progress := group.Progress[key]
	current := latestNext(progress)
	if next < current {
		return api.ErrCommitRegression
	}
	if next > cursor.delivered {
		return api.ErrInvalidCommit
	}
	end, err := partition.EndOffset()
	if err != nil {
		return err
	}
	if next > end {
		return api.ErrInvalidCommit
	}
	if progress.HasCommit && next == current {
		return nil
	}
	body, err := encodeCommitBody(consumer.store.storeID, consumer.store.catalogState.revision, consumer.groupID, cursor.owner, consumer.generation, key, progress, next)
	if err != nil {
		return err
	}
	if err := consumer.store.appendOffsetsEventLocked(EventOffsetCommitted, body); err != nil {
		return consumer.store.commitAppendError(err)
	}
	return nil
}

// Close durably revokes the complete membership snapshot without committing any
// delivered records. A replacement snapshot must be opened explicitly.
func (consumer *GroupConsumer) Close() error {
	if err := consumer.store.acquireOffsetsAdmission(context.Background()); err != nil {
		return err
	}
	defer consumer.store.releaseOffsetsAdmission()
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	if consumer.closed || consumer.store.closed || consumer.store.groupConsumers[consumer.groupID] != consumer {
		consumer.closed = true
		return nil
	}
	if consumer.store.offsetsUnavailable {
		consumer.closed = true
		delete(consumer.store.groupConsumers, consumer.groupID)
		return api.ErrGroupUnavailable
	}
	group := consumer.store.offsetsState.groups[consumer.groupID]
	if group == nil || group.Generation != consumer.generation || group.Generation == ^uint64(0) {
		consumer.closed = true
		delete(consumer.store.groupConsumers, consumer.groupID)
		return nil
	}
	body, err := encodeEmptyAssignmentBody(consumer.store.storeID, consumer.store.catalogState.revision, consumer.groupID, group.Generation)
	if err == nil {
		err = consumer.store.appendOffsetsEventLocked(EventLocalAssignmentChanged, body)
	}
	consumer.closed = true
	delete(consumer.store.groupConsumers, consumer.groupID)
	if err != nil {
		return consumer.store.groupAppendError(err)
	}
	return nil
}

// Subscriptions returns caller-owned keys currently assigned to this snapshot.
func (consumer *GroupConsumer) Subscriptions() []api.TopicPartition {
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	keys := make([]topicKey, 0, len(consumer.cursors))
	for key := range consumer.cursors {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool { return compareTopicKey(keys[left], keys[right]) < 0 })
	result := make([]api.TopicPartition, len(keys))
	for index, key := range keys {
		result[index] = api.TopicPartition{Topic: key.topic, Partition: key.partition}
	}
	return result
}

func (consumer *GroupConsumer) cursor(key topicKey) (*groupCursor, error) {
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	if consumer.closed {
		return nil, api.ErrClosed
	}
	cursor := consumer.cursors[key]
	if cursor == nil {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("partition is not assigned to this consumer group"))
	}
	return cursor, nil
}

func (consumer *GroupConsumer) activeCursorLocked(cursor *groupCursor) (*Partition, error) {
	if consumer.closed || consumer.store.closed {
		return nil, api.ErrClosed
	}
	if consumer.store.closing.Load() {
		return nil, api.ErrClosing
	}
	if consumer.store.offsetsUnavailable {
		consumer.store.expiredGroups[consumer.groupID] = true
		return nil, api.ErrGroupUnavailable
	}
	if consumer.store.groupConsumers[consumer.groupID] != consumer {
		return nil, assignmentLost("live consumer was replaced")
	}
	now := time.Now()
	for _, member := range consumer.members {
		if !now.Before(member.deadline) {
			delete(consumer.store.groupConsumers, consumer.groupID)
			consumer.store.expiredGroups[consumer.groupID] = true
			return nil, assignmentLost("member progress deadline expired")
		}
	}
	group := consumer.store.offsetsState.groups[consumer.groupID]
	if group == nil {
		return nil, assignmentLost("durable group assignment is missing")
	}
	if group.Generation != consumer.generation {
		return nil, assignmentLost("durable group generation changed")
	}
	if group.AssignmentInstance != [16]byte(consumer.store.storeID) {
		return nil, assignmentLost("durable assignment belongs to another store instance")
	}
	if group.AssignmentOwner[cursor.key] != cursor.owner {
		return nil, assignmentLost("cursor assignment owner changed")
	}
	partition := consumer.store.partitions[partitionKey{topic: cursor.key.topic, partition: cursor.key.partition}]
	if partition == nil {
		return nil, assignmentLost("assigned partition is unavailable")
	}
	return partition, nil
}

func assignmentLost(reason string) error {
	return fmt.Errorf("%w: %s", api.ErrAssignmentLost, reason)
}

func normalizeConsumerGroupOptions(options *api.ConsumerGroupOptions) error {
	if options.Start == 0 {
		options.Start = api.GroupStartEarliest
	}
	if options.Start != api.GroupStartEarliest && options.Start != api.GroupStartLatest && options.Start != api.GroupStartExplicit {
		return errors.Join(api.ErrInvalidArgument, errors.New("consumer group start policy is invalid"))
	}
	if options.ProgressTimeout == 0 {
		options.ProgressTimeout = defaultConsumerProgressTimeout
	}
	if options.ProgressTimeout < minConsumerProgressTimeout || options.ProgressTimeout > maxConsumerProgressTimeout {
		return errors.Join(api.ErrInvalidArgument, fmt.Errorf("consumer group progress timeout %s is outside [%s, %s]", options.ProgressTimeout, minConsumerProgressTimeout, maxConsumerProgressTimeout))
	}
	fetch, err := normalizeFetchOptions(options.Fetch)
	if err != nil {
		return err
	}
	options.Fetch = fetch
	return nil
}

func canonicalGroupMembers(specs []api.ConsumerGroupMember) (canonicalGroupMembership, map[topicKey]struct{}, error) {
	if len(specs) == 0 || len(specs) > maxAssignmentMembers {
		return nil, nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer group member count is outside limits"))
	}
	members := make(canonicalGroupMembership, len(specs))
	keys := make(map[topicKey]struct{})
	for index, spec := range specs {
		member := canonicalGroupMember{subscriptions: make([]topicKey, len(spec.Subscriptions))}
		for subIndex, subscription := range spec.Subscriptions {
			if subscription.Topic.IsZero() || subscription.Topic == api.ClusterMetadataTopicID || subscription.Topic == api.ConsumerOffsetsTopicID || subscription.Partition > int32Max {
				return nil, nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer group subscription is invalid"))
			}
			member.subscriptions[subIndex] = topicKey{topic: subscription.Topic, partition: subscription.Partition}
		}
		sort.Slice(member.subscriptions, func(left, right int) bool {
			return compareTopicKey(member.subscriptions[left], member.subscriptions[right]) < 0
		})
		for subIndex, key := range member.subscriptions {
			if subIndex > 0 && compareTopicKey(member.subscriptions[subIndex-1], key) == 0 {
				return nil, nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer group member has duplicate subscriptions"))
			}
			if _, exists := keys[key]; exists {
				return nil, nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer group subscriptions overlap"))
			}
			keys[key] = struct{}{}
			if len(keys) > maxSubscriptionKeys {
				return nil, nil, errors.Join(api.ErrResourceLimit, errors.New("consumer group subscription count exceeds limit"))
			}
		}
		members[index] = member
	}
	sort.Slice(members, func(left, right int) bool {
		return compareCanonicalGroupMember(members[left], members[right]) < 0
	})
	return members, keys, nil
}

func prepareGroupMembers(canonical canonicalGroupMembership) ([]groupMember, error) {
	members := make([]groupMember, len(canonical))
	for index, spec := range canonical {
		session, err := newConsumerSession()
		if err != nil {
			return nil, err
		}
		members[index] = groupMember{session: session, subscriptions: append([]topicKey(nil), spec.subscriptions...)}
	}
	sort.Slice(members, func(left, right int) bool {
		return string(members[left].session[:]) < string(members[right].session[:])
	})
	for index := 1; index < len(members); index++ {
		if members[index-1].session == members[index].session {
			return nil, errors.Join(api.ErrResourceLimit, errors.New("consumer group session ID collision"))
		}
	}
	return members, nil
}

func compareCanonicalGroupMember(left, right canonicalGroupMember) int {
	for index := 0; index < len(left.subscriptions) && index < len(right.subscriptions); index++ {
		if comparison := compareTopicKey(left.subscriptions[index], right.subscriptions[index]); comparison != 0 {
			return comparison
		}
	}
	if len(left.subscriptions) < len(right.subscriptions) {
		return -1
	}
	if len(left.subscriptions) > len(right.subscriptions) {
		return 1
	}
	return 0
}

func equalCanonicalGroupMembers(left, right canonicalGroupMembership) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if compareCanonicalGroupMember(left[index], right[index]) != 0 {
			return false
		}
	}
	return true
}

func explicitStartMap(starts []api.ExplicitStart) (map[topicKey]uint64, error) {
	result := make(map[topicKey]uint64, len(starts))
	for _, start := range starts {
		if start.Topic.IsZero() || start.Topic == api.ClusterMetadataTopicID || start.Topic == api.ConsumerOffsetsTopicID || start.Partition > int32Max {
			return nil, errors.Join(api.ErrInvalidArgument, errors.New("explicit consumer start is invalid"))
		}
		key := topicKey{topic: start.Topic, partition: start.Partition}
		if _, exists := result[key]; exists {
			return nil, errors.Join(api.ErrInvalidArgument, errors.New("duplicate explicit consumer start"))
		}
		result[key] = start.Next
	}
	return result, nil
}

func sameKeySet(keys map[topicKey]struct{}, starts map[topicKey]uint64) bool {
	if len(keys) != len(starts) {
		return false
	}
	for key := range keys {
		if _, exists := starts[key]; !exists {
			return false
		}
	}
	return true
}

func explicitStartsMatchKeys(stored, current map[topicKey]uint64) bool {
	for key, value := range current {
		if other, exists := stored[key]; !exists || other != value {
			return false
		}
	}
	return true
}

func resolveGroupStart(partition *Partition, start api.GroupStart, explicit uint64) (uint64, error) {
	options := api.ConsumerOptions{Start: start, ExplicitStart: explicit}
	return resolveConsumerStart(partition, options)
}

func encodeGroupCreatedGroupBody(storeID StoreID, catalogNext uint64, groupID string, start api.GroupStart, starts map[topicKey]uint64) ([]byte, error) {
	body := append([]byte(nil), storeID[:]...)
	body = appendU64(body, catalogNext)
	var err error
	body, err = appendString(body, groupID, maxGroupIDBytes)
	if err != nil {
		return nil, err
	}
	body = append(body, byte(start), 0, 0, 0)
	keys := make([]topicKey, 0, len(starts))
	for key := range starts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool { return compareTopicKey(keys[left], keys[right]) < 0 })
	body = appendEventU32(body, uint32(len(keys)))
	for _, key := range keys {
		body = appendID(body, [16]byte(key.topic))
		body = appendEventU32(body, key.partition)
		body = appendU64(body, starts[key])
	}
	return body, nil
}

func encodeGroupAssignmentBody(storeID StoreID, catalogNext uint64, groupID string, generation uint64, reason byte, members []groupMember, assignments []groupAssignment) ([]byte, error) {
	body := append([]byte(nil), storeID[:]...)
	body = appendU64(body, catalogNext)
	var err error
	body, err = appendString(body, groupID, maxGroupIDBytes)
	if err != nil {
		return nil, err
	}
	body = appendID(body, [16]byte(storeID))
	body = appendU64(body, generation)
	body = appendU64(body, generation+1)
	body = append(body, reason, 0, 0, 0)
	body = appendEventU32(body, uint32(len(members)))
	for _, member := range members {
		body = appendID(body, member.session)
		body = appendEventU32(body, uint32(len(member.subscriptions)))
		for _, key := range member.subscriptions {
			body = appendID(body, [16]byte(key.topic))
			body = appendEventU32(body, key.partition)
		}
	}
	sort.Slice(assignments, func(left, right int) bool { return compareTopicKey(assignments[left].key, assignments[right].key) < 0 })
	body = appendEventU32(body, uint32(len(assignments)))
	for _, assignment := range assignments {
		body = appendID(body, [16]byte(assignment.key.topic))
		body = appendEventU32(body, assignment.key.partition)
		body = appendID(body, assignment.owner)
		body = appendU64(body, assignment.resume)
		if assignment.createBaseline {
			body = append(body, 1, 0, 0, 0)
			body = appendU64(body, assignment.resume)
		} else {
			body = append(body, 0, 0, 0, 0)
			body = appendU64(body, 0)
		}
	}
	return body, nil
}
