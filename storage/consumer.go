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
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayeshLK/immulog/api"
)

const (
	defaultConsumerProgressTimeout = 30 * time.Second
	minConsumerProgressTimeout     = time.Millisecond
	maxConsumerProgressTimeout     = 24 * time.Hour
)

var consumerAdmissionEpoch = time.Now()

// consumerAdmission keeps a lease alive while an operation waits to acquire
// store.mu. The earliest start is retained so a call that began before expiry
// cannot be mistaken for a newly started idle operation.
type consumerAdmission struct {
	mu       sync.Mutex
	pending  []int64
	earliest atomic.Int64
}

func (admission *consumerAdmission) begin() int64 {
	started := time.Since(consumerAdmissionEpoch).Nanoseconds()
	admission.mu.Lock()
	admission.pending = append(admission.pending, started)
	earliest := admission.earliest.Load()
	if earliest == 0 || started < earliest {
		admission.earliest.Store(started)
	}
	admission.mu.Unlock()
	return started
}

func (admission *consumerAdmission) end(started int64) {
	admission.mu.Lock()
	for index, pending := range admission.pending {
		if pending == started {
			admission.pending = append(admission.pending[:index], admission.pending[index+1:]...)
			break
		}
	}
	var earliest int64
	for _, pending := range admission.pending {
		if earliest == 0 || pending < earliest {
			earliest = pending
		}
	}
	admission.earliest.Store(earliest)
	admission.mu.Unlock()
}

func (admission *consumerAdmission) startedBefore(deadline time.Time) bool {
	started := admission.earliest.Load()
	return started != 0 && started < deadline.Sub(consumerAdmissionEpoch).Nanoseconds()
}

// Consumer owns one same-process group assignment for one topic partition. A
// later OpenConsumer for that key durably fences this handle before activating
// its replacement. An active Poll or synchronous Commit keeps its progress
// lease alive; an idle handle eventually expires. Consumers provide
// at-least-once delivery, not exclusive application processing.
type Consumer struct {
	store           *Store
	groupID         string
	key             topicKey
	session         [16]byte
	generation      uint64
	fetch           api.FetchOptions
	progressTimeout time.Duration

	operation       sync.Mutex
	admission       consumerAdmission
	next            uint64
	delivered       uint64
	deadline        time.Time
	operationActive bool
	expired         bool
	closed          bool
}

// OpenConsumer durably creates or reassigns a local group key. Start is
// persisted on first use, and its resolved offset is persisted before any
// managed records are returned.
func (s *Store) OpenConsumer(ctx context.Context, groupID string, topic api.TopicID, partition uint32, options api.ConsumerOptions) (*Consumer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := appendString(nil, groupID, maxGroupIDBytes); err != nil {
		return nil, err
	}
	if topic.IsZero() || topic == api.ClusterMetadataTopicID || topic == api.ConsumerOffsetsTopicID || partition > int32Max {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer topic partition is invalid"))
	}
	if options.Start == 0 {
		options.Start = api.GroupStartEarliest
	}
	if options.ProgressTimeout == 0 {
		options.ProgressTimeout = defaultConsumerProgressTimeout
	}
	if options.ProgressTimeout < minConsumerProgressTimeout || options.ProgressTimeout > maxConsumerProgressTimeout {
		return nil, errors.Join(api.ErrInvalidArgument, fmt.Errorf("consumer progress timeout %s is outside [%s, %s]", options.ProgressTimeout, minConsumerProgressTimeout, maxConsumerProgressTimeout))
	}
	if options.Start != api.GroupStartEarliest && options.Start != api.GroupStartLatest && options.Start != api.GroupStartExplicit {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer start policy is invalid"))
	}
	if options.Start != api.GroupStartExplicit && options.ExplicitStart != 0 {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("explicit start requires GroupStartExplicit"))
	}
	if _, err := normalizeFetchOptions(options.Fetch); err != nil {
		return nil, err
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
	key := topicKey{topic: topic, partition: partition}
	user, err := s.openConsumerPartitionLocked(key)
	if err != nil {
		return nil, err
	}
	group := s.offsetsState.groups[groupID]
	addGroups, addProgressKeys := uint64(0), uint64(0)
	if group == nil {
		addGroups, addProgressKeys = 1, 1
	} else if !groupHasProjectionKey(group, key) {
		addProgressKeys = 1
	}
	if err := admitOffsetsProjectionGrowth(s.offsetsState, s.options, addGroups, addProgressKeys); err != nil {
		return nil, err
	}
	if group == nil {
		initial, err := resolveConsumerStart(user, options)
		if err != nil {
			return nil, err
		}
		body, err := encodeGroupCreatedBody(s.storeID, s.catalogState.revision, groupID, options.Start, key, initial)
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
	progress, exists := group.Progress[key]
	var resume uint64
	if exists {
		resume = latestNext(progress)
		if err := user.validateReadOffset(resume); err != nil {
			return nil, err
		}
	} else {
		if options.Start == api.GroupStartExplicit {
			stored, ok := group.ExplicitStarts[key]
			if !ok || stored != options.ExplicitStart {
				return nil, errors.Join(api.ErrInvalidArgument, errors.New("explicit group start was not persisted for this partition"))
			}
			resume = stored
		} else {
			resume, err = resolveConsumerStart(user, options)
			if err != nil {
				return nil, err
			}
		}
	}
	if group.Generation == ^uint64(0) {
		return nil, errors.Join(api.ErrResourceLimit, errors.New("consumer group generation is exhausted"))
	}
	session, err := newConsumerSession()
	if err != nil {
		return nil, err
	}
	reason := byte(1)
	if s.expiredGroups[groupID] {
		reason = 4
		delete(s.expiredGroups, groupID)
	} else if s.consumers[consumerMapKey(groupID, key)] != nil || s.groupConsumers[groupID] != nil {
		reason = 3
	} else if group.Generation != 0 {
		reason = 5
	}
	body, err := encodeAssignmentBody(s.storeID, s.catalogState.revision, groupID, session, group.Generation, reason, key, resume, !exists)
	if err != nil {
		return nil, err
	}
	if err := s.appendOffsetsEventLocked(EventLocalAssignmentChanged, body); err != nil {
		return nil, s.groupAppendError(err)
	}
	group = s.offsetsState.groups[groupID]
	consumer := &Consumer{store: s, groupID: groupID, key: key, session: session, generation: group.Generation, fetch: options.Fetch, progressTimeout: options.ProgressTimeout, deadline: time.Now().Add(options.ProgressTimeout), next: resume, delivered: resume}
	s.consumers[consumerMapKey(groupID, key)] = consumer
	return consumer, nil
}

// Poll returns a bounded durable-record prefix and advances the live delivery
// cursor only after the result passes the current assignment fence.
func (consumer *Consumer) Poll(ctx context.Context, options api.FetchOptions) (api.FetchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !consumer.operation.TryLock() {
		return api.FetchResult{}, api.ErrConcurrentOperation
	}
	defer consumer.operation.Unlock()
	if err := ctx.Err(); err != nil {
		return api.FetchResult{NextOffset: consumer.next}, err
	}
	admission := consumer.admission.begin()
	defer consumer.admission.end(admission)
	if options.MaxRecords == 0 && options.MaxBytes == 0 {
		if options.MaxWait != 0 {
			wait := options.MaxWait
			options = consumer.fetch
			options.MaxWait = wait
		} else {
			options = consumer.fetch
		}
	}
	options, err := normalizeFetchOptions(options)
	if err != nil {
		return api.FetchResult{NextOffset: consumer.next}, err
	}
	if options.MaxWait != 0 && options.MaxWait >= consumer.progressTimeout {
		return api.FetchResult{NextOffset: consumer.next}, errors.Join(api.ErrInvalidArgument, fmt.Errorf("poll wait %s must be below the progress timeout %s", options.MaxWait, consumer.progressTimeout))
	}
	consumer.store.mu.Lock()
	partition, err := consumer.beginOperationLocked()
	consumer.store.mu.Unlock()
	if err != nil {
		return api.FetchResult{NextOffset: consumer.next}, err
	}
	defer func() {
		consumer.store.mu.Lock()
		consumer.endOperationLocked()
		consumer.store.mu.Unlock()
	}()
	result, err := partition.Fetch(ctx, consumer.next, options)
	if err != nil {
		return result, err
	}
	if len(result.Records) == 0 && options.MaxWait != 0 {
		if err := partition.waitForData(ctx, consumer.next, options.MaxWait); err != nil {
			return api.FetchResult{NextOffset: consumer.next}, err
		}
		result, err = partition.Fetch(ctx, consumer.next, options)
		if err != nil {
			return result, err
		}
	}
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	if _, err := consumer.activePartitionLocked(); err != nil {
		return api.FetchResult{NextOffset: consumer.next}, err
	}
	consumer.next = result.NextOffset
	consumer.delivered = result.NextOffset
	return result, nil
}

// Commit synchronously persists a next offset inside the currently delivered
// prefix. It never advances progress for a fenced assignment.
func (consumer *Consumer) Commit(ctx context.Context, next uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !consumer.operation.TryLock() {
		return api.ErrConcurrentOperation
	}
	defer consumer.operation.Unlock()
	admission := consumer.admission.begin()
	defer consumer.admission.end(admission)
	if err := consumer.store.acquireOffsetsAdmission(ctx); err != nil {
		return err
	}
	defer consumer.store.releaseOffsetsAdmission()
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	partition, err := consumer.beginOperationLocked()
	if err != nil {
		return err
	}
	defer consumer.endOperationLocked()
	group := consumer.store.offsetsState.groups[consumer.groupID]
	progress := group.Progress[consumer.key]
	current := latestNext(progress)
	if next < current {
		return api.ErrCommitRegression
	}
	if next > consumer.delivered {
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
	body, err := encodeCommitBody(consumer.store.storeID, consumer.store.catalogState.revision, consumer.groupID, consumer.session, consumer.generation, consumer.key, progress, next)
	if err != nil {
		return err
	}
	if err := consumer.store.appendOffsetsEventLocked(EventOffsetCommitted, body); err != nil {
		return consumer.store.commitAppendError(err)
	}
	return nil
}

// NextOffset returns the next offset this assignment will request. It does not
// reveal a durable commit position.
func (consumer *Consumer) NextOffset() (uint64, error) {
	consumer.operation.Lock()
	defer consumer.operation.Unlock()
	if consumer.closed {
		return 0, api.ErrClosed
	}
	return consumer.next, nil
}

// Close durably revokes this active local assignment before withdrawing the
// handle. It never auto-commits delivered records.
func (consumer *Consumer) Close() error {
	consumer.operation.Lock()
	defer consumer.operation.Unlock()
	if consumer.closed {
		return nil
	}
	if err := consumer.store.acquireOffsetsAdmission(context.Background()); err != nil {
		return err
	}
	defer consumer.store.releaseOffsetsAdmission()
	consumer.store.mu.Lock()
	defer consumer.store.mu.Unlock()
	if consumer.store.closed || consumer.store.consumers[consumerMapKey(consumer.groupID, consumer.key)] != consumer {
		consumer.closed = true
		delete(consumer.store.consumers, consumerMapKey(consumer.groupID, consumer.key))
		return nil
	}
	if consumer.store.offsetsUnavailable {
		consumer.closed = true
		delete(consumer.store.consumers, consumerMapKey(consumer.groupID, consumer.key))
		return api.ErrGroupUnavailable
	}
	group := consumer.store.offsetsState.groups[consumer.groupID]
	if group == nil || group.Generation != consumer.generation || group.AssignmentOwner[consumer.key] != consumer.session || group.Generation == ^uint64(0) {
		consumer.closed = true
		delete(consumer.store.consumers, consumerMapKey(consumer.groupID, consumer.key))
		return nil
	}
	body, err := encodeEmptyAssignmentBody(consumer.store.storeID, consumer.store.catalogState.revision, consumer.groupID, group.Generation)
	if err == nil {
		err = consumer.store.appendOffsetsEventLocked(EventLocalAssignmentChanged, body)
	}
	consumer.closed = true
	delete(consumer.store.consumers, consumerMapKey(consumer.groupID, consumer.key))
	if err != nil {
		return consumer.store.groupAppendError(err)
	}
	return nil
}

func (consumer *Consumer) beginOperationLocked() (*Partition, error) {
	partition, err := consumer.activePartitionLocked()
	if err != nil {
		return nil, err
	}
	consumer.operationActive = true
	consumer.deadline = time.Now().Add(consumer.progressTimeout)
	return partition, nil
}

func (consumer *Consumer) endOperationLocked() {
	consumer.operationActive = false
	if !consumer.closed && !consumer.store.closed && consumer.store.consumers[consumerMapKey(consumer.groupID, consumer.key)] == consumer {
		consumer.deadline = time.Now().Add(consumer.progressTimeout)
	}
}

func (consumer *Consumer) activePartitionLocked() (*Partition, error) {
	if consumer.closed || consumer.store.closed {
		return nil, api.ErrClosed
	}
	if consumer.store.closing.Load() {
		return nil, api.ErrClosing
	}
	if consumer.expired || (!consumer.operationActive && !consumer.deadline.IsZero() && !time.Now().Before(consumer.deadline) && !consumer.admission.startedBefore(consumer.deadline)) {
		consumer.expired = true
		consumer.store.expiredGroups[consumer.groupID] = true
		delete(consumer.store.consumers, consumerMapKey(consumer.groupID, consumer.key))
		return nil, api.ErrAssignmentLost
	}
	if consumer.store.offsetsUnavailable {
		return nil, api.ErrGroupUnavailable
	}
	if consumer.store.consumers[consumerMapKey(consumer.groupID, consumer.key)] != consumer {
		return nil, api.ErrAssignmentLost
	}
	group := consumer.store.offsetsState.groups[consumer.groupID]
	if group == nil || group.Generation != consumer.generation || group.AssignmentInstance != [16]byte(consumer.store.storeID) || group.AssignmentOwner[consumer.key] != consumer.session {
		return nil, api.ErrAssignmentLost
	}
	partition := consumer.store.partitions[partitionKey{topic: consumer.key.topic, partition: consumer.key.partition}]
	if partition == nil {
		return nil, api.ErrAssignmentLost
	}
	return partition, nil
}

func (s *Store) openConsumerPartitionLocked(key topicKey) (*Partition, error) {
	topic := s.catalogState.topicsByID[key.topic]
	if topic == nil {
		return nil, api.ErrUnknownTopic
	}
	if key.partition >= uint32(len(topic.descriptor.Partitions)) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("consumer partition is not in the catalog topic"))
	}
	partitionKey := partitionKey{topic: key.topic, partition: key.partition}
	if existing := s.partitions[partitionKey]; existing != nil {
		return existing, nil
	}
	if err := admitOpenPartitions(len(s.partitions), 1, s.options.MaxOpenPartitions); err != nil {
		return nil, err
	}
	partitionDir := filepath.Join(s.rootPath, "topics", key.topic.String(), fmt.Sprintf("%d", key.partition))
	if err := osMkdirAndSyncPartition(partitionDir); err != nil {
		return nil, err
	}
	descriptor := topic.descriptor.Partitions[key.partition]
	options := descriptor.Config.options()
	options.InitialOffset = descriptor.RetainedL
	opened, err := openPartition(partitionDir, key.topic, key.partition, options, s.storeID)
	if err != nil {
		return nil, err
	}
	if err := validateRetainedAnchor(opened, descriptor); err != nil {
		_ = opened.Close()
		return nil, err
	}
	opened.store = s
	s.attachTailBudget(opened)
	s.partitions[partitionKey] = opened
	return opened, nil
}

func osMkdirAndSyncPartition(partitionDir string) error {
	if err := fsMkdirAll(partitionDir, 0o755); err != nil {
		return fmt.Errorf("create partition directory: %w", err)
	}
	if err := syncDir(filepath.Dir(partitionDir)); err != nil {
		return fmt.Errorf("sync topic directory: %w", err)
	}
	return syncDir(partitionDir)
}

func resolveConsumerStart(partition *Partition, options api.ConsumerOptions) (uint64, error) {
	partition.mu.RLock()
	defer partition.mu.RUnlock()
	if partition.closed {
		return 0, api.ErrClosed
	}
	if partition.closing {
		return 0, api.ErrClosing
	}
	if partition.unavailable {
		return 0, api.ErrPartitionUnavailable
	}
	if len(partition.segments) == 0 {
		return 0, api.ErrOffsetOutOfRange
	}
	switch options.Start {
	case api.GroupStartEarliest:
		return partition.segments[0].header.BaseOffset, nil
	case api.GroupStartLatest:
		return partition.logEnd, nil
	case api.GroupStartExplicit:
		if options.ExplicitStart < partition.segments[0].header.BaseOffset || options.ExplicitStart > partition.logEnd {
			return 0, api.ErrOffsetOutOfRange
		}
		return options.ExplicitStart, nil
	default:
		return 0, errors.Join(api.ErrInvalidArgument, errors.New("consumer start policy is invalid"))
	}
}

func (s *Store) acquireOffsetsAdmission(ctx context.Context) error {
	if s.closing.Load() {
		return api.ErrClosing
	}
	select {
	case s.offsetsAdmission <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Store) releaseOffsetsAdmission() { <-s.offsetsAdmission }

func (s *Store) appendOffsetsEventLocked(eventType EventType, body []byte) error {
	value, err := encodeSystemEvent(eventType, body)
	if err != nil {
		return err
	}
	base, err := s.offsets.EndOffset()
	if err != nil {
		return err
	}
	s.offsets.mu.RLock()
	err = systemEventAdmission(s.offsets, api.ConsumerOffsetsTopicID, base, value, s.options.MaxOffsetsHistoryBytes)
	s.offsets.mu.RUnlock()
	if err != nil {
		return err
	}
	batch := api.RecordBatch{
		Topic: api.ConsumerOffsetsTopicID, Partition: 0, BaseOffset: base,
		Records: []api.Record{{Topic: api.ConsumerOffsetsTopicID, Partition: 0, Offset: base, Value: value}},
	}
	appendOffsets := s.offsets.AppendBatch
	if s.offsetsAppend != nil {
		appendOffsets = s.offsetsAppend
	}
	_, err = appendOffsets(batch)
	if err != nil {
		return err
	}
	if err := s.offsetsState.apply(api.Record{Topic: api.ConsumerOffsetsTopicID, Partition: 0, Offset: base, Value: value}, s.catalogState); err != nil {
		s.offsetsUnavailable = true
		return errors.Join(api.ErrGroupUnavailable, err)
	}
	return nil
}

func (s *Store) groupAppendError(err error) error {
	if errors.Is(err, api.ErrAppendOutcomeUnknown) {
		s.offsetsUnavailable = true
		return errors.Join(api.ErrGroupUnavailable, err)
	}
	if errors.Is(err, api.ErrPartitionUnavailable) || errors.Is(err, api.ErrGroupUnavailable) {
		s.offsetsUnavailable = true
		return errors.Join(api.ErrGroupUnavailable, err)
	}
	return err
}

func (s *Store) commitAppendError(err error) error {
	if errors.Is(err, api.ErrAppendOutcomeUnknown) {
		s.offsetsUnavailable = true
		return errors.Join(api.ErrCommitOutcomeUnknown, err)
	}
	return s.groupAppendError(err)
}

func encodeGroupCreatedBody(storeID StoreID, catalogNext uint64, groupID string, start api.GroupStart, key topicKey, initial uint64) ([]byte, error) {
	body := append([]byte(nil), storeID[:]...)
	body = appendU64(body, catalogNext)
	var err error
	body, err = appendString(body, groupID, maxGroupIDBytes)
	if err != nil {
		return nil, err
	}
	body = append(body, byte(start), 0, 0, 0)
	if start == api.GroupStartExplicit {
		body = appendEventU32(body, 1)
		body = appendID(body, [16]byte(key.topic))
		body = appendEventU32(body, key.partition)
		body = appendU64(body, initial)
		return body, nil
	}
	return appendEventU32(body, 0), nil
}

func encodeAssignmentBody(storeID StoreID, catalogNext uint64, groupID string, session [16]byte, generation uint64, reason byte, key topicKey, resume uint64, createBaseline bool) ([]byte, error) {
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
	body = appendEventU32(body, 1)
	body = appendID(body, session)
	body = appendEventU32(body, 1)
	body = appendID(body, [16]byte(key.topic))
	body = appendEventU32(body, key.partition)
	body = appendEventU32(body, 1)
	body = appendID(body, [16]byte(key.topic))
	body = appendEventU32(body, key.partition)
	body = appendID(body, session)
	body = appendU64(body, resume)
	if createBaseline {
		body = append(body, 1, 0, 0, 0)
		return appendU64(body, resume), nil
	}
	body = append(body, 0, 0, 0, 0)
	return appendU64(body, 0), nil
}

func encodeEmptyAssignmentBody(storeID StoreID, catalogNext uint64, groupID string, generation uint64) ([]byte, error) {
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
	body = append(body, 2, 0, 0, 0)
	body = appendEventU32(body, 0)
	return appendEventU32(body, 0), nil
}

func encodeCommitBody(storeID StoreID, catalogNext uint64, groupID string, session [16]byte, generation uint64, key topicKey, progress offsetProgress, next uint64) ([]byte, error) {
	body := append([]byte(nil), storeID[:]...)
	body = appendU64(body, catalogNext)
	var err error
	body, err = appendString(body, groupID, maxGroupIDBytes)
	if err != nil {
		return nil, err
	}
	body = appendID(body, [16]byte(storeID))
	body = appendID(body, session)
	body = appendU64(body, generation)
	body = appendID(body, [16]byte(key.topic))
	body = appendEventU32(body, key.partition)
	if progress.HasCommit {
		body = append(body, 1, 0, 0, 0)
		body = appendU64(body, progress.CommittedNext)
	} else {
		body = append(body, 0, 0, 0, 0)
		body = appendU64(body, progress.InitialNext)
	}
	return appendU64(body, next), nil
}

func newConsumerSession() ([16]byte, error) {
	var session [16]byte
	if _, err := rand.Read(session[:]); err != nil {
		return session, fmt.Errorf("generate consumer session: %w", err)
	}
	if session == [16]byte{} {
		session[15] = 1
	}
	return session, nil
}

func consumerMapKey(groupID string, key topicKey) string {
	return groupID + "\x00" + key.topic.String() + fmt.Sprintf("/%d", key.partition)
}
