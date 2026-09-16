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
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/ayeshLK/immulog/api"
)

const (
	retentionReasonTime uint8 = 1 << iota
	retentionReasonSize
)

type retentionProposal struct {
	retired        []*segment
	retained       []*segment
	newL           uint64
	anchor         *segment
	reasonMask     uint8
	remainingBytes uint64
}

// RunRetention evaluates every catalog-owned user partition once. A successful
// pass makes the next retained segment the durable catalog anchor before it
// removes the exact, now-expired segment artifacts.
func (store *Store) RunRetention(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return store.runRetentionAt(ctx, time.Now())
}

func (store *Store) runRetentionAt(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.retentionMu.Lock()
	defer store.retentionMu.Unlock()

	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return api.ErrClosed
	}
	if store.closing.Load() {
		store.mu.Unlock()
		return api.ErrClosing
	}
	if store.metadataUnavailable {
		store.mu.Unlock()
		return api.ErrMetadataUnavailable
	}
	descriptors := make([]TopicDescriptor, 0, len(store.catalogState.topicsByID))
	for _, topic := range store.catalogState.topicsByID {
		descriptors = append(descriptors, cloneTopicDescriptor(topic.descriptor))
	}
	store.mu.Unlock()
	if err := store.reconcileRetiredCleanup(); err != nil {
		return err
	}

	var result error
	for _, descriptor := range descriptors {
		for _, partition := range descriptor.Partitions {
			if partition.Config.RetentionMask == 0 {
				continue
			}
			if err := ctx.Err(); err != nil {
				return errors.Join(result, err)
			}
			opened, err := store.openRetentionPartition(descriptor.ID, partition.Partition)
			if err == nil {
				err = store.retainPartitionAt(opened, now)
			}
			result = errors.Join(result, err)
		}
	}
	return result
}

func (store *Store) openRetentionPartition(topicID api.TopicID, partitionID uint32) (*Partition, error) {
	key := partitionKey{topic: topicID, partition: partitionID}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, api.ErrClosed
	}
	if store.closing.Load() {
		return nil, api.ErrClosing
	}
	if store.metadataUnavailable {
		return nil, api.ErrMetadataUnavailable
	}
	if existing := store.partitions[key]; existing != nil {
		return existing, nil
	}
	topic := store.catalogState.topicsByID[topicID]
	if topic == nil || partitionID >= uint32(len(topic.descriptor.Partitions)) {
		return nil, api.ErrUnknownTopic
	}
	descriptor := topic.descriptor.Partitions[partitionID]
	options := descriptor.Config.options()
	options.InitialOffset = descriptor.RetainedL
	partitionDir := filepath.Join(store.rootPath, "topics", topicID.String(), fmt.Sprintf("%d", partitionID))
	opened, err := openPartition(partitionDir, topicID, partitionID, options, store.storeID)
	if err != nil {
		return nil, err
	}
	if err := validateRetainedAnchor(opened, descriptor); err != nil {
		_ = opened.Close()
		return nil, err
	}
	opened.store = store
	store.attachTailBudget(opened)
	store.partitions[key] = opened
	return opened, nil
}

func validateRetainedAnchor(partition *Partition, descriptor TopicPartition) error {
	if partition == nil || len(partition.segments) == 0 ||
		partition.segments[0].header.BaseOffset != descriptor.RetainedL ||
		partition.segments[0].header.ID != descriptor.CurrentRetainedSegment ||
		partition.segments[0].headerHash != descriptor.CurrentRetainedHeaderHash {
		return corrupt(errInvalidSegment, "topic partition retained anchor mismatch")
	}
	return nil
}

func (store *Store) retainPartitionAt(partition *Partition, now time.Time) error {
	if partition == nil {
		return errors.Join(api.ErrInvalidArgument, errors.New("retention partition is nil"))
	}
	retired, err := func() ([]*segment, error) {
		partition.mu.Lock()
		defer partition.mu.Unlock()
		if partition.closed {
			return nil, api.ErrClosed
		}
		if partition.closing || store.closing.Load() {
			return nil, api.ErrClosing
		}
		if partition.unavailable {
			return nil, api.ErrPartitionUnavailable
		}

		store.mu.Lock()
		defer store.mu.Unlock()
		if store.closed {
			return nil, api.ErrClosed
		}
		if store.closing.Load() {
			return nil, api.ErrClosing
		}
		if store.metadataUnavailable {
			return nil, api.ErrMetadataUnavailable
		}
		topic := store.catalogState.topicsByID[partition.topic]
		if topic == nil || partition.partition >= uint32(len(topic.descriptor.Partitions)) {
			return nil, api.ErrUnknownTopic
		}
		descriptor := topic.descriptor.Partitions[partition.partition]
		if err := validateRetainedAnchor(partition, descriptor); err != nil {
			partition.unavailable = true
			partition.signalFetchWaitersLocked()
			return nil, errors.Join(api.ErrPartitionUnavailable, err)
		}
		proposal, err := planRetentionLocked(partition, descriptor.Config, now.UnixMilli())
		if err != nil || proposal == nil {
			return nil, err
		}

		event := partitionLogStartAdvancedEvent{
			StoreID: store.storeID, ExpectedCatalogNext: store.catalog.logEnd,
			Key:          topicKey{topic: partition.topic, partition: partition.partition},
			ExpectedOldL: descriptor.RetainedL, NewL: proposal.newL,
			EvaluatedDurableEnd: partition.logEnd,
			RetainedAnchor:      eventAnchor{ID: proposal.anchor.header.ID, HeaderHash: proposal.anchor.headerHash},
			ReasonMask:          proposal.reasonMask, EvaluatedUnixMillis: now.UnixMilli(),
			EvaluatedRetainedBytes: proposal.remainingBytes,
			Retired:                retiredEvents(proposal.retired),
		}
		payload, err := encodePartitionLogStartAdvancedPayload(event)
		if err != nil {
			return nil, err
		}
		value, err := encodeSystemEvent(EventPartitionLogStartAdvanced, payload)
		if err != nil {
			return nil, err
		}
		if err := systemEventAdmission(store.catalog, api.ClusterMetadataTopicID, store.catalog.logEnd, value, store.options.MaxCatalogHistoryBytes); err != nil {
			return nil, err
		}
		appendCatalog := store.catalog.AppendBatch
		if store.catalogAppend != nil {
			appendCatalog = store.catalogAppend
		}
		catalogOffset := store.catalog.logEnd
		_, err = appendCatalog(api.RecordBatch{
			Topic: api.ClusterMetadataTopicID, Partition: 0, BaseOffset: catalogOffset,
			Records: []api.Record{{Topic: api.ClusterMetadataTopicID, Partition: 0, Offset: catalogOffset, Value: value}},
		})
		if err != nil {
			if errors.Is(err, api.ErrAppendOutcomeUnknown) {
				store.metadataUnavailable = true
				partition.unavailable = true
				partition.signalFetchWaitersLocked()
			}
			return nil, err
		}
		if err := store.catalogState.apply(api.Record{Topic: api.ClusterMetadataTopicID, Partition: 0, Offset: catalogOffset, Value: value}); err != nil {
			store.metadataUnavailable = true
			partition.unavailable = true
			partition.signalFetchWaitersLocked()
			return nil, errors.Join(api.ErrMetadataUnavailable, err)
		}
		partition.segments = proposal.retained
		partition.tail.clear()
		partition.signalFetchWaitersLocked()
		return proposal.retired, nil
	}()
	if err != nil || len(retired) == 0 {
		return err
	}
	return cleanupRetiredSegments(partition.dir, retired)
}

func retiredEvents(segments []*segment) []retiredSegmentEvent {
	events := make([]retiredSegmentEvent, len(segments))
	for index, segment := range segments {
		events[index] = retiredSegmentEvent{
			Base: segment.header.BaseOffset, End: segment.end, Bytes: uint64(segment.size),
			Anchor: eventAnchor{ID: segment.header.ID, HeaderHash: segment.headerHash},
		}
	}
	return events
}

func planRetentionLocked(partition *Partition, config PartitionConfigV1, nowMillis int64) (*retentionProposal, error) {
	if len(partition.segments) == 0 {
		return nil, corrupt(errInvalidSegment, "partition has no retained segment")
	}
	total, err := retainedSegmentBytes(partition.segments)
	if err != nil {
		return nil, err
	}
	active := partition.segments[len(partition.segments)-1]
	if active.records != 0 && retentionNeedsRoll(active, total, config, nowMillis) {
		rolled, err := createSegment(partition.dir, partition.topic, partition.partition, partition.logEnd)
		if err != nil {
			partition.unavailable = true
			partition.signalFetchWaitersLocked()
			return nil, errors.Join(api.ErrPartitionUnavailable, err)
		}
		// The prior active segment is immutable after the roll. Its indexes
		// can be checkpointed without affecting the retention boundary.
		_ = installSegmentIndexes(active, partition.storeID, partition.options.IndexStride)
		partition.segments = append(partition.segments, rolled)
		if total > math.MaxUint64-uint64(rolled.size) {
			return nil, errors.Join(api.ErrResourceLimit, errors.New("retained segment bytes overflow"))
		}
		total += uint64(rolled.size)
	}

	retired := make([]*segment, 0)
	limit := maxRetiredSegmentsPerEvent()
	var reason uint8
	for index := 0; index+1 < len(partition.segments) && len(retired) < limit; index++ {
		segment := partition.segments[index]
		timeEligible := config.RetentionMask&retentionReasonTime != 0 && elapsedAtLeast(nowMillis, segment.maxTime, config.RetentionMillis)
		sizeEligible := config.RetentionMask&retentionReasonSize != 0 && total > config.RetentionBytes
		if !timeEligible && !sizeEligible {
			break
		}
		if timeEligible {
			reason |= retentionReasonTime
		}
		if sizeEligible {
			reason |= retentionReasonSize
		}
		if segment.size < 0 || uint64(segment.size) > total {
			return nil, corrupt(errInvalidSegment, "retired segment has invalid logical size")
		}
		total -= uint64(segment.size)
		retired = append(retired, segment)
	}
	if len(retired) == 0 {
		return nil, nil
	}
	return &retentionProposal{
		retired: retired, retained: append([]*segment(nil), partition.segments[len(retired):]...),
		newL: retired[len(retired)-1].end, anchor: partition.segments[len(retired)],
		reasonMask: reason, remainingBytes: total,
	}, nil
}

func maxRetiredSegmentsPerEvent() int {
	const retiredEventBytes = 72
	available := int(MaxRecordBytes) - RecordPrefixBytes - 8 - eventEnvelopeLen - 160
	if available < retiredEventBytes {
		return 1
	}
	limit := available / retiredEventBytes
	if limit > maxTopicPartitions {
		return maxTopicPartitions
	}
	return limit
}

func retainedSegmentBytes(segments []*segment) (uint64, error) {
	var total uint64
	for _, segment := range segments {
		if segment == nil || segment.size < 0 || uint64(segment.size) > math.MaxUint64-total {
			return 0, corrupt(errInvalidSegment, "retained segment has invalid logical size")
		}
		total += uint64(segment.size)
	}
	return total, nil
}

func retentionNeedsRoll(active *segment, total uint64, config PartitionConfigV1, nowMillis int64) bool {
	if config.RetentionMask&retentionReasonSize != 0 && total > config.RetentionBytes {
		return true
	}
	return config.RetentionMask&retentionReasonTime != 0 && elapsedAtLeast(nowMillis, active.maxTime, config.MaxSegmentAgeMillis)
}

func elapsedAtLeast(nowMillis, timestampMillis int64, durationMillis uint64) bool {
	if timestampMillis == math.MinInt64 || timestampMillis > nowMillis {
		return false
	}
	return uint64(nowMillis)-uint64(timestampMillis) >= durationMillis
}

func cleanupRetiredSegments(dir string, segments []*segment) error {
	var result error
	for _, segment := range segments {
		if segment == nil {
			continue
		}
		if err := fileClose(segment.file); err != nil {
			result = errors.Join(result, err)
			continue
		}
		result = errors.Join(result, removeRetiredArtifact(segment.path))
		result = errors.Join(result, removeRetiredArtifact(indexPath(segment.path, false)))
		result = errors.Join(result, removeRetiredArtifact(indexPath(segment.path, true)))
	}
	if len(segments) != 0 {
		result = errors.Join(result, syncDir(dir))
	}
	return result
}

func removeRetiredArtifact(path string) error {
	err := fsRemove(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
func reconcileRetiredArtifacts(rootPath string, projection *catalogProjection) error {
	return reconcileRetiredPlans(retiredArtifactPlans(rootPath, projection))
}

type retiredArtifactPlan struct {
	dir   string
	bases []uint64
}

func retiredArtifactPlans(rootPath string, projection *catalogProjection) []retiredArtifactPlan {
	plans := make([]retiredArtifactPlan, 0)
	for _, topic := range projection.topicsByID {
		for partition, retired := range topic.retired {
			if len(retired) == 0 {
				continue
			}
			plan := retiredArtifactPlan{dir: filepath.Join(rootPath, "topics", topic.descriptor.ID.String(), fmt.Sprintf("%d", partition)), bases: make([]uint64, 0, len(retired))}
			for base := range retired {
				plan.bases = append(plan.bases, base)
			}
			plans = append(plans, plan)
		}
	}
	return plans
}

func (store *Store) reconcileRetiredCleanup() error {
	store.mu.Lock()
	plans := retiredArtifactPlans(store.rootPath, store.catalogState)
	store.mu.Unlock()
	return reconcileRetiredPlans(plans)
}

func reconcileRetiredPlans(plans []retiredArtifactPlan) error {
	var result error
	for _, plan := range plans {
		for _, base := range plan.bases {
			path := filepath.Join(plan.dir, fmt.Sprintf("%020d.log", base))
			result = errors.Join(result, removeRetiredArtifact(path))
			result = errors.Join(result, removeRetiredArtifact(indexPath(path, false)))
			result = errors.Join(result, removeRetiredArtifact(indexPath(path, true)))
		}
		result = errors.Join(result, syncDir(plan.dir))
	}
	return result
}

func hasRetentionPolicy(projection *catalogProjection) bool {
	if projection == nil {
		return false
	}
	for _, topic := range projection.topicsByID {
		for _, partition := range topic.descriptor.Partitions {
			if partition.Config.RetentionMask != 0 {
				return true
			}
		}
	}
	return false
}

func (store *Store) startRetention() {
	if store.retentionStop == nil || store.retentionWake == nil || store.retentionDone == nil || !store.retentionStarted.CompareAndSwap(false, true) {
		return
	}
	go store.runRetentionLoop(store.retentionStop, store.retentionWake, store.retentionDone)
}

func (store *Store) stopRetention() {
	if !store.retentionStarted.Load() || !store.retentionStopped.CompareAndSwap(false, true) || store.retentionStop == nil {
		return
	}
	close(store.retentionStop)
	<-store.retentionDone
}

func (store *Store) signalRetention() {
	if !store.retentionStarted.Load() || store.retentionStopped.Load() || store.retentionWake == nil {
		return
	}
	select {
	case store.retentionWake <- struct{}{}:
	default:
	}
}

func (store *Store) runRetentionLoop(stop <-chan struct{}, wake <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		delay := store.retentionCheckInterval()
		timer := time.NewTimer(delay)
		select {
		case <-stop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-timer.C:
			_ = store.RunRetention(context.Background())
		}
	}
}

func (store *Store) retentionCheckInterval() time.Duration {
	store.mu.Lock()
	defer store.mu.Unlock()
	interval := time.Hour
	if store.catalogState == nil {
		return interval
	}
	for _, topic := range store.catalogState.topicsByID {
		for _, partition := range topic.descriptor.Partitions {
			if partition.Config.RetentionMask == 0 || partition.Config.RetentionCheckMillis == 0 {
				continue
			}
			candidate := time.Duration(partition.Config.RetentionCheckMillis) * time.Millisecond
			if candidate < interval {
				interval = candidate
			}
		}
	}
	return interval
}
