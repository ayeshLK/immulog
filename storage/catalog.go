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
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ayeshLK/immulog/api"
)

const (
	clusterMetadataDir     = "system/cluster-metadata/0"
	topicPreparationMarker = ".topic-preparation-v1"
	consumerOffsetsDir     = "system/consumer-offsets/0"
)

// TopicPartition describes one partition in a catalog-owned topic.
type TopicPartition struct {
	Partition                  uint32
	Config                     PartitionConfigV1
	InitialSegment             api.SegmentID
	InitialHeaderHash          [32]byte
	RetainedL                  uint64
	CurrentRetainedSegment     api.SegmentID
	CurrentRetainedHeaderHash  [32]byte
	BoundaryCatalogNext        uint64
	MaximumRecordedRetirementH uint64
}

// TopicDescriptor is a caller-owned catalog view. Topic IDs and partition
// configuration are stable after CreateTopic succeeds.
type TopicDescriptor struct {
	ID         api.TopicID
	Name       string
	Partitions []TopicPartition
}

type catalogTopic struct {
	descriptor     TopicDescriptor
	retired        map[uint32]map[uint64]retiredSegmentEvent
	creationOffset uint64
}

type catalogProjection struct {
	storeID       StoreID
	initialized   storeInitializedEvent
	config        PartitionConfigV1
	offsetsConfig PartitionConfigV1
	revision      uint64
	topicsByName  map[string]*catalogTopic
	topicsByID    map[api.TopicID]*catalogTopic
}

func newCatalogProjection() *catalogProjection {
	return &catalogProjection{
		topicsByName: make(map[string]*catalogTopic),
		topicsByID:   make(map[api.TopicID]*catalogTopic),
	}
}

func (projection *catalogProjection) apply(record api.Record) error {
	eventType, payload, err := decodeSystemEvent(record, api.ClusterMetadataTopicID)
	if err != nil {
		return err
	}
	switch eventType {
	case EventStoreInitialized:
		if record.Offset != 0 || projection.revision != 0 {
			return corrupt(errInvalidRecord, "duplicate or misplaced StoreInitialized")
		}
		event, err := decodeStoreInitializedPayload(payload)
		if err != nil {
			return err
		}
		projection.storeID = event.StoreID
		projection.initialized = event
		projection.config = event.CatalogConfig
		projection.offsetsConfig = event.OffsetsConfig
		projection.revision = 1
	case EventTopicCreated:
		if projection.revision == 0 {
			return corrupt(errInvalidRecord, "TopicCreated precedes StoreInitialized")
		}
		event, err := decodeTopicCreatedPayload(payload)
		if err != nil {
			return err
		}
		if event.StoreID != projection.storeID || event.ExpectedCatalogNext != record.Offset {
			return corrupt(errInvalidRecord, "TopicCreated store or catalog revision mismatch")
		}
		if _, exists := projection.topicsByName[event.Name]; exists {
			return corrupt(errInvalidRecord, "duplicate topic name in catalog")
		}
		if _, exists := projection.topicsByID[event.TopicID]; exists {
			return corrupt(errInvalidRecord, "duplicate topic ID in catalog")
		}
		partitions := make([]TopicPartition, len(event.Partitions))
		for index, partition := range event.Partitions {
			partitions[index] = TopicPartition{
				Partition: partition.Partition, Config: partition.Config,
				InitialSegment: partition.Initial.ID, InitialHeaderHash: partition.Initial.HeaderHash,
				CurrentRetainedSegment: partition.Initial.ID, CurrentRetainedHeaderHash: partition.Initial.HeaderHash,
				BoundaryCatalogNext: record.Offset + 1,
			}
		}
		topic := &catalogTopic{
			descriptor:     TopicDescriptor{ID: event.TopicID, Name: event.Name, Partitions: partitions},
			retired:        make(map[uint32]map[uint64]retiredSegmentEvent),
			creationOffset: record.Offset,
		}
		projection.topicsByName[event.Name] = topic
		projection.topicsByID[event.TopicID] = topic
		projection.revision = record.Offset + 1
	case EventPartitionLogStartAdvanced:
		event, err := decodePartitionLogStartAdvancedPayload(payload)
		if err != nil {
			return err
		}
		if event.StoreID != projection.storeID || event.ExpectedCatalogNext != record.Offset {
			return corrupt(errInvalidRecord, "retention event store or catalog revision mismatch")
		}
		topic := projection.topicsByID[event.Key.topic]
		if topic == nil || event.Key.partition >= uint32(len(topic.descriptor.Partitions)) {
			return corrupt(errInvalidRecord, "retention event references an unknown partition")
		}
		partition := &topic.descriptor.Partitions[event.Key.partition]
		if partition.RetainedL != event.ExpectedOldL || event.NewL <= event.ExpectedOldL || event.NewL > event.EvaluatedDurableEnd {
			return corrupt(errInvalidRecord, "retention event retained-start transition is invalid")
		}
		if event.ReasonMask&^partition.Config.RetentionMask != 0 || event.ReasonMask == 0 {
			return corrupt(errInvalidRecord, "retention event reason is not enabled by partition policy")
		}
		if event.EvaluatedDurableEnd < partition.MaximumRecordedRetirementH {
			return corrupt(errInvalidRecord, "retention event durable end regresses")
		}
		if len(event.Retired) == 0 || event.Retired[0].Base != event.ExpectedOldL {
			return corrupt(errInvalidRecord, "retention retirement list does not start at the retained boundary")
		}
		if !sameEventAnchor(event.Retired[0].Anchor, eventAnchor{ID: partition.CurrentRetainedSegment, HeaderHash: partition.CurrentRetainedHeaderHash}) {
			return corrupt(errInvalidRecord, "retention retirement list has the wrong retained anchor")
		}
		cursor := event.ExpectedOldL

		seen := make(map[api.SegmentID]struct{}, len(event.Retired))
		for _, retired := range event.Retired {
			if retired.Base != cursor || retired.Base >= retired.End || retired.End > event.NewL || retired.Bytes == 0 {
				return corrupt(errInvalidRecord, "retention retirement list has a gap or invalid extent")
			}
			if _, duplicate := seen[retired.Anchor.ID]; duplicate {
				return corrupt(errInvalidRecord, "retention retirement list repeats a segment identity")
			}
			seen[retired.Anchor.ID] = struct{}{}
			if retired.Anchor.ID == event.RetainedAnchor.ID {
				return corrupt(errInvalidRecord, "retention successor anchor is retired")
			}
			cursor = retired.End
		}
		if cursor != event.NewL {
			return corrupt(errInvalidRecord, "retention retirement list does not cover the boundary")
		}
		if topic.retired == nil {
			topic.retired = make(map[uint32]map[uint64]retiredSegmentEvent)
		}
		retiredByBase := topic.retired[event.Key.partition]
		if retiredByBase == nil {
			retiredByBase = make(map[uint64]retiredSegmentEvent)
			topic.retired[event.Key.partition] = retiredByBase
		}
		for _, retired := range event.Retired {
			retiredByBase[retired.Base] = retired
		}
		partition.RetainedL = event.NewL
		partition.CurrentRetainedSegment = event.RetainedAnchor.ID
		partition.CurrentRetainedHeaderHash = event.RetainedAnchor.HeaderHash
		partition.BoundaryCatalogNext = record.Offset + 1
		if event.EvaluatedDurableEnd > partition.MaximumRecordedRetirementH {
			partition.MaximumRecordedRetirementH = event.EvaluatedDurableEnd
		}
		projection.revision = record.Offset + 1
	default:
		return errors.Join(api.ErrUnsupportedFormat, errors.New("unsupported catalog event"))
	}
	return nil
}

func sameEventAnchor(left, right eventAnchor) bool {
	return left.ID == right.ID && left.HeaderHash == right.HeaderHash
}

func (projection *catalogProjection) replay(partition *Partition) error {
	end, err := partition.EndOffset()

	if err != nil {
		return err
	}
	for offset := uint64(0); offset < end; {
		records, err := partition.Read(offset, MaxBatchRecords)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return corrupt(errInvalidRecord, "catalog replay stopped before durable end")
		}
		for _, record := range records {
			if record.Offset != offset {
				return corrupt(errInvalidRecord, "catalog replay offset gap")
			}
			if err := projection.apply(record); err != nil {
				return err
			}
			offset++
		}
	}
	if projection.revision == 0 {
		return corrupt(errInvalidRecord, "catalog is missing StoreInitialized")
	}
	return nil
}

func cloneTopicDescriptor(descriptor TopicDescriptor) TopicDescriptor {
	clone := TopicDescriptor{ID: descriptor.ID, Name: descriptor.Name, Partitions: make([]TopicPartition, len(descriptor.Partitions))}
	copy(clone.Partitions, descriptor.Partitions)
	return clone
}
func (store *Store) CreateTopic(name string, partitions uint32, options PartitionOptions) (TopicDescriptor, error) {
	return store.CreateTopicContext(context.Background(), name, partitions, options)
}

// CreateTopicContext admits at most one catalog mutation at a time. Context
// cancellation before sequencer ownership is definitive. Once the sequencer
// starts a mutation, cancellation reports an unknown outcome while the
// sequencer completes its durable work before admitting the next mutation.
func (store *Store) CreateTopicContext(ctx context.Context, name string, partitions uint32, options PartitionOptions) (TopicDescriptor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return TopicDescriptor{}, err
	}
	if store.closing.Load() {
		return TopicDescriptor{}, api.ErrClosing
	}
	if store.catalogAdmission == nil {
		return store.createTopic(name, partitions, options)
	}
	select {
	case store.catalogAdmission <- struct{}{}:
	case <-ctx.Done():
		return TopicDescriptor{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		<-store.catalogAdmission
		return TopicDescriptor{}, err
	}
	result := make(chan catalogCreateResult, 1)
	go func() {
		descriptor, err := store.createTopic(name, partitions, options)
		result <- catalogCreateResult{descriptor: descriptor, err: err}
		<-store.catalogAdmission
	}()
	select {
	case completed := <-result:
		return completed.descriptor, completed.err
	case <-ctx.Done():
		select {
		case completed := <-result:
			return completed.descriptor, completed.err
		default:
		}
		return TopicDescriptor{}, errors.Join(api.ErrMetadataOutcomeUnknown, ctx.Err())
	}
}

type catalogCreateResult struct {
	descriptor TopicDescriptor
	err        error
}

func (store *Store) createTopic(name string, partitions uint32, options PartitionOptions) (TopicDescriptor, error) {
	if !validTopicName(name) {
		return TopicDescriptor{}, errors.Join(api.ErrInvalidArgument, errors.New("topic name is invalid"))
	}
	if partitions == 0 || partitions > maxTopicPartitions {
		return TopicDescriptor{}, errors.Join(api.ErrInvalidArgument, errors.New("topic partition count is outside limits"))
	}
	if options.InitialOffset != 0 {
		return TopicDescriptor{}, errors.Join(api.ErrInvalidArgument, errors.New("catalog topics must start at offset zero"))
	}
	options.normalize()
	if err := options.validateWriter(); err != nil {
		return TopicDescriptor{}, err
	}
	config, err := configFromOptions(options)
	if err != nil {
		return TopicDescriptor{}, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return TopicDescriptor{}, api.ErrClosed
	}
	if store.closing.Load() {
		return TopicDescriptor{}, api.ErrClosing
	}
	if store.metadataUnavailable {
		return TopicDescriptor{}, api.ErrMetadataUnavailable
	}
	if _, exists := store.catalogState.topicsByName[name]; exists {
		return TopicDescriptor{}, api.ErrTopicExists
	}
	if uint64(len(store.catalogState.topicsByID)) >= uint64(store.options.MaxTopics) {
		return TopicDescriptor{}, errors.Join(api.ErrResourceLimit, fmt.Errorf("topic limit %d is exhausted", store.options.MaxTopics))
	}
	if err := validateCatalogOperatingLimits(store.catalogState, store.options); err != nil {
		return TopicDescriptor{}, err
	}
	var existingPartitions uint64
	for _, topic := range store.catalogState.topicsByID {
		existingPartitions += uint64(len(topic.descriptor.Partitions))
	}
	if uint64(partitions) > uint64(store.options.MaxUserPartitions)-existingPartitions {
		return TopicDescriptor{}, errors.Join(api.ErrResourceLimit, fmt.Errorf("topic needs %d user partitions, operating limit is %d", partitions, store.options.MaxUserPartitions))
	}
	if err := admitOpenPartitions(len(store.partitions), partitions, store.options.MaxOpenPartitions); err != nil {
		return TopicDescriptor{}, err
	}
	topicID, err := newTopicID()
	if err != nil {
		return TopicDescriptor{}, err
	}
	if _, err := prepareTopicStorage(store.rootPath, topicID); err != nil {
		return TopicDescriptor{}, err
	}
	prepared := make([]*Partition, partitions)
	parts := make([]topicCreatedPartition, partitions)
	for index := range prepared {
		partitionDir := filepath.Join(store.rootPath, "topics", topicID.String(), fmt.Sprintf("%d", index))
		if err := fsMkdirAll(partitionDir, 0o755); err != nil {
			closePrepared(prepared)
			return TopicDescriptor{}, fmt.Errorf("prepare topic partition directory: %w", err)
		}
		opened, err := openPartition(partitionDir, topicID, uint32(index), options, store.storeID)
		if err != nil {
			closePrepared(prepared)
			return TopicDescriptor{}, err
		}
		prepared[index] = opened
		parts[index] = topicCreatedPartition{Partition: uint32(index), Config: config, Initial: eventAnchor{ID: opened.segments[0].header.ID, HeaderHash: opened.segments[0].headerHash}}
	}
	payload, err := encodeTopicCreatedPayload(topicCreatedEvent{
		StoreID: store.storeID, ExpectedCatalogNext: store.catalog.logEnd,
		TopicID: topicID, Name: name, Partitions: parts,
	})
	if err != nil {
		closePrepared(prepared)
		return TopicDescriptor{}, err
	}
	value, err := encodeSystemEvent(EventTopicCreated, payload)
	if err != nil {
		closePrepared(prepared)
		return TopicDescriptor{}, err
	}
	if err := systemEventAdmission(store.catalog, api.ClusterMetadataTopicID, store.catalog.logEnd, value, store.options.MaxCatalogHistoryBytes); err != nil {
		closePrepared(prepared)
		return TopicDescriptor{}, err
	}
	appendCatalog := store.catalog.AppendBatch
	if store.catalogAppend != nil {
		appendCatalog = store.catalogAppend
	}
	_, err = appendCatalog(api.RecordBatch{
		Topic: api.ClusterMetadataTopicID, Partition: 0, BaseOffset: store.catalog.logEnd,
		Records: []api.Record{{Topic: api.ClusterMetadataTopicID, Partition: 0, Offset: store.catalog.logEnd, Value: value}},
	})
	if err != nil {
		closePrepared(prepared)
		if errors.Is(err, api.ErrAppendOutcomeUnknown) {
			store.metadataUnavailable = true
		}
		return TopicDescriptor{}, err
	}
	if err := store.catalogState.apply(api.Record{Topic: api.ClusterMetadataTopicID, Partition: 0, Offset: store.catalog.logEnd - 1, Value: value}); err != nil {
		store.metadataUnavailable = true
		closePrepared(prepared)
		return TopicDescriptor{}, errors.Join(api.ErrMetadataUnavailable, err)
	}
	descriptor := cloneTopicDescriptor(store.catalogState.topicsByName[name].descriptor)
	for index, partition := range prepared {
		partition.store = store
		store.attachTailBudget(partition)
		store.partitions[partitionKey{topic: topicID, partition: uint32(index)}] = partition
	}
	if config.RetentionMask != 0 {
		store.startRetention()
	}
	store.signalRetention()
	return descriptor, nil
}

func closePrepared(partitions []*Partition) {
	for _, partition := range partitions {
		if partition != nil {
			_ = partition.Close()
		}
	}
}

func topicPreparationMarkerBytes(topic api.TopicID) []byte {
	return []byte("IEL-TOPIC-PREPARATION-V1\n" + topic.String() + "\n")
}

// prepareTopicStorage records a durable, non-authoritative marker before any
// TopicCreated append. Recovery can distinguish this artifact from ambiguous
// unreferenced storage and preserve it for reconciliation.
func prepareTopicStorage(rootPath string, topic api.TopicID) (string, error) {
	topicsDir := filepath.Join(rootPath, "topics")
	topicDir := filepath.Join(topicsDir, topic.String())
	if err := fsMkdirAll(topicDir, 0o755); err != nil {
		return "", fmt.Errorf("create topic preparation directory: %w", err)
	}
	if err := syncDir(rootPath); err != nil {
		return "", fmt.Errorf("sync topic root: %w", err)
	}
	if err := syncDir(topicsDir); err != nil {
		return "", fmt.Errorf("sync topics directory: %w", err)
	}
	if err := syncDir(topicDir); err != nil {
		return "", fmt.Errorf("sync topic preparation directory: %w", err)
	}
	markerPath := filepath.Join(topicDir, topicPreparationMarker)
	marker, err := fsOpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", fmt.Errorf("create topic preparation marker: %w", err)
	}
	data := topicPreparationMarkerBytes(topic)
	if count, err := fileWrite(marker, data); err != nil || count != len(data) {
		_ = fileClose(marker)
		return "", errors.Join(err, io.ErrShortWrite)
	}
	if err := fileSync(marker); err != nil {
		_ = fileClose(marker)
		return "", fmt.Errorf("sync topic preparation marker: %w", err)
	}
	if err := fileClose(marker); err != nil {
		return "", fmt.Errorf("close topic preparation marker: %w", err)
	}
	if err := syncDir(topicDir); err != nil {
		return "", fmt.Errorf("sync topic preparation marker directory: %w", err)
	}
	return topicDir, nil
}

func validateTopicPreparationMarker(path string, topic api.TopicID) error {
	data := topicPreparationMarkerBytes(topic)
	info, err := fsStat(path)
	if err != nil {
		return fmt.Errorf("stat topic preparation marker: %w", err)
	}
	if info.Size() != int64(len(data)) {
		return corrupt(errInvalidSegment, "topic preparation marker has an invalid length")
	}
	actual, err := fsReadFile(path)
	if err != nil {
		return fmt.Errorf("read topic preparation marker: %w", err)
	}
	if string(actual) != string(data) {
		return corrupt(errInvalidSegment, "topic preparation marker does not match its TopicID")
	}
	return nil
}

func newTopicID() (api.TopicID, error) {
	var id api.TopicID
	if _, err := rand.Read(id[:]); err != nil {
		return api.TopicID{}, fmt.Errorf("generate topic ID: %w", err)
	}
	if id.IsZero() || id == api.ClusterMetadataTopicID || id == api.ConsumerOffsetsTopicID {
		id[15] ^= 1
	}
	return id, nil
}

func (store *Store) DescribeTopic(name string) (TopicDescriptor, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return TopicDescriptor{}, api.ErrClosed
	}
	if store.closing.Load() {
		return TopicDescriptor{}, api.ErrClosing
	}
	if store.metadataUnavailable {
		return TopicDescriptor{}, api.ErrMetadataUnavailable
	}
	topic := store.catalogState.topicsByName[name]
	if topic == nil {
		return TopicDescriptor{}, api.ErrUnknownTopic
	}
	return cloneTopicDescriptor(topic.descriptor), nil
}

func (store *Store) ListTopics() ([]TopicDescriptor, error) {
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
	names := make([]string, 0, len(store.catalogState.topicsByName))
	for name := range store.catalogState.topicsByName {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]TopicDescriptor, 0, len(names))
	for _, name := range names {
		result = append(result, cloneTopicDescriptor(store.catalogState.topicsByName[name].descriptor))
	}
	return result, nil
}

func (store *Store) OpenTopic(name string) ([]*Partition, error) {
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
	topic := store.catalogState.topicsByName[name]
	if topic == nil {
		return nil, api.ErrUnknownTopic
	}
	var missing uint32
	for index := range topic.descriptor.Partitions {
		key := partitionKey{topic: topic.descriptor.ID, partition: uint32(index)}
		if store.partitions[key] == nil {
			missing++
		}
	}
	if err := admitOpenPartitions(len(store.partitions), missing, store.options.MaxOpenPartitions); err != nil {
		return nil, err
	}
	result := make([]*Partition, len(topic.descriptor.Partitions))
	for index, descriptor := range topic.descriptor.Partitions {
		key := partitionKey{topic: topic.descriptor.ID, partition: uint32(index)}
		if existing := store.partitions[key]; existing != nil {
			result[index] = existing
			continue
		}
		partitionDir := filepath.Join(store.rootPath, "topics", topic.descriptor.ID.String(), fmt.Sprintf("%d", index))
		options := descriptor.Config.options()
		options.InitialOffset = descriptor.RetainedL
		opened, err := openPartition(partitionDir, topic.descriptor.ID, uint32(index), options, store.storeID)
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
		result[index] = opened
	}
	return result, nil
}
