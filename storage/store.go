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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayeshLK/immulog/api"
)

// SnapshotDiagnostics reports optional projection-cache validation or publication
// failures. Authoritative system-log replay remains available when either field is non-nil.
type SnapshotDiagnostics struct {
	Catalog error
	Offsets error
}

// Store owns one immulog data directory. Ownership lasts until Close has
// stopped every partition and released the operating-system lock.
type Store struct {
	closeMu sync.Mutex
	// snapshotMu serializes snapshot publication and keeps it from racing a
	// close that is about to release directory ownership.
	snapshotMu       sync.Mutex
	retentionMu      sync.Mutex
	retentionStop    chan struct{}
	retentionWake    chan struct{}
	retentionDone    chan struct{}
	retentionStopped atomic.Bool
	retentionStarted atomic.Bool
	mu               sync.Mutex
	closing          atomic.Bool
	rootPath         string
	root             *os.File
	lock             *os.File
	identity         dirIdentity
	storeID          StoreID
	partitions       map[partitionKey]*Partition
	catalog          *Partition
	offsets          *Partition
	tailBudget       *tailBudget
	segmentFiles     *segmentFileCache
	disk             *diskPressureLedger
	options          StoreOptions
	catalogState     *catalogProjection
	catalogAdmission chan struct{}
	// catalogAppend is a deterministic test seam for uncertain metadata writes.
	catalogAppend func(api.RecordBatch) (uint64, error)
	// offsetsAppend is a deterministic test seam for uncertain offsets writes.
	offsetsAppend       func(api.RecordBatch) (uint64, error)
	offsetsState        *offsetsProjection
	consumers           map[string]*Consumer
	groupConsumers      map[string]*GroupConsumer
	expiredGroups       map[string]bool
	offsetsAdmission    chan struct{}
	offsetsUnavailable  bool
	snapshotDiagnostics SnapshotDiagnostics
	metadataUnavailable bool
	closed              bool
	openedAt            time.Time
	closeStartedAt      time.Time
	closeFinishedAt     time.Time
	closeCause          string
}

type partitionKey struct {
	topic     api.TopicID
	partition uint32
}

// StoreOptions controls non-persistent, instance-wide operating limits.
type StoreOptions struct {
	// TailBytes bounds all optional partition-tail entries held by this Store.
	// Zero selects the finite default; individual partitions remain disabled
	// until both PartitionOptions.TailSlots and TailBytes are configured.
	TailBytes uint64
	// MaxTopics caps catalog-owned user topics for this opening instance. Zero
	// selects the finite default and does not change persisted catalog state.
	MaxTopics uint32
	// MaxUserPartitions caps catalog-owned user partitions across all topics.
	// Zero selects the finite default and does not change persisted topic state.
	MaxUserPartitions uint32
	// MaxOpenPartitions caps active user-partition writers/rings. Zero selects
	// the finite default and does not limit durable catalog history.
	MaxOpenPartitions uint32
	// MaxOpenSegmentFiles caps descriptors held for sealed segments across this
	// store. Each open partition also keeps one writer handle for its active
	// segment, so the process holds at most this many plus one per open
	// partition. Zero selects the finite default; readers briefly exceed the
	// cap rather than close a descriptor that a fetch is still using.
	MaxOpenSegmentFiles uint32
	// MaxCatalogHistoryBytes caps new logical growth of __cluster_metadata.
	// Zero selects the finite default; an existing larger history remains
	// readable, but cannot grow until the Store is reopened with a larger limit.
	MaxCatalogHistoryBytes uint64
	// MaxOffsetsHistoryBytes caps new logical growth of __consumer_offsets.
	// Zero selects the finite default; it never authorizes system-log deletion.
	MaxOffsetsHistoryBytes uint64
	// MaxConsumerGroups caps durable historical consumer-group identities.
	// Zero selects the finite default and does not delete existing groups.
	MaxConsumerGroups uint32
	// MaxConsumerProgressKeys caps durable (group, topic, partition) baselines
	// and commits across the offsets projection. Zero selects the finite default.
	MaxConsumerProgressKeys uint32
	// DiskSafetyBytes, DiskUserStopBytes, and DiskResumeBytes define the shared
	// filesystem byte floors F < S < R for this Open instance.
	DiskSafetyBytes   uint64
	DiskUserStopBytes uint64
	DiskResumeBytes   uint64
	// DiskSafetyInodes, DiskUserStopInodes, and DiskResumeInodes define the
	// equivalent finite inode floors when the platform can measure inodes.
	DiskSafetyInodes   uint64
	DiskUserStopInodes uint64
	DiskResumeInodes   uint64
}

func (options *StoreOptions) normalize() {
	if options.TailBytes == 0 {
		options.TailBytes = maxTailBytes
	}
	if options.MaxTopics == 0 {
		options.MaxTopics = 4096
	}
	if options.MaxUserPartitions == 0 {
		options.MaxUserPartitions = maxTopicPartitions
	}
	if options.MaxOpenPartitions == 0 {
		options.MaxOpenPartitions = 1024
	}
	if options.MaxOpenSegmentFiles == 0 {
		options.MaxOpenSegmentFiles = 512
	}
	if options.MaxCatalogHistoryBytes == 0 {
		options.MaxCatalogHistoryBytes = defaultSystemLogCapacityBytes
	}
	if options.MaxOffsetsHistoryBytes == 0 {
		options.MaxOffsetsHistoryBytes = defaultSystemLogCapacityBytes
	}
	if options.MaxConsumerGroups == 0 {
		options.MaxConsumerGroups = 4096
	}
	if options.MaxConsumerProgressKeys == 0 {
		options.MaxConsumerProgressKeys = maxTopicPartitions
	}
	if options.DiskSafetyBytes == 0 {
		options.DiskSafetyBytes = 32 << 20
	}
	if options.DiskUserStopBytes == 0 {
		options.DiskUserStopBytes = 64 << 20
	}
	if options.DiskResumeBytes == 0 {
		options.DiskResumeBytes = 96 << 20
	}
	if options.DiskSafetyInodes == 0 {
		options.DiskSafetyInodes = 16
	}
	if options.DiskUserStopInodes == 0 {
		options.DiskUserStopInodes = 32
	}
	if options.DiskResumeInodes == 0 {
		options.DiskResumeInodes = 64
	}
}

func (options StoreOptions) validate() error {
	if options.TailBytes > maxTailBytes {
		return errors.Join(api.ErrResourceLimit, errors.New("store tail byte limit exceeds runtime limit"))
	}
	if options.MaxTopics == 0 || options.MaxTopics > maxTopicPartitions {
		return errors.Join(api.ErrResourceLimit, errors.New("store topic limit is outside the supported range"))
	}
	if options.MaxUserPartitions == 0 || options.MaxUserPartitions > maxTopicPartitions {
		return errors.Join(api.ErrResourceLimit, errors.New("store user-partition limit is outside the supported range"))
	}
	if options.MaxCatalogHistoryBytes == 0 || options.MaxOffsetsHistoryBytes == 0 {
		return errors.Join(api.ErrResourceLimit, errors.New("system-log history limit must be positive"))
	}
	if options.MaxConsumerGroups == 0 || options.MaxConsumerGroups > maxTopicPartitions {
		return errors.Join(api.ErrResourceLimit, errors.New("consumer-group limit is outside the supported range"))
	}
	if options.MaxConsumerProgressKeys == 0 || options.MaxConsumerProgressKeys > maxTopicPartitions {
		return errors.Join(api.ErrResourceLimit, errors.New("consumer progress-key limit is outside the supported range"))
	}
	if options.DiskSafetyBytes >= options.DiskUserStopBytes || options.DiskUserStopBytes >= options.DiskResumeBytes {
		return errors.Join(api.ErrInvalidArgument, errors.New("disk byte floors must satisfy safety < user-stop < resume"))
	}
	if options.DiskSafetyInodes >= options.DiskUserStopInodes || options.DiskUserStopInodes >= options.DiskResumeInodes {
		return errors.Join(api.ErrInvalidArgument, errors.New("disk inode floors must satisfy safety < user-stop < resume"))
	}
	if options.MaxOpenPartitions == 0 || options.MaxOpenPartitions > maxTopicPartitions {
		return errors.Join(api.ErrResourceLimit, errors.New("store active-partition limit is outside the supported range"))
	}
	return nil
}

func validateCatalogOperatingLimits(projection *catalogProjection, options StoreOptions) error {
	if projection == nil {
		return errors.Join(api.ErrResourceLimit, errors.New("catalog projection is unavailable"))
	}
	if uint64(len(projection.topicsByID)) > uint64(options.MaxTopics) {
		return errors.Join(api.ErrResourceLimit, fmt.Errorf("catalog has %d topics, operating limit is %d", len(projection.topicsByID), options.MaxTopics))
	}
	var partitions uint64
	for _, topic := range projection.topicsByID {
		partitions += uint64(len(topic.descriptor.Partitions))
		if partitions > uint64(options.MaxUserPartitions) {
			return errors.Join(api.ErrResourceLimit, fmt.Errorf("catalog has %d user partitions, operating limit is %d", partitions, options.MaxUserPartitions))
		}
	}
	return nil
}

func offsetsProjectionUsage(projection *offsetsProjection) (groups, progressKeys uint64) {
	if projection == nil {
		return 0, 0
	}
	groups = uint64(len(projection.groups))
	for _, group := range projection.groups {
		if group != nil {
			progressKeys += uint64(len(group.Progress))
			for key := range group.ExplicitStarts {
				if _, exists := group.Progress[key]; !exists {
					progressKeys++
				}
			}
		}
	}
	return groups, progressKeys
}

func groupHasProjectionKey(group *offsetGroup, key topicKey) bool {
	if group == nil {
		return false
	}
	if _, exists := group.Progress[key]; exists {
		return true
	}
	_, exists := group.ExplicitStarts[key]
	return exists
}

func validateOffsetsOperatingLimits(projection *offsetsProjection, options StoreOptions) error {
	if projection == nil {
		return errors.Join(api.ErrResourceLimit, errors.New("offsets projection is unavailable"))
	}
	groups, progressKeys := offsetsProjectionUsage(projection)
	if groups > uint64(options.MaxConsumerGroups) {
		return errors.Join(api.ErrResourceLimit, fmt.Errorf("offsets history has %d consumer groups, operating limit is %d", groups, options.MaxConsumerGroups))
	}
	if progressKeys > uint64(options.MaxConsumerProgressKeys) {
		return errors.Join(api.ErrResourceLimit, fmt.Errorf("offsets history has %d consumer progress keys, operating limit is %d", progressKeys, options.MaxConsumerProgressKeys))
	}
	return nil
}

func admitOffsetsProjectionGrowth(projection *offsetsProjection, options StoreOptions, addGroups, addProgressKeys uint64) error {
	groups, progressKeys := offsetsProjectionUsage(projection)
	if addGroups > uint64(options.MaxConsumerGroups) || groups > uint64(options.MaxConsumerGroups)-addGroups {
		return errors.Join(api.ErrResourceLimit, fmt.Errorf("consumer-group limit %d is exhausted", options.MaxConsumerGroups))
	}
	if addProgressKeys > uint64(options.MaxConsumerProgressKeys) || progressKeys > uint64(options.MaxConsumerProgressKeys)-addProgressKeys {
		return errors.Join(api.ErrResourceLimit, fmt.Errorf("consumer progress-key limit %d is exhausted", options.MaxConsumerProgressKeys))
	}
	return nil
}

func admitOpenPartitions(current int, needed, limit uint32) error {
	if needed > limit || uint64(current)+uint64(needed) > uint64(limit) {
		return errors.Join(api.ErrResourceLimit, fmt.Errorf("opening %d partitions exceeds active-partition limit %d", needed, limit))
	}
	return nil
}

// Open acquires exclusive ownership with the default finite operating profile.
// LOCK is stable and is never removed, renamed, truncated, or replaced.
func Open(dir string) (*Store, error) {
	return OpenWithOptions(dir, StoreOptions{})
}

// OpenWithOptions acquires exclusive ownership using non-persistent runtime
// limits. It never rewrites existing partition configuration.
func OpenWithOptions(dir string, options StoreOptions) (*Store, error) {
	options.normalize()
	if err := options.validate(); err != nil {
		return nil, err
	}
	if dir == "" {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("data directory is empty"))
	}
	requested, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve data directory: %v", api.ErrInvalidArgument, err)
	}
	if err := fsMkdirAll(requested, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory %q: %w", requested, err)
	}
	rootPath, err := filepath.EvalSymlinks(requested)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory %q: %w", requested, err)
	}
	root, err := fsOpen(rootPath)
	if err != nil {
		return nil, fmt.Errorf("open data directory %q: %w", rootPath, err)
	}
	identity, err := identityOf(root)
	if err != nil {
		_ = fileClose(root)
		return nil, fmt.Errorf("identify data directory %q: %w", rootPath, err)
	}
	if !reserveDirectory(identity) {
		_ = fileClose(root)
		return nil, api.ErrDataDirLocked
	}

	lock, err := acquireLock(filepath.Join(rootPath, "LOCK"))
	if err != nil {
		releaseDirectory(identity)
		_ = fileClose(root)
		return nil, err
	}
	if err := syncDir(rootPath); err != nil {
		_ = releaseLock(lock)
		releaseDirectory(identity)
		_ = fileClose(root)
		return nil, fmt.Errorf("sync data-directory bootstrap: %w", err)
	}
	metadata, err := bootstrapMetadata(rootPath, storeIDForIdentity(identity), options)
	if err != nil {
		_ = releaseLock(lock)
		releaseDirectory(identity)
		_ = fileClose(root)
		return nil, err
	}
	if err := validateCatalogOperatingLimits(metadata.projection, options); err != nil {
		_ = metadata.offsets.Close()
		_ = metadata.catalog.Close()
		_ = releaseLock(lock)
		releaseDirectory(identity)
		_ = fileClose(root)
		return nil, err
	}
	if err := validateOffsetsOperatingLimits(metadata.offsetsState, options); err != nil {
		_ = metadata.offsets.Close()
		_ = metadata.catalog.Close()
		_ = releaseLock(lock)
		releaseDirectory(identity)
		_ = fileClose(root)
		return nil, err
	}
	disk, err := newDiskPressureLedger(options, filesystemCapacityProbe(rootPath))
	if err != nil {
		_ = metadata.offsets.Close()
		_ = metadata.catalog.Close()
		_ = releaseLock(lock)
		releaseDirectory(identity)
		_ = fileClose(root)
		return nil, err
	}
	store := &Store{
		rootPath: rootPath, root: root, lock: lock, identity: identity,
		storeID: metadata.projection.storeID, partitions: make(map[partitionKey]*Partition), tailBudget: newTailBudget(options.TailBytes), disk: disk,
		segmentFiles: newSegmentFileCache(options.MaxOpenSegmentFiles),
		options:      options,
		catalog:      metadata.catalog, offsets: metadata.offsets, catalogState: metadata.projection, catalogAdmission: make(chan struct{}, 1), offsetsState: metadata.offsetsState, consumers: make(map[string]*Consumer), groupConsumers: make(map[string]*GroupConsumer), expiredGroups: make(map[string]bool), offsetsAdmission: make(chan struct{}, 1), snapshotDiagnostics: metadata.snapshotDiagnostics,
		openedAt: time.Now(), retentionStop: make(chan struct{}), retentionWake: make(chan struct{}, 1), retentionDone: make(chan struct{}),
	}
	// System partitions need the store back-reference so their appends route
	// through the protected control headroom (§7.9).
	store.catalog.store = store
	store.offsets.store = store
	if hasRetentionPolicy(store.catalogState) {
		store.startRetention()
	}
	return store, nil
}

func (s *Store) attachTailBudget(partition *Partition) {
	if partition != nil {
		partition.tail.attachBudget(s.tailBudget)
	}
}

// RootPath returns the canonical path used for this opening instance.
func (s *Store) RootPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rootPath
}

// SnapshotDiagnostics returns the latest optional projection-cache failures.
func (s *Store) SnapshotDiagnostics() SnapshotDiagnostics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotDiagnostics
}

// OpenPartition opens or creates one local partition under this store. The
// reserved system logs can only be opened by the metadata subsystem.
func (s *Store) OpenPartition(topic api.TopicID, partition uint32, options PartitionOptions) (*Partition, error) {
	if topic.IsZero() {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("topic ID must be nonzero"))
	}
	if topic == api.ClusterMetadataTopicID || topic == api.ConsumerOffsetsTopicID {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("reserved system logs are not public partitions"))
	}
	if partition > int32Max {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("partition ID is outside v1 range"))
	}
	if options.SegmentBytes != 0 && options.SegmentBytes < uint64(SegmentHeaderBytes)+uint64(BatchHeaderBytes)+BatchTrailerBytes+RecordPrefixBytes {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("segment size cannot fit the minimum batch"))
	}
	if options.InitialOffset > maxOffset {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("initial offset is outside v1 range"))
	}
	options.normalize()
	if err := options.validateWriter(); err != nil {
		return nil, err
	}
	tailSlots, tailBytes := options.TailSlots, options.TailBytes
	key := partitionKey{topic: topic, partition: partition}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, api.ErrClosed
	}
	if s.closing.Load() {
		return nil, api.ErrClosing
	}
	if s.metadataUnavailable {
		return nil, api.ErrMetadataUnavailable
	}
	if topicInfo := s.catalogState.topicsByID[topic]; topicInfo != nil {
		if partition >= uint32(len(topicInfo.descriptor.Partitions)) {
			return nil, errors.Join(api.ErrInvalidArgument, errors.New("partition is not part of the catalog topic"))
		}
		descriptor := topicInfo.descriptor.Partitions[partition]
		options = descriptor.Config.options()
		options.InitialOffset = descriptor.RetainedL
		options.TailSlots, options.TailBytes = tailSlots, tailBytes
	}
	if existing := s.partitions[key]; existing != nil {
		return existing, nil
	}
	if err := admitOpenPartitions(len(s.partitions), 1, s.options.MaxOpenPartitions); err != nil {
		return nil, err
	}
	partitionDir := filepath.Join(s.rootPath, "topics", topic.String(), fmt.Sprintf("%d", partition))
	if err := fsMkdirAll(partitionDir, 0o755); err != nil {
		return nil, fmt.Errorf("create partition directory: %w", err)
	}
	if err := syncDir(filepath.Dir(partitionDir)); err != nil {
		return nil, fmt.Errorf("sync topic directory: %w", err)
	}
	if err := syncDir(partitionDir); err != nil {
		return nil, fmt.Errorf("sync partition directory: %w", err)
	}
	opened, err := openPartition(partitionDir, topic, partition, options, s.storeID)
	if err != nil {
		return nil, err
	}
	if topicInfo := s.catalogState.topicsByID[topic]; topicInfo != nil {
		if err := validateRetainedAnchor(opened, topicInfo.descriptor.Partitions[partition]); err != nil {
			_ = opened.Close()
			return nil, err
		}
	}
	opened.store = s
	s.attachTailBudget(opened)
	s.partitions[key] = opened
	return opened, nil
}

// Close fences new work, drains commands already admitted to their durable
// sequencers, then closes all partitions and releases directory ownership last.
func (s *Store) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.closing.Store(true)
	// Snapshot publication runs outside the store mutex, so wait for an
	// in-flight publication before ownership of the directory is released.
	// Builds that start later observe the closing flag and never publish.
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	s.mu.Lock()
	if s.closeStartedAt.IsZero() {
		s.closeStartedAt = time.Now()
	}
	s.mu.Unlock()
	s.stopRetention()
	s.retentionMu.Lock()
	defer s.retentionMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	partitions := make([]*Partition, 0, len(s.partitions))
	for _, partition := range s.partitions {
		partitions = append(partitions, partition)
	}
	s.mu.Unlock()

	var closeErr error
	for _, partition := range partitions {
		closeErr = errors.Join(closeErr, partition.Close())
	}
	closeErr = errors.Join(closeErr, s.catalog.Close(), s.offsets.Close())
	if s.segmentFiles != nil {
		closeErr = errors.Join(closeErr, s.segmentFiles.closeAll())
	}
	s.mu.Lock()
	closeErr = errors.Join(closeErr, releaseLock(s.lock))
	closeErr = errors.Join(closeErr, fileClose(s.root))
	if s.closeCause == "" && closeErr != nil {
		s.closeCause = boundedDiagnosticError(closeErr)
	}
	s.closeFinishedAt = time.Now()
	releaseDirectory(s.identity)
	s.mu.Unlock()
	return closeErr
}
