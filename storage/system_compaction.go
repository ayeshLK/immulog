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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/ayeshLK/immulog/api"
)

const (
	metadataRootDir         = "system/metadata"
	metadataGenerationsDir  = "generations"
	activeManifestName      = "active-manifest"
	generationManifestName  = "manifest"
	checkpointName          = "checkpoint"
	checkpointMagic         = "IELCKP00"
	checkpointHeaderBytes   = 104
	checkpointSchemaVersion = uint16(1)
	manifestMagic           = "IELMNF00"
	manifestBytes           = 256
	manifestSchemaVersion   = uint16(1)
	maxGenerationProbe      = 1024
)

type systemCheckpoint struct {
	kind       uint16
	generation uint64
	storeID    StoreID
	next       uint64
	entries    uint64
	payload    []byte
}

type systemManifest struct {
	generation        uint64
	storeID           StoreID
	catalogNext       uint64
	offsetsNext       uint64
	catalogCheckpoint [32]byte
	offsetsCheckpoint [32]byte
	catalogAnchor     eventAnchor
	offsetsAnchor     eventAnchor
}

func metadataRoot(root string) string { return filepath.Join(root, metadataRootDir) }

func generationRoot(root string, generation uint64) string {
	return filepath.Join(metadataRoot(root), metadataGenerationsDir, fmt.Sprintf("%020d", generation))
}

func generationPartitionDir(root string, generation uint64, catalog bool) string {
	name := "consumer-offsets"
	if catalog {
		name = "cluster-metadata"
	}
	return filepath.Join(generationRoot(root, generation), name, "0")
}

func encodeCheckpoint(checkpoint systemCheckpoint) ([]byte, error) {
	if checkpoint.kind != snapshotKindCatalog && checkpoint.kind != snapshotKindOffsets || checkpoint.generation == 0 || checkpoint.storeID == (StoreID{}) || checkpoint.next > maxOffset || uint64(len(checkpoint.payload)) > maxSnapshotPayload || checkpoint.entries > maxSnapshotEntries {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("system checkpoint identity or bounds are invalid"))
	}
	header := make([]byte, checkpointHeaderBytes)
	copy(header[:8], checkpointMagic)
	binary.LittleEndian.PutUint16(header[8:10], checkpointSchemaVersion)
	binary.LittleEndian.PutUint16(header[10:12], checkpointHeaderBytes)
	binary.LittleEndian.PutUint16(header[12:14], checkpoint.kind)
	binary.LittleEndian.PutUint64(header[16:24], checkpoint.generation)
	copy(header[24:40], checkpoint.storeID[:])
	binary.LittleEndian.PutUint64(header[40:48], checkpoint.next)
	binary.LittleEndian.PutUint64(header[48:56], uint64(len(checkpoint.payload)))
	binary.LittleEndian.PutUint64(header[56:64], checkpoint.entries)
	payloadDigest := sha256.Sum256(checkpoint.payload)
	copy(header[64:96], payloadDigest[:])
	binary.LittleEndian.PutUint32(header[100:104], CRC32C(header[:100]))
	return append(header, checkpoint.payload...), nil
}

func decodeCheckpoint(data []byte, kind uint16, manifest systemManifest) (systemCheckpoint, error) {
	if len(data) < checkpointHeaderBytes || string(data[:8]) != checkpointMagic || binary.LittleEndian.Uint16(data[10:12]) != checkpointHeaderBytes {
		return systemCheckpoint{}, corrupt(errInvalidRecord, "system checkpoint header is invalid")
	}
	if binary.LittleEndian.Uint32(data[100:104]) != CRC32C(data[:100]) {
		return systemCheckpoint{}, corrupt(errInvalidRecord, "system checkpoint header checksum mismatch")
	}
	if binary.LittleEndian.Uint16(data[8:10]) != checkpointSchemaVersion {
		return systemCheckpoint{}, unsupported("unsupported system checkpoint version")
	}
	if binary.LittleEndian.Uint16(data[14:16]) != 0 || binary.LittleEndian.Uint32(data[96:100]) != 0 {
		return systemCheckpoint{}, corrupt(errInvalidRecord, "system checkpoint reserved fields are nonzero")
	}
	checkpoint := systemCheckpoint{kind: binary.LittleEndian.Uint16(data[12:14]), generation: binary.LittleEndian.Uint64(data[16:24]), next: binary.LittleEndian.Uint64(data[40:48]), entries: binary.LittleEndian.Uint64(data[56:64])}
	copy(checkpoint.storeID[:], data[24:40])
	payloadBytes := binary.LittleEndian.Uint64(data[48:56])
	if checkpoint.kind != kind || checkpoint.generation != manifest.generation || checkpoint.storeID != manifest.storeID || checkpoint.next > maxOffset || payloadBytes > maxSnapshotPayload || checkpoint.entries > maxSnapshotEntries || payloadBytes != uint64(len(data)-checkpointHeaderBytes) {
		return systemCheckpoint{}, corrupt(errInvalidRecord, "system checkpoint identity or length mismatch")
	}
	checkpoint.payload = data[checkpointHeaderBytes:]
	payloadDigest := sha256.Sum256(checkpoint.payload)
	if !bytes.Equal(payloadDigest[:], data[64:96]) {
		return systemCheckpoint{}, corrupt(errInvalidRecord, "system checkpoint payload digest mismatch")
	}
	return checkpoint, nil
}

func encodeManifest(manifest systemManifest) ([]byte, error) {
	if manifest.generation == 0 || manifest.storeID == (StoreID{}) || manifest.catalogNext == 0 || manifest.catalogNext > maxOffset || manifest.offsetsNext > maxOffset || manifest.catalogCheckpoint == ([32]byte{}) || manifest.offsetsCheckpoint == ([32]byte{}) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("system manifest identity is invalid"))
	}
	if manifest.catalogAnchor.ID.IsZero() || manifest.catalogAnchor.HeaderHash == ([32]byte{}) || manifest.offsetsAnchor.ID.IsZero() || manifest.offsetsAnchor.HeaderHash == ([32]byte{}) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("system manifest anchor is invalid"))
	}
	data := make([]byte, manifestBytes)
	copy(data[:8], manifestMagic)
	binary.LittleEndian.PutUint16(data[8:10], manifestSchemaVersion)
	binary.LittleEndian.PutUint16(data[10:12], manifestBytes)
	binary.LittleEndian.PutUint64(data[16:24], manifest.generation)
	copy(data[24:40], manifest.storeID[:])
	binary.LittleEndian.PutUint64(data[40:48], manifest.catalogNext)
	binary.LittleEndian.PutUint64(data[48:56], manifest.offsetsNext)
	copy(data[56:88], manifest.catalogCheckpoint[:])
	copy(data[88:120], manifest.offsetsCheckpoint[:])
	copy(data[120:136], manifest.catalogAnchor.ID[:])
	copy(data[136:168], manifest.catalogAnchor.HeaderHash[:])
	copy(data[168:184], manifest.offsetsAnchor.ID[:])
	copy(data[184:216], manifest.offsetsAnchor.HeaderHash[:])
	binary.LittleEndian.PutUint32(data[252:256], CRC32C(data[:252]))
	return data, nil
}

func decodeManifest(data []byte) (systemManifest, error) {
	if len(data) != manifestBytes || string(data[:8]) != manifestMagic || binary.LittleEndian.Uint16(data[10:12]) != manifestBytes {
		return systemManifest{}, corrupt(errInvalidRecord, "system manifest header is invalid")
	}
	if binary.LittleEndian.Uint32(data[252:256]) != CRC32C(data[:252]) {
		return systemManifest{}, corrupt(errInvalidRecord, "system manifest checksum mismatch")
	}
	if binary.LittleEndian.Uint16(data[8:10]) != manifestSchemaVersion {
		return systemManifest{}, unsupported("unsupported system manifest version")
	}
	if binary.LittleEndian.Uint32(data[12:16]) != 0 {
		return systemManifest{}, corrupt(errInvalidRecord, "system manifest reserved fields are nonzero")
	}
	for _, value := range data[216:252] {
		if value != 0 {
			return systemManifest{}, corrupt(errInvalidRecord, "system manifest reserved fields are nonzero")
		}
	}
	manifest := systemManifest{generation: binary.LittleEndian.Uint64(data[16:24]), catalogNext: binary.LittleEndian.Uint64(data[40:48]), offsetsNext: binary.LittleEndian.Uint64(data[48:56])}
	copy(manifest.storeID[:], data[24:40])
	copy(manifest.catalogCheckpoint[:], data[56:88])
	copy(manifest.offsetsCheckpoint[:], data[88:120])
	copy(manifest.catalogAnchor.ID[:], data[120:136])
	copy(manifest.catalogAnchor.HeaderHash[:], data[136:168])
	copy(manifest.offsetsAnchor.ID[:], data[168:184])
	copy(manifest.offsetsAnchor.HeaderHash[:], data[184:216])
	if manifest.generation == 0 || manifest.storeID == (StoreID{}) || manifest.catalogNext == 0 || manifest.catalogNext > maxOffset || manifest.offsetsNext > maxOffset || manifest.catalogCheckpoint == ([32]byte{}) || manifest.offsetsCheckpoint == ([32]byte{}) || manifest.catalogAnchor.ID.IsZero() || manifest.catalogAnchor.HeaderHash == ([32]byte{}) || manifest.offsetsAnchor.ID.IsZero() || manifest.offsetsAnchor.HeaderHash == ([32]byte{}) {
		return systemManifest{}, corrupt(errInvalidRecord, "system manifest identity is invalid")
	}
	return manifest, nil
}

func publishAuthoritativeFile(path string, data []byte) error {
	if err := fsMkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := fsCreateTemp(filepath.Dir(path), ".metadata-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	keep := false
	closed := false
	defer func() {
		if !closed {
			_ = fileClose(temporary)
		}
		if !keep {
			_ = fsRemove(temporaryPath)
		}
	}()
	if count, writeErr := fileWrite(temporary, data); writeErr != nil || count != len(data) {
		return errors.Join(writeErr, io.ErrShortWrite)
	}
	if err := fileSync(temporary); err != nil {
		return err
	}
	if err := fileClose(temporary); err != nil {
		return err
	}
	closed = true
	if err := fsRename(temporaryPath, path); err != nil {
		return err
	}
	keep = true
	return syncDir(filepath.Dir(path))
}

func encodeCatalogCheckpointPayload(projection *catalogProjection) ([]byte, uint64, error) {
	payload, entries, err := encodeCatalogSnapshotPayload(projection)
	if err != nil {
		return nil, 0, err
	}
	type retiredEntry struct {
		topic api.TopicID
		part  uint32
		item  retiredSegmentEvent
	}
	retired := make([]retiredEntry, 0)
	for topicID, topic := range projection.topicsByID {
		for partition, byBase := range topic.retired {
			for _, item := range byBase {
				retired = append(retired, retiredEntry{topic: topicID, part: partition, item: item})
			}
		}
	}
	sort.Slice(retired, func(i, j int) bool {
		if comparison := bytes.Compare(retired[i].topic[:], retired[j].topic[:]); comparison != 0 {
			return comparison < 0
		}
		if retired[i].part != retired[j].part {
			return retired[i].part < retired[j].part
		}
		return retired[i].item.Base < retired[j].item.Base
	})
	if uint64(len(retired)) > maxSnapshotEntries {
		return nil, 0, errors.Join(api.ErrResourceLimit, errors.New("catalog checkpoint retired inventory exceeds bounds"))
	}
	payload = appendU32(payload, uint32(len(retired)))
	for _, entry := range retired {
		payload = appendID(payload, [16]byte(entry.topic))
		payload = appendU32(payload, entry.part)
		payload = appendU64(payload, entry.item.Base)
		payload = appendU64(payload, entry.item.End)
		payload = appendU64(payload, entry.item.Bytes)
		payload, err = encodeAnchor(payload, entry.item.Anchor)
		if err != nil {
			return nil, 0, err
		}
	}
	entries += uint64(len(retired))
	if uint64(len(payload)) > maxSnapshotPayload || entries > maxSnapshotEntries {
		return nil, 0, errors.Join(api.ErrResourceLimit, errors.New("catalog checkpoint exceeds bounds"))
	}
	return payload, entries, nil
}

func decodeCatalogCheckpointPayload(data []byte, next uint64, expectedStore StoreID) (*catalogProjection, error) {
	const initializedBytes = 240
	if len(data) < initializedBytes+4 || next == 0 {
		return nil, corrupt(errInvalidRecord, "catalog checkpoint is incomplete")
	}
	initialized, err := decodeStoreInitializedPayload(data[:initializedBytes])
	if err != nil {
		return nil, err
	}
	if initialized.StoreID != expectedStore {
		return nil, corrupt(errInvalidRecord, "catalog checkpoint StoreID mismatch")
	}
	projection := newCatalogProjection()
	projection.storeID = initialized.StoreID
	projection.initialized = initialized
	projection.config = initialized.CatalogConfig
	projection.offsetsConfig = initialized.OffsetsConfig
	projection.revision = next
	cursor := eventCursor{data: data, pos: initializedBytes}
	topicCount, err := cursor.u32()
	if err != nil || topicCount > maxTopicPartitions {
		return nil, corrupt(errInvalidRecord, "catalog checkpoint topic count is invalid")
	}
	var previous api.TopicID
	for index := uint32(0); index < topicCount; index++ {
		creation, err := cursor.u64()
		if err != nil {
			return nil, err
		}
		encodedID, err := cursor.id()
		if err != nil {
			return nil, err
		}
		topicID := api.TopicID(encodedID)
		name, err := cursor.stringValue(maxTopicNameLen)
		if err != nil {
			return nil, err
		}
		partitionCount, err := cursor.u32()
		if err != nil || partitionCount == 0 || partitionCount > maxTopicPartitions || topicID.IsZero() || creation >= next || index > 0 && bytes.Compare(previous[:], topicID[:]) >= 0 {
			return nil, corrupt(errInvalidRecord, "catalog checkpoint topic identity is invalid")
		}
		if _, duplicate := projection.topicsByName[name]; duplicate {
			return nil, corrupt(errInvalidRecord, "catalog checkpoint has duplicate topic name")
		}
		descriptor := TopicDescriptor{ID: topicID, Name: name, Partitions: make([]TopicPartition, partitionCount)}
		for partitionIndex := uint32(0); partitionIndex < partitionCount; partitionIndex++ {
			partitionID, err := cursor.u32()
			if err != nil || partitionID != partitionIndex {
				return nil, corrupt(errInvalidRecord, "catalog checkpoint partition order is invalid")
			}
			config, err := decodePartitionConfig(&cursor, false)
			if err != nil {
				return nil, err
			}
			initial, err := decodeAnchor(&cursor)
			if err != nil {
				return nil, err
			}
			retainedL, err := cursor.u64()
			if err != nil {
				return nil, err
			}
			retained, err := decodeAnchor(&cursor)
			if err != nil {
				return nil, err
			}
			boundary, err := cursor.u64()
			if err != nil {
				return nil, err
			}
			maximumH, err := cursor.u64()
			if err != nil || boundary > next || maximumH < retainedL {
				return nil, corrupt(errInvalidRecord, "catalog checkpoint retention state is invalid")
			}
			descriptor.Partitions[partitionIndex] = TopicPartition{Partition: partitionID, Config: config, InitialSegment: initial.ID, InitialHeaderHash: initial.HeaderHash, RetainedL: retainedL, CurrentRetainedSegment: retained.ID, CurrentRetainedHeaderHash: retained.HeaderHash, BoundaryCatalogNext: boundary, MaximumRecordedRetirementH: maximumH}
		}
		topic := &catalogTopic{descriptor: descriptor, retired: make(map[uint32]map[uint64]retiredSegmentEvent), creationOffset: creation}
		projection.topicsByName[name] = topic
		projection.topicsByID[topicID] = topic
		previous = topicID
	}
	retiredCount, err := cursor.u32()
	if err != nil || uint64(retiredCount) > maxSnapshotEntries {
		return nil, corrupt(errInvalidRecord, "catalog checkpoint retired count is invalid")
	}
	for index := uint32(0); index < retiredCount; index++ {
		encodedTopic, err := cursor.id()
		if err != nil {
			return nil, err
		}
		partition, err := cursor.u32()
		if err != nil {
			return nil, err
		}
		item := retiredSegmentEvent{}
		if item.Base, err = cursor.u64(); err != nil {
			return nil, err
		}
		if item.End, err = cursor.u64(); err != nil {
			return nil, err
		}
		if item.Bytes, err = cursor.u64(); err != nil {
			return nil, err
		}
		if item.Anchor, err = decodeAnchor(&cursor); err != nil {
			return nil, err
		}
		topic := projection.topicsByID[api.TopicID(encodedTopic)]
		if topic == nil || partition >= uint32(len(topic.descriptor.Partitions)) || item.Base >= item.End {
			return nil, corrupt(errInvalidRecord, "catalog checkpoint retired entry is invalid")
		}
		if topic.retired[partition] == nil {
			topic.retired[partition] = make(map[uint64]retiredSegmentEvent)
		}
		if _, duplicate := topic.retired[partition][item.Base]; duplicate {
			return nil, corrupt(errInvalidRecord, "catalog checkpoint has duplicate retired entry")
		}
		topic.retired[partition][item.Base] = item
	}
	if cursor.remaining() != 0 {
		return nil, corrupt(errInvalidRecord, "catalog checkpoint has trailing bytes")
	}
	return projection, nil
}

func decodeOffsetsCheckpointPayload(data []byte, next uint64, storeID StoreID, catalog *catalogProjection) (*offsetsProjection, error) {
	projection := newOffsetsProjection(storeID)
	cursor := eventCursor{data: data}
	groupCount, err := cursor.u32()
	if err != nil || groupCount > maxTopicPartitions {
		return nil, corrupt(errInvalidRecord, "offsets checkpoint group count is invalid")
	}
	for index := uint32(0); index < groupCount; index++ {
		creationOffset, err := cursor.u64()
		if err != nil || creationOffset >= next {
			if err == nil {
				err = corrupt(errInvalidRecord, "offsets checkpoint creation offset is invalid")
			}
			return nil, err
		}
		creationBytes, err := cursor.u32()
		if err != nil || creationBytes == 0 || creationBytes > uint32(maxSnapshotBodyBytes) {
			return nil, corrupt(errInvalidRecord, "offsets checkpoint creation body is invalid")
		}
		creationBody, err := cursor.take(int(creationBytes))
		if err != nil {
			return nil, err
		}
		generation, err := cursor.u64()
		if err != nil {
			return nil, err
		}
		assignmentOffset, err := cursor.u64()
		if err != nil || assignmentOffset >= next && assignmentOffset != 0 {
			if err == nil {
				err = corrupt(errInvalidRecord, "offsets checkpoint assignment offset is invalid")
			}
			return nil, err
		}
		assignmentBytes, err := cursor.u32()
		if err != nil || assignmentBytes > uint32(maxSnapshotBodyBytes) {
			return nil, corrupt(errInvalidRecord, "offsets checkpoint assignment body is invalid")
		}
		assignmentBody, err := cursor.take(int(assignmentBytes))
		if err != nil {
			return nil, err
		}
		progressCount, err := cursor.u32()
		if err != nil || progressCount > maxTopicPartitions {
			return nil, corrupt(errInvalidRecord, "offsets checkpoint progress count is invalid")
		}
		progress := make(map[topicKey]offsetProgress, progressCount)
		for progressIndex := uint32(0); progressIndex < progressCount; progressIndex++ {
			key, err := decodeTopicKey(&cursor)
			if err != nil {
				return nil, err
			}
			initial, err := cursor.u64()
			if err != nil {
				return nil, err
			}
			hasCommit, err := cursor.u8()
			if err != nil || hasCommit > 1 {
				return nil, corrupt(errInvalidRecord, "offsets checkpoint commit flag is invalid")
			}
			if err := expectReserved(&cursor); err != nil {
				return nil, err
			}
			committed, err := cursor.u64()
			if err != nil || hasCommit == 0 && committed != 0 || hasCommit == 1 && committed < initial {
				return nil, corrupt(errInvalidRecord, "offsets checkpoint progress is invalid")
			}
			if _, err := catalogPartition(catalog, key, catalog.revision); err != nil {
				return nil, err
			}
			if _, duplicate := progress[key]; duplicate {
				return nil, corrupt(errInvalidRecord, "offsets checkpoint has duplicate progress key")
			}
			progress[key] = offsetProgress{InitialNext: initial, HasCommit: hasCommit == 1, CommittedNext: committed}
		}
		encoded, err := encodeSystemEvent(EventGroupCreated, creationBody)
		if err != nil {
			return nil, err
		}
		if err := projection.apply(api.Record{Topic: api.ConsumerOffsetsTopicID(), Partition: 0, Offset: creationOffset, Value: encoded}, catalog); err != nil {
			return nil, err
		}
		groupID, err := checkpointGroupID(creationBody)
		if err != nil {
			return nil, err
		}
		group := projection.groups[groupID]
		group.Progress = progress
		if len(assignmentBody) != 0 {
			if err := decodeCheckpointAssignment(groupID, assignmentBody, assignmentOffset, generation, group, catalog, storeID); err != nil {
				return nil, err
			}
		} else if generation != 0 || assignmentOffset != 0 {
			return nil, corrupt(errInvalidRecord, "offsets checkpoint empty assignment state is invalid")
		}
	}
	if cursor.remaining() != 0 {
		return nil, corrupt(errInvalidRecord, "offsets checkpoint has trailing bytes")
	}
	projection.revision = next
	return projection, nil
}

func checkpointGroupID(body []byte) (string, error) {
	cursor := eventCursor{data: body}
	if _, err := cursor.id(); err != nil {
		return "", err
	}
	if _, err := cursor.u64(); err != nil {
		return "", err
	}
	return cursor.stringValue(maxGroupIDBytes)
}

func decodeCheckpointAssignment(groupID string, body []byte, offset, generation uint64, group *offsetGroup, catalog *catalogProjection, storeID StoreID) error {
	cursor := eventCursor{data: body}
	encodedStore, err := cursor.id()
	if err != nil || StoreID(encodedStore) != storeID {
		return corrupt(errInvalidRecord, "checkpoint assignment StoreID mismatch")
	}
	catalogNext, err := cursor.u64()
	if err != nil || catalogNext == 0 || catalogNext > catalog.revision {
		return corrupt(errInvalidRecord, "checkpoint assignment catalog revision is invalid")
	}
	encodedGroup, err := cursor.stringValue(maxGroupIDBytes)
	if err != nil || encodedGroup != groupID {
		return corrupt(errInvalidRecord, "checkpoint assignment group mismatch")
	}
	instance, err := requireID(&cursor, "assignment instance ID")
	if err != nil {
		return err
	}
	expected, err := cursor.u64()
	if err != nil {
		return err
	}
	newGeneration, err := cursor.u64()
	if err != nil || newGeneration != generation || expected+1 != newGeneration {
		return corrupt(errInvalidRecord, "checkpoint assignment generation is invalid")
	}
	if _, err := cursor.u8(); err != nil {
		return err
	}
	if err := expectReserved(&cursor); err != nil {
		return err
	}
	members, err := cursor.u32()
	if err != nil || members > maxTopicPartitions {
		return corrupt(errInvalidRecord, "checkpoint assignment member count is invalid")
	}
	memberIDs := make(map[[16]byte]struct{}, members)
	subscribed := make(map[topicKey][16]byte)
	for member := uint32(0); member < members; member++ {
		session, err := requireID(&cursor, "assignment session ID")
		if err != nil {
			return err
		}
		memberIDs[session] = struct{}{}
		subscriptions, err := cursor.u32()
		if err != nil || subscriptions > maxTopicPartitions {
			return corrupt(errInvalidRecord, "checkpoint assignment subscription count is invalid")
		}
		for subscription := uint32(0); subscription < subscriptions; subscription++ {
			key, err := decodeTopicKey(&cursor)
			if err != nil {
				return err
			}
			if _, err := catalogPartition(catalog, key, catalogNext); err != nil {
				return err
			}
			if _, duplicate := subscribed[key]; duplicate {
				return corrupt(errInvalidRecord, "checkpoint assignment subscription has multiple owners")
			}
			subscribed[key] = session
		}
	}
	assignments, err := cursor.u32()
	if err != nil || assignments > maxTopicPartitions {
		return corrupt(errInvalidRecord, "checkpoint assignment count is invalid")
	}
	owners := make(map[topicKey][16]byte, assignments)
	for assignment := uint32(0); assignment < assignments; assignment++ {
		key, err := decodeTopicKey(&cursor)
		if err != nil {
			return err
		}
		owner, err := requireID(&cursor, "assignment owner session ID")
		if err != nil || subscribed[key] != owner {
			return corrupt(errInvalidRecord, "checkpoint assignment owner is invalid")
		}
		resume, err := cursor.u64()
		if err != nil {
			return err
		}
		action, err := cursor.u8()
		if err != nil || action > 1 {
			return corrupt(errInvalidRecord, "checkpoint assignment baseline action is invalid")
		}
		if err := expectReserved(&cursor); err != nil {
			return err
		}
		initial, err := cursor.u64()
		if err != nil {
			return err
		}
		projected, exists := group.Progress[key]
		validBaseline := action == 1 && initial == projected.InitialNext && resume == initial || action == 0 && initial == 0 && resume >= projected.InitialNext && resume <= latestNext(projected)
		if !exists || !validBaseline {
			return corrupt(errInvalidRecord, "checkpoint assignment baseline does not match progress")
		}
		owners[key] = owner
	}
	if len(owners) != len(subscribed) || cursor.remaining() != 0 {
		return corrupt(errInvalidRecord, "checkpoint assignment coverage is invalid")
	}
	group.Generation = generation
	group.AssignmentOffset = offset
	group.AssignmentBody = append([]byte(nil), body...)
	group.AssignmentOwner = owners
	group.AssignmentMember = memberIDs
	group.AssignmentInstance = instance
	return nil
}

// CompactSystemLogs replaces obsolete authoritative system-log prefixes with
// one versioned checkpoint and fresh absolute-offset suffix per log.
func (store *Store) CompactSystemLogs(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.systemMaintenanceMu.Lock()
	defer store.systemMaintenanceMu.Unlock()
	store.mu.Lock()
	store.systemMaintenance.Running = true
	store.systemMaintenance.LastAttempt = time.Now()
	store.mu.Unlock()
	var result error
	defer func() {
		store.mu.Lock()
		store.systemMaintenance.Running = false
		store.systemMaintenance.LastFailure = boundedDiagnosticError(result)
		store.mu.Unlock()
	}()

	store.snapshotMu.Lock()
	defer store.snapshotMu.Unlock()
	store.retentionMu.Lock()
	defer store.retentionMu.Unlock()
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		result = api.ErrClosed
		return result
	}
	if store.closing.Load() {
		store.mu.Unlock()
		result = api.ErrClosing
		return result
	}
	if store.metadataUnavailable || store.offsetsUnavailable {
		store.mu.Unlock()
		result = errors.Join(api.ErrMetadataUnavailable, api.ErrGroupUnavailable)
		return result
	}
	if err := ctx.Err(); err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	catalogPayload, catalogEntries, err := encodeCatalogCheckpointPayload(store.catalogState)
	if err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	offsetsPayload, offsetsEntries, err := encodeOffsetsSnapshotPayload(store.offsetsState)
	if err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	catalogNext, offsetsNext := store.catalogState.revision, store.offsetsState.revision
	generation, err := nextMetadataGeneration(store.rootPath, store.metadataGeneration)
	if err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	catalogCheckpoint, err := encodeCheckpoint(systemCheckpoint{kind: snapshotKindCatalog, generation: generation, storeID: store.storeID, next: catalogNext, entries: catalogEntries, payload: catalogPayload})
	if err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	offsetsCheckpoint, err := encodeCheckpoint(systemCheckpoint{kind: snapshotKindOffsets, generation: generation, storeID: store.storeID, next: offsetsNext, entries: offsetsEntries, payload: offsetsPayload})
	if err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	estimated := uint64(len(catalogCheckpoint)+len(offsetsCheckpoint)+2*manifestBytes) + 2*uint64(SegmentHeaderBytes)
	reservation, err := store.reserveDiskControl(estimated, 16)
	if err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	defer reservation.release()
	oldCatalog, oldOffsets := store.catalog, store.offsets
	oldGeneration := store.metadataGeneration
	newCatalog, newOffsets, manifest, err := store.prepareSystemGeneration(generation, catalogNext, offsetsNext, catalogCheckpoint, offsetsCheckpoint)
	if err != nil {
		store.mu.Unlock()
		result = err
		return result
	}
	manifestBytesValue, err := encodeManifest(manifest)
	if err != nil {
		store.mu.Unlock()
		_ = newOffsets.Close()
		_ = newCatalog.Close()
		result = err
		return result
	}
	if err := publishAuthoritativeFile(filepath.Join(generationRoot(store.rootPath, generation), generationManifestName), manifestBytesValue); err != nil {
		store.mu.Unlock()
		_ = newOffsets.Close()
		_ = newCatalog.Close()
		result = err
		return result
	}
	if err := publishAuthoritativeFile(filepath.Join(metadataRoot(store.rootPath), activeManifestName), manifestBytesValue); err != nil {
		store.metadataUnavailable = true
		store.offsetsUnavailable = true
		store.mu.Unlock()
		_ = newOffsets.Close()
		_ = newCatalog.Close()
		result = errors.Join(api.ErrMetadataOutcomeUnknown, api.ErrCommitOutcomeUnknown, err)
		return result
	}
	newCatalog.store = store
	newOffsets.store = store
	store.catalog = newCatalog
	store.offsets = newOffsets
	store.catalogAppend = nil
	store.offsetsAppend = nil
	store.metadataGeneration = generation
	store.systemMaintenance.Generation = generation
	store.systemMaintenance.LastSuccess = time.Now()
	store.snapshotDiagnostics = SnapshotDiagnostics{}
	store.mu.Unlock()

	closeErr := errors.Join(oldCatalog.Close(), oldOffsets.Close())
	reclaimed, pending, cleanupErr := cleanupOldSystemAuthority(store.rootPath, oldGeneration, oldCatalog.dir, oldOffsets.dir)
	staleReclaimed, stalePending, staleErr := cleanupUnselectedSystemAuthorities(store.rootPath, generation, oldGeneration)
	reclaimed = saturatingUint64Add(reclaimed, staleReclaimed)
	pending = saturatingUint64Add(pending, stalePending)
	cleanupErr = errors.Join(cleanupErr, staleErr)
	store.mu.Lock()
	store.systemMaintenance.ReclaimedBytes = saturatingUint64Add(store.systemMaintenance.ReclaimedBytes, reclaimed)
	store.systemMaintenance.PendingCleanupBytes = pending
	store.mu.Unlock()
	result = errors.Join(closeErr, cleanupErr)
	return result
}

func nextMetadataGeneration(root string, current uint64) (uint64, error) {
	candidate := current + 1
	if candidate == 0 {
		return 0, errors.Join(api.ErrResourceLimit, errors.New("metadata generation exhausted"))
	}
	for attempt := 0; attempt < maxGenerationProbe; attempt++ {
		_, err := fsStat(generationRoot(root, candidate))
		if errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		}
		if err != nil {
			return 0, err
		}
		candidate++
		if candidate == 0 {
			return 0, errors.Join(api.ErrResourceLimit, errors.New("metadata generation exhausted"))
		}
	}
	return 0, errors.Join(api.ErrResourceLimit, errors.New("too many unpublished metadata generations"))
}

func (store *Store) prepareSystemGeneration(generation, catalogNext, offsetsNext uint64, catalogCheckpoint, offsetsCheckpoint []byte) (*Partition, *Partition, systemManifest, error) {
	root := generationRoot(store.rootPath, generation)
	catalogDir := generationPartitionDir(store.rootPath, generation, true)
	offsetsDir := generationPartitionDir(store.rootPath, generation, false)
	if err := fsMkdirAll(catalogDir, 0o755); err != nil {
		return nil, nil, systemManifest{}, err
	}
	if err := fsMkdirAll(offsetsDir, 0o755); err != nil {
		return nil, nil, systemManifest{}, err
	}
	for _, directory := range []string{filepath.Join(store.rootPath, "system"), metadataRoot(store.rootPath), filepath.Dir(root), root} {
		if err := syncDir(directory); err != nil {
			return nil, nil, systemManifest{}, err
		}
	}
	if err := publishAuthoritativeFile(filepath.Join(catalogDir, checkpointName), catalogCheckpoint); err != nil {
		return nil, nil, systemManifest{}, err
	}
	if err := publishAuthoritativeFile(filepath.Join(offsetsDir, checkpointName), offsetsCheckpoint); err != nil {
		return nil, nil, systemManifest{}, err
	}
	catalogOptions := store.catalogState.config.options()
	catalogOptions.InitialOffset = catalogNext
	catalog, err := openPartition(catalogDir, api.ClusterMetadataTopicID(), 0, catalogOptions, store.storeID)
	if err != nil {
		return nil, nil, systemManifest{}, err
	}
	offsetsOptions := store.catalogState.offsetsConfig.options()
	offsetsOptions.InitialOffset = offsetsNext
	offsets, err := openPartition(offsetsDir, api.ConsumerOffsetsTopicID(), 0, offsetsOptions, store.storeID)
	if err != nil {
		_ = catalog.Close()
		return nil, nil, systemManifest{}, err
	}
	manifest := systemManifest{generation: generation, storeID: store.storeID, catalogNext: catalogNext, offsetsNext: offsetsNext, catalogCheckpoint: sha256.Sum256(catalogCheckpoint), offsetsCheckpoint: sha256.Sum256(offsetsCheckpoint), catalogAnchor: eventAnchor{ID: catalog.segments[0].header.ID, HeaderHash: catalog.segments[0].headerHash}, offsetsAnchor: eventAnchor{ID: offsets.segments[0].header.ID, HeaderHash: offsets.segments[0].headerHash}}
	return catalog, offsets, manifest, nil
}

func cleanupOldSystemAuthority(root string, generation uint64, catalogDir, offsetsDir string) (uint64, uint64, error) {
	var reclaimed, pending uint64
	var result error
	for _, directory := range []string{catalogDir, offsetsDir} {
		entries, err := fsReadDir(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if name != snapshotPathName && name != checkpointName && !isSegmentArtifactName(name) && !isRecognizedSegmentTemporary(name) && !(len(name) >= len(".metadata-") && name[:len(".metadata-")] == ".metadata-" && filepath.Ext(name) == ".tmp") {
				continue
			}
			path := filepath.Join(directory, name)
			info, statErr := fsStat(path)
			if statErr != nil {
				result = errors.Join(result, statErr)
				continue
			}
			size := uint64(0)
			if info.Size() > 0 {
				size = uint64(info.Size())
			}
			if err := fsRemove(path); err != nil {
				pending = saturatingUint64Add(pending, size)
				result = errors.Join(result, err)
			} else {
				reclaimed = saturatingUint64Add(reclaimed, size)
			}
		}
		result = errors.Join(result, syncDir(directory))
	}
	if generation != 0 {
		manifestPath := filepath.Join(generationRoot(root, generation), generationManifestName)
		if info, err := fsStat(manifestPath); err == nil {
			size := uint64(info.Size())
			if removeErr := fsRemove(manifestPath); removeErr != nil {
				pending = saturatingUint64Add(pending, size)
				result = errors.Join(result, removeErr)
			} else {
				reclaimed = saturatingUint64Add(reclaimed, size)
			}
		}
		for _, directory := range []string{catalogDir, offsetsDir, filepath.Dir(catalogDir), filepath.Dir(offsetsDir), generationRoot(root, generation)} {
			if err := fsRemove(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
		result = errors.Join(result, syncDir(filepath.Join(metadataRoot(root), metadataGenerationsDir)))
	}
	return reclaimed, pending, result
}

func cleanupUnselectedSystemAuthorities(root string, activeGeneration, alreadyCleaned uint64) (uint64, uint64, error) {
	var reclaimed, pending uint64
	var result error
	if alreadyCleaned != 0 {
		removed, owed, cleanupErr := cleanupOldSystemAuthority(root, 0, filepath.Join(root, clusterMetadataDir), filepath.Join(root, consumerOffsetsDir))
		reclaimed = saturatingUint64Add(reclaimed, removed)
		pending = saturatingUint64Add(pending, owed)
		result = errors.Join(result, cleanupErr)
	}
	entries, err := fsReadDir(filepath.Join(metadataRoot(root), metadataGenerationsDir))
	if errors.Is(err, os.ErrNotExist) {
		return reclaimed, pending, result
	}
	if err != nil {
		return reclaimed, pending, errors.Join(result, err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == fmt.Sprintf("%020d", activeGeneration) || entry.Name() == fmt.Sprintf("%020d", alreadyCleaned) {
			continue
		}
		generation, parseErr := strconv.ParseUint(entry.Name(), 10, 64)
		if parseErr != nil || fmt.Sprintf("%020d", generation) != entry.Name() || generation == 0 {
			result = errors.Join(result, errors.New("metadata generations directory contains an unexpected entry"))
			continue
		}
		catalogDir := generationPartitionDir(root, generation, true)
		offsetsDir := generationPartitionDir(root, generation, false)
		removed, owed, cleanupErr := cleanupOldSystemAuthority(root, generation, catalogDir, offsetsDir)
		reclaimed = saturatingUint64Add(reclaimed, removed)
		pending = saturatingUint64Add(pending, owed)
		result = errors.Join(result, cleanupErr)
	}
	return reclaimed, pending, result
}

const maxSystemCleanupDiagnosticFiles = 1024

func scanSystemAuthorityCleanup(root string, activeGeneration uint64) (uint32, uint64, bool, bool) {
	paths := make([]string, 0)
	if activeGeneration != 0 {
		paths = append(paths, filepath.Join(root, clusterMetadataDir), filepath.Join(root, consumerOffsetsDir))
	}
	generationsPath := filepath.Join(metadataRoot(root), metadataGenerationsDir)
	entries, err := fsReadDir(generationsPath)
	scanError := err != nil && !errors.Is(err, os.ErrNotExist)
	var files uint32
	truncated := false
	if err == nil {
		activeName := fmt.Sprintf("%020d", activeGeneration)
		for _, entry := range entries {
			if !entry.IsDir() {
				scanError = true
				continue
			}
			if activeGeneration != 0 && entry.Name() == activeName {
				continue
			}
			if files < maxSystemCleanupDiagnosticFiles {
				files++
			} else {
				truncated = true
			}
			generationPath := filepath.Join(generationsPath, entry.Name())
			paths = append(paths, generationPath, filepath.Join(generationPath, "cluster-metadata", "0"), filepath.Join(generationPath, "consumer-offsets", "0"))
		}
	}
	var bytesTotal uint64
	for _, directory := range paths {
		entries, err := fsReadDir(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			scanError = true
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if name != snapshotPathName && name != checkpointName && name != generationManifestName && !isSegmentArtifactName(name) && !(len(name) > 10 && name[:10] == ".metadata-") {
				scanError = true
				continue
			}
			if files == maxSystemCleanupDiagnosticFiles {
				truncated = true
				continue
			}
			info, err := fsStat(filepath.Join(directory, name))
			if err != nil {
				scanError = true
				continue
			}
			files++
			if info.Size() > 0 {
				bytesTotal = saturatingUint64Add(bytesTotal, uint64(info.Size()))
			}
		}
	}
	return files, bytesTotal, truncated, scanError
}

func isSegmentArtifactName(name string) bool {
	if len(name) < 21 {
		return false
	}
	base := name[:20]
	for _, character := range base {
		if character < '0' || character > '9' {
			return false
		}
	}
	suffix := name[20:]
	return suffix == ".log" || suffix == ".index" || suffix == ".timeindex"
}

func bootstrapCompactedMetadata(rootPath string, limits StoreOptions) (*metadataBootstrap, bool, error) {
	activePath := filepath.Join(metadataRoot(rootPath), activeManifestName)
	active, err := fsReadFile(activePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	manifest, err := decodeManifest(active)
	if err != nil {
		return nil, true, err
	}
	generationPath := generationRoot(rootPath, manifest.generation)
	committed, err := fsReadFile(filepath.Join(generationPath, generationManifestName))
	if err != nil || !bytes.Equal(active, committed) {
		if err == nil {
			err = corrupt(errInvalidRecord, "active and generation manifests differ")
		} else {
			err = errors.Join(api.ErrCorruptLog, fmt.Errorf("read selected generation manifest: %w", err))
		}
		return nil, true, err
	}
	catalogBytes, err := fsReadFile(filepath.Join(generationPartitionDir(rootPath, manifest.generation, true), checkpointName))
	if err != nil || sha256.Sum256(catalogBytes) != manifest.catalogCheckpoint {
		if err == nil {
			err = corrupt(errInvalidRecord, "catalog checkpoint manifest digest mismatch")
		} else {
			err = errors.Join(api.ErrCorruptLog, fmt.Errorf("read selected catalog checkpoint: %w", err))
		}
		return nil, true, err
	}
	offsetsBytes, err := fsReadFile(filepath.Join(generationPartitionDir(rootPath, manifest.generation, false), checkpointName))
	if err != nil || sha256.Sum256(offsetsBytes) != manifest.offsetsCheckpoint {
		if err == nil {
			err = corrupt(errInvalidRecord, "offsets checkpoint manifest digest mismatch")
		} else {
			err = errors.Join(api.ErrCorruptLog, fmt.Errorf("read selected offsets checkpoint: %w", err))
		}
		return nil, true, err
	}
	catalogCheckpoint, err := decodeCheckpoint(catalogBytes, snapshotKindCatalog, manifest)
	if err != nil || catalogCheckpoint.next != manifest.catalogNext {
		if err == nil {
			err = corrupt(errInvalidRecord, "catalog checkpoint coverage mismatch")
		}
		return nil, true, err
	}
	offsetsCheckpoint, err := decodeCheckpoint(offsetsBytes, snapshotKindOffsets, manifest)
	if err != nil || offsetsCheckpoint.next != manifest.offsetsNext {
		if err == nil {
			err = corrupt(errInvalidRecord, "offsets checkpoint coverage mismatch")
		}
		return nil, true, err
	}
	projection, err := decodeCatalogCheckpointPayload(catalogCheckpoint.payload, catalogCheckpoint.next, manifest.storeID)
	if err != nil {
		return nil, true, err
	}
	canonicalCatalog, catalogEntries, err := encodeCatalogCheckpointPayload(projection)
	if err != nil || catalogEntries != catalogCheckpoint.entries || !bytes.Equal(canonicalCatalog, catalogCheckpoint.payload) {
		if err == nil {
			err = corrupt(errInvalidRecord, "catalog checkpoint payload is not canonical")
		}
		return nil, true, err
	}
	catalogOptions := projection.config.options()
	catalogOptions.InitialOffset = manifest.catalogNext
	if err := preflightSystemLogSuffixDirectory(generationPartitionDir(rootPath, manifest.generation, true), api.ClusterMetadataTopicID(), 0, projection.config, manifest.catalogNext); err != nil {
		return nil, true, fmt.Errorf("preflight catalog suffix: %w", err)
	}
	catalog, err := openPartition(generationPartitionDir(rootPath, manifest.generation, true), api.ClusterMetadataTopicID(), 0, catalogOptions, manifest.storeID)
	if err != nil {
		return nil, true, err
	}
	if !sameEventAnchor(eventAnchor{ID: catalog.segments[0].header.ID, HeaderHash: catalog.segments[0].headerHash}, manifest.catalogAnchor) {
		_ = catalog.Close()
		return nil, true, corrupt(errInvalidSegment, "catalog suffix anchor mismatch")
	}
	if err := projection.replaySuffix(catalog); err != nil {
		_ = catalog.Close()
		return nil, true, err
	}
	offsetsOptions := projection.offsetsConfig.options()
	offsetsOptions.InitialOffset = manifest.offsetsNext
	if err := preflightSystemLogSuffixDirectory(generationPartitionDir(rootPath, manifest.generation, false), api.ConsumerOffsetsTopicID(), 0, projection.offsetsConfig, manifest.offsetsNext); err != nil {
		_ = catalog.Close()
		return nil, true, fmt.Errorf("preflight offsets suffix: %w", err)
	}
	offsets, err := openPartition(generationPartitionDir(rootPath, manifest.generation, false), api.ConsumerOffsetsTopicID(), 0, offsetsOptions, manifest.storeID)
	if err != nil {
		_ = catalog.Close()
		return nil, true, err
	}
	if !sameEventAnchor(eventAnchor{ID: offsets.segments[0].header.ID, HeaderHash: offsets.segments[0].headerHash}, manifest.offsetsAnchor) {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, true, corrupt(errInvalidSegment, "offsets suffix anchor mismatch")
	}
	offsetsState, err := decodeOffsetsCheckpointPayload(offsetsCheckpoint.payload, offsetsCheckpoint.next, manifest.storeID, projection)
	if err == nil {
		canonicalOffsets, entries, encodeErr := encodeOffsetsSnapshotPayload(offsetsState)
		if encodeErr != nil || entries != offsetsCheckpoint.entries || !bytes.Equal(canonicalOffsets, offsetsCheckpoint.payload) {
			if encodeErr == nil {
				encodeErr = corrupt(errInvalidRecord, "offsets checkpoint payload is not canonical")
			}
			err = encodeErr
		}
	}
	if err == nil {
		err = offsetsState.replaySuffix(offsets, projection)
	}
	if err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, true, err
	}
	if err := preflightTopicStorage(rootPath, projection); err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, true, err
	}
	if err := reconcileRetiredArtifacts(rootPath, projection); err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, true, err
	}
	if err := admitSystemHistory(catalog, offsets, limits); err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, true, err
	}
	diagnostics := validateOptionalSnapshots(manifest.storeID, catalog, offsets, projection, offsetsState)
	return &metadataBootstrap{catalog: catalog, offsets: offsets, projection: projection, offsetsState: offsetsState, snapshotDiagnostics: diagnostics, generation: manifest.generation}, true, nil
}

func (projection *catalogProjection) replaySuffix(partition *Partition) error {
	end, err := partition.EndOffset()
	if err != nil {
		return err
	}
	for offset := projection.revision; offset < end; {
		records, err := partition.Read(offset, MaxBatchRecords)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return corrupt(errInvalidRecord, "catalog suffix stopped before durable end")
		}
		for _, record := range records {
			if record.Offset != offset {
				return corrupt(errInvalidRecord, "catalog suffix offset gap")
			}
			if err := projection.apply(record); err != nil {
				return err
			}
			offset++
		}
	}
	return nil
}

func (projection *offsetsProjection) replaySuffix(partition *Partition, catalog *catalogProjection) error {
	end, err := partition.EndOffset()
	if err != nil {
		return err
	}
	for offset := projection.revision; offset < end; {
		records, err := partition.Read(offset, MaxBatchRecords)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return corrupt(errInvalidRecord, "offsets suffix stopped before durable end")
		}
		for _, record := range records {
			if record.Offset != offset {
				return corrupt(errInvalidRecord, "offsets suffix offset gap")
			}
			if err := projection.apply(record, catalog); err != nil {
				return err
			}
			offset++
		}
	}
	return nil
}
