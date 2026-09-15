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

	"github.com/ayeshLK/immulog/api"
)

type offsetProgress struct {
	InitialNext   uint64
	HasCommit     bool
	CommittedNext uint64
}

type offsetGroup struct {
	CreationOffset     uint64
	CreationBody       []byte
	Generation         uint64
	AssignmentOffset   uint64
	AssignmentBody     []byte
	AssignmentOwner    map[topicKey][16]byte
	AssignmentMember   map[[16]byte]struct{}
	AssignmentInstance [16]byte
	CreationStartMode  uint8
	ExplicitStarts     map[topicKey]uint64
	Progress           map[topicKey]offsetProgress
}

type offsetsProjection struct {
	storeID  StoreID
	revision uint64
	groups   map[string]*offsetGroup
}

func newOffsetsProjection(storeID StoreID) *offsetsProjection {
	return &offsetsProjection{storeID: storeID, groups: make(map[string]*offsetGroup)}
}

func replayOffsetsHistory(partition *Partition, storeID StoreID, catalog *catalogProjection) (*offsetsProjection, error) {
	projection := newOffsetsProjection(storeID)
	end, err := partition.EndOffset()
	if err != nil {
		return nil, err
	}
	for offset := uint64(0); offset < end; {
		records, err := partition.Read(offset, MaxBatchRecords)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return nil, corrupt(errInvalidRecord, "offset replay stopped before durable end")
		}
		for _, record := range records {
			if record.Offset != offset {
				return nil, corrupt(errInvalidRecord, "offset replay gap")
			}
			if err := projection.apply(record, catalog); err != nil {
				return nil, err
			}
			offset++
		}
	}
	return projection, nil
}

func (projection *offsetsProjection) apply(record api.Record, catalog *catalogProjection) error {
	eventType, payload, err := decodeSystemEvent(record, api.ConsumerOffsetsTopicID)
	if err != nil {
		return err
	}
	cursor := eventCursor{data: payload}
	encodedStore, err := cursor.id()
	if err != nil {
		return err
	}
	if StoreID(encodedStore) != projection.storeID {
		return corrupt(errInvalidRecord, "offset event StoreID mismatch")
	}
	catalogNext, err := cursor.u64()
	if err != nil {
		return err
	}
	if catalog == nil || catalogNext == 0 || catalogNext > catalog.revision {
		return corrupt(errInvalidRecord, "offset event catalog revision is unavailable")
	}
	switch eventType {
	case EventGroupCreated:
		return projection.applyGroupCreated(record.Offset, payload, cursor, catalog, catalogNext)
	case EventLocalAssignmentChanged:
		return projection.applyAssignment(record.Offset, payload, cursor, catalog, catalogNext)
	case EventOffsetCommitted:
		return projection.applyCommit(record.Offset, cursor, catalog, catalogNext)
	default:
		return corrupt(errInvalidRecord, "invalid offsets event type")
	}
}

func (projection *offsetsProjection) applyGroupCreated(offset uint64, payload []byte, cursor eventCursor, catalog *catalogProjection, catalogNext uint64) error {
	groupID, err := cursor.stringValue(maxGroupIDBytes)
	if err != nil {
		return err
	}
	if _, exists := projection.groups[groupID]; exists {
		return corrupt(errInvalidRecord, "duplicate GroupCreated event")
	}
	startMode, err := cursor.u8()
	if err != nil {
		return err
	}
	if startMode < 1 || startMode > 3 {
		return unsupported("unsupported group start mode")
	}
	if err := expectReserved(&cursor); err != nil {
		return err
	}
	count, err := cursor.u32()
	if err != nil {
		return err
	}
	if (startMode == 1 || startMode == 2) && count != 0 || startMode == 3 && count == 0 {
		return corrupt(errInvalidRecord, "group start policy and explicit starts disagree")
	}
	explicit := make(map[topicKey]uint64, count)
	var previous topicKey
	for index := uint32(0); index < count; index++ {
		key, err := decodeTopicKey(&cursor)
		if err != nil {
			return err
		}
		if index > 0 && compareTopicKey(previous, key) >= 0 {
			return corrupt(errInvalidRecord, "group explicit starts are not canonical")
		}
		if _, err := catalogPartition(catalog, key, catalogNext); err != nil {
			return err
		}
		next, err := cursor.u64()
		if err != nil {
			return err
		}
		explicit[key] = next
		previous = key
	}
	if err := finishEvent(&cursor); err != nil {
		return err
	}
	projection.groups[groupID] = &offsetGroup{
		CreationOffset: offset, CreationBody: append([]byte(nil), payload...),
		CreationStartMode: startMode, ExplicitStarts: explicit,
		AssignmentOwner: make(map[topicKey][16]byte), AssignmentMember: make(map[[16]byte]struct{}),
		Progress: make(map[topicKey]offsetProgress),
	}
	projection.revision = offset + 1
	return nil
}

func (projection *offsetsProjection) applyAssignment(offset uint64, payload []byte, cursor eventCursor, catalog *catalogProjection, catalogNext uint64) error {
	groupID, err := cursor.stringValue(maxGroupIDBytes)
	if err != nil {
		return err
	}
	group := projection.groups[groupID]
	if group == nil {
		return corrupt(errInvalidRecord, "assignment references an unknown group")
	}
	progressState := make(map[topicKey]offsetProgress, len(group.Progress))
	for key, progress := range group.Progress {
		progressState[key] = progress
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
	if err != nil {
		return err
	}
	if expected != group.Generation || newGeneration != expected+1 {
		return corrupt(errInvalidRecord, "assignment generation does not match projected state")
	}
	if _, err := cursor.u8(); err != nil {
		return err
	}
	if err := expectReserved(&cursor); err != nil {
		return err
	}
	members, err := cursor.u32()
	if err != nil {
		return err
	}
	memberIDs := make(map[[16]byte]struct{}, members)
	subscribed := make(map[topicKey][16]byte)
	var previousSession [16]byte
	for index := uint32(0); index < members; index++ {
		session, err := requireID(&cursor, "assignment session ID")
		if err != nil {
			return err
		}
		if index > 0 && bytes.Compare(previousSession[:], session[:]) >= 0 {
			return corrupt(errInvalidRecord, "assignment members are not canonical")
		}
		previousSession = session
		memberIDs[session] = struct{}{}
		subscriptions, err := cursor.u32()
		if err != nil {
			return err
		}
		var previousKey topicKey
		for subscription := uint32(0); subscription < subscriptions; subscription++ {
			key, err := decodeTopicKey(&cursor)
			if err != nil {
				return err
			}
			if subscription > 0 && compareTopicKey(previousKey, key) >= 0 {
				return corrupt(errInvalidRecord, "member subscriptions are not canonical")
			}
			if _, err := catalogPartition(catalog, key, catalogNext); err != nil {
				return err
			}
			if _, duplicate := subscribed[key]; duplicate {
				return corrupt(errInvalidRecord, "subscription key has multiple owners")
			}
			subscribed[key] = session
			previousKey = key
		}
	}
	assignments, err := cursor.u32()
	if err != nil {
		return err
	}
	owners := make(map[topicKey][16]byte, assignments)
	var previousKey topicKey
	for index := uint32(0); index < assignments; index++ {
		key, err := decodeTopicKey(&cursor)
		if err != nil {
			return err
		}
		if index > 0 && compareTopicKey(previousKey, key) >= 0 {
			return corrupt(errInvalidRecord, "assignments are not canonical")
		}
		owner, err := requireID(&cursor, "assignment owner session ID")
		if err != nil {
			return err
		}
		if subscribed[key] != owner {
			return corrupt(errInvalidRecord, "assignment owner is not the key subscriber")
		}
		resume, err := cursor.u64()
		if err != nil {
			return err
		}
		action, err := cursor.u8()
		if err != nil {
			return err
		}
		if action > 1 {
			return unsupported("unsupported assignment baseline action")
		}
		if err := expectReserved(&cursor); err != nil {
			return err
		}
		initial, err := cursor.u64()
		if err != nil {
			return err
		}
		progress, exists := progressState[key]
		if action == 1 {
			if exists || initial != resume {
				return corrupt(errInvalidRecord, "assignment creates an existing baseline")
			}
			progressState[key] = offsetProgress{InitialNext: initial}
		} else if initial != 0 || !exists || resume != latestNext(progress) {
			return corrupt(errInvalidRecord, "assignment baseline reuse does not match state")
		}
		owners[key] = owner
		previousKey = key
	}
	if len(owners) != len(subscribed) {
		return corrupt(errInvalidRecord, "assignment does not cover every subscription")
	}
	if err := finishEvent(&cursor); err != nil {
		return err
	}
	group.Progress = progressState
	group.Generation = newGeneration
	group.AssignmentOffset = offset
	group.AssignmentBody = append([]byte(nil), payload...)
	group.AssignmentOwner = owners
	group.AssignmentMember = memberIDs
	group.AssignmentInstance = instance
	projection.revision = offset + 1
	return nil
}

func (projection *offsetsProjection) applyCommit(offset uint64, cursor eventCursor, catalog *catalogProjection, catalogNext uint64) error {
	groupID, err := cursor.stringValue(maxGroupIDBytes)
	if err != nil {
		return err
	}
	group := projection.groups[groupID]
	if group == nil {
		return corrupt(errInvalidRecord, "commit references an unknown group")
	}
	instance, err := requireID(&cursor, "commit instance ID")
	if err != nil {
		return err
	}
	session, err := requireID(&cursor, "commit member session ID")
	if err != nil {
		return err
	}
	generation, err := cursor.u64()
	if err != nil {
		return err
	}
	key, err := decodeTopicKey(&cursor)
	if err != nil {
		return err
	}
	previousKind, err := cursor.u8()
	if err != nil {
		return err
	}
	if previousKind > 1 {
		return unsupported("unsupported commit previous-state kind")
	}
	if err := expectReserved(&cursor); err != nil {
		return err
	}
	expectedPrevious, err := cursor.u64()
	if err != nil {
		return err
	}
	next, err := cursor.u64()
	if err != nil {
		return err
	}
	if err := finishEvent(&cursor); err != nil {
		return err
	}
	if _, err := catalogPartition(catalog, key, catalogNext); err != nil {
		return err
	}
	progress, exists := group.Progress[key]
	owner, assigned := group.AssignmentOwner[key]
	if generation != group.Generation || !exists || !assigned || owner != session || instance != group.AssignmentInstance {
		return corrupt(errInvalidRecord, "commit ownership token does not match assignment")
	}
	if previousKind == 0 {
		if progress.HasCommit || expectedPrevious != progress.InitialNext {
			return corrupt(errInvalidRecord, "commit baseline does not match state")
		}
	} else if !progress.HasCommit || expectedPrevious != progress.CommittedNext {
		return corrupt(errInvalidRecord, "commit previous position does not match state")
	}
	if next < expectedPrevious {
		return corrupt(errInvalidRecord, "commit regresses position")
	}
	progress.HasCommit = true
	progress.CommittedNext = next
	group.Progress[key] = progress
	projection.revision = offset + 1
	return nil
}

func latestNext(progress offsetProgress) uint64 {
	if progress.HasCommit {
		return progress.CommittedNext
	}
	return progress.InitialNext
}

func catalogPartition(catalog *catalogProjection, key topicKey, catalogNext uint64) (TopicPartition, error) {
	topic := catalog.topicsByID[key.topic]
	if topic == nil || key.partition >= uint32(len(topic.descriptor.Partitions)) {
		return TopicPartition{}, corrupt(errInvalidRecord, "offset event references an unknown topic partition")
	}
	if topic.creationOffset+1 > catalogNext {
		return TopicPartition{}, corrupt(errInvalidRecord, "offset event references a topic created after its catalog revision")
	}
	return topic.descriptor.Partitions[key.partition], nil
}
