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
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/ayeshLK/immulog/api"
)

const (
	eventMagic         = "IELE"
	eventEnvelopeLen   = 16
	maxTopicNameLen    = 249
	maxTopicPartitions = 65_536
)

type eventCursor struct {
	data []byte
	pos  int
}

func (cursor *eventCursor) remaining() int { return len(cursor.data) - cursor.pos }

func (cursor *eventCursor) take(size int) ([]byte, error) {
	if size < 0 || size > cursor.remaining() {
		return nil, corrupt(errInvalidRecord, "event field exceeds payload")
	}
	value := cursor.data[cursor.pos : cursor.pos+size]
	cursor.pos += size
	return value, nil
}

func (cursor *eventCursor) u8() (uint8, error) {
	value, err := cursor.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (cursor *eventCursor) u16() (uint16, error) {
	value, err := cursor.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(value), nil
}

func (cursor *eventCursor) u32() (uint32, error) {
	value, err := cursor.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (cursor *eventCursor) u64() (uint64, error) {
	value, err := cursor.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (cursor *eventCursor) id() ([16]byte, error) {
	value, err := cursor.take(16)
	var id [16]byte
	if err != nil {
		return id, err
	}
	copy(id[:], value)
	return id, nil
}

func (cursor *eventCursor) stringValue(max int) (string, error) {
	length, err := cursor.u32()
	if err != nil {
		return "", err
	}
	if length == 0 || uint64(length) > uint64(max) {
		return "", corrupt(errInvalidRecord, "event string length is outside limits")
	}
	value, err := cursor.take(int(length))
	if err != nil {
		return "", err
	}
	if bytesContainZero(value) || !utf8.Valid(value) {
		return "", corrupt(errInvalidRecord, "event string is not valid UTF-8")
	}
	return string(value), nil
}

func bytesContainZero(value []byte) bool {
	for _, character := range value {
		if character == 0 {
			return true
		}
	}
	return false
}

func appendU16(data []byte, value uint16) []byte {
	var encoded [2]byte
	binary.LittleEndian.PutUint16(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendU64(data []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendID(data []byte, value [16]byte) []byte { return append(data, value[:]...) }

func appendString(data []byte, value string, max int) ([]byte, error) {
	if len(value) == 0 || len(value) > max || bytesContainZero([]byte(value)) || !utf8.ValidString(value) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("event string is invalid"))
	}
	data = appendEventU32(data, uint32(len(value)))
	return append(data, value...), nil
}

func appendEventU32(data []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(data, encoded[:]...)
}

type eventAnchor struct {
	ID         api.SegmentID
	HeaderHash [32]byte
}

type storeInitializedEvent struct {
	StoreID        StoreID
	CatalogInitial eventAnchor
	OffsetsInitial eventAnchor
	CatalogConfig  PartitionConfigV1
	OffsetsConfig  PartitionConfigV1
}

type topicCreatedPartition struct {
	Partition uint32
	Config    PartitionConfigV1
	Initial   eventAnchor
}

type topicCreatedEvent struct {
	StoreID             StoreID
	ExpectedCatalogNext uint64
	TopicID             api.TopicID
	Name                string
	Partitions          []topicCreatedPartition
}

func encodeAnchor(data []byte, anchor eventAnchor) ([]byte, error) {
	if anchor.ID.IsZero() || anchor.HeaderHash == [32]byte{} {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("event anchor is incomplete"))
	}
	data = appendID(data, [16]byte(anchor.ID))
	return append(data, anchor.HeaderHash[:]...), nil
}

func decodeAnchor(cursor *eventCursor) (eventAnchor, error) {
	id, err := cursor.id()
	if err != nil {
		return eventAnchor{}, err
	}
	hash, err := cursor.take(32)
	if err != nil {
		return eventAnchor{}, err
	}
	var anchor eventAnchor
	copy(anchor.ID[:], id[:])
	copy(anchor.HeaderHash[:], hash)
	if anchor.ID.IsZero() || anchor.HeaderHash == [32]byte{} {
		return eventAnchor{}, corrupt(errInvalidRecord, "event anchor is zero")
	}
	return anchor, nil
}

func encodeStoreInitializedPayload(event storeInitializedEvent) ([]byte, error) {
	if event.StoreID == (StoreID{}) {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("store ID is zero"))
	}
	catalogConfig, err := encodePartitionConfig(event.CatalogConfig)
	if err != nil {
		return nil, err
	}
	offsetsConfig, err := encodePartitionConfig(event.OffsetsConfig)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, 240)
	data = append(data, event.StoreID[:]...)
	data, err = encodeAnchor(data, event.CatalogInitial)
	if err != nil {
		return nil, err
	}
	data, err = encodeAnchor(data, event.OffsetsInitial)
	if err != nil {
		return nil, err
	}
	data = append(data, catalogConfig...)
	data = append(data, offsetsConfig...)
	if len(data) != 240 {
		return nil, errors.New("internal StoreInitialized size mismatch")
	}
	return data, nil
}

func decodeStoreInitializedPayload(data []byte) (storeInitializedEvent, error) {
	if len(data) != 240 {
		return storeInitializedEvent{}, corrupt(errInvalidRecord, "StoreInitialized payload size mismatch")
	}
	cursor := eventCursor{data: data}
	var event storeInitializedEvent
	storeID, err := cursor.id()
	if err != nil {
		return event, err
	}
	copy(event.StoreID[:], storeID[:])
	if event.StoreID == (StoreID{}) {
		return event, corrupt(errInvalidRecord, "StoreInitialized store ID is zero")
	}
	if event.CatalogInitial, err = decodeAnchor(&cursor); err != nil {
		return event, err
	}
	if event.OffsetsInitial, err = decodeAnchor(&cursor); err != nil {
		return event, err
	}
	if event.CatalogConfig, err = decodePartitionConfig(&cursor, true); err != nil {
		return event, err
	}
	if event.OffsetsConfig, err = decodePartitionConfig(&cursor, true); err != nil {
		return event, err
	}
	if cursor.remaining() != 0 {
		return event, corrupt(errInvalidRecord, "trailing StoreInitialized payload bytes")
	}
	return event, nil
}

func encodeTopicCreatedPayload(event topicCreatedEvent) ([]byte, error) {
	if event.StoreID == (StoreID{}) || event.TopicID.IsZero() || event.TopicID == api.ClusterMetadataTopicID || event.TopicID == api.ConsumerOffsetsTopicID {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("invalid TopicCreated identity"))
	}
	if len(event.Partitions) == 0 || len(event.Partitions) > maxTopicPartitions {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("topic partition count is outside limits"))
	}
	data := make([]byte, 0, 256)
	data = append(data, event.StoreID[:]...)
	data = appendU64(data, event.ExpectedCatalogNext)
	data = appendID(data, [16]byte(event.TopicID))
	var err error
	data, err = appendString(data, event.Name, maxTopicNameLen)
	if err != nil {
		return nil, err
	}
	data = appendEventU32(data, uint32(len(event.Partitions)))
	for index, partition := range event.Partitions {
		if partition.Partition != uint32(index) {
			return nil, errors.Join(api.ErrInvalidArgument, errors.New("topic partitions must be canonical"))
		}
		data = appendEventU32(data, partition.Partition)
		config, err := encodePartitionConfig(partition.Config)
		if err != nil {
			return nil, err
		}
		data = append(data, config...)
		data, err = encodeAnchor(data, partition.Initial)
		if err != nil {
			return nil, err
		}
	}
	if len(data) > int(MaxRecordBytes)-RecordPrefixBytes-8-eventEnvelopeLen {
		return nil, errors.Join(api.ErrRecordTooLarge, errors.New("TopicCreated payload is too large"))
	}
	return data, nil
}

func decodeTopicCreatedPayload(data []byte) (topicCreatedEvent, error) {
	cursor := eventCursor{data: data}
	var event topicCreatedEvent
	storeID, err := cursor.id()
	if err != nil {
		return event, err
	}
	copy(event.StoreID[:], storeID[:])
	if event.StoreID == (StoreID{}) {
		return event, corrupt(errInvalidRecord, "TopicCreated store ID is zero")
	}
	if event.ExpectedCatalogNext == 0 {
		if event.ExpectedCatalogNext, err = cursor.u64(); err != nil {
			return event, err
		}
	} else {
		return event, corrupt(errInvalidRecord, "internal TopicCreated cursor state")
	}
	topicID, err := cursor.id()
	if err != nil {
		return event, err
	}
	copy(event.TopicID[:], topicID[:])
	if event.TopicID.IsZero() || event.TopicID == api.ClusterMetadataTopicID || event.TopicID == api.ConsumerOffsetsTopicID {
		return event, corrupt(errInvalidRecord, "TopicCreated topic ID is reserved or zero")
	}
	if event.Name, err = cursor.stringValue(maxTopicNameLen); err != nil {
		return event, err
	}
	if !validTopicName(event.Name) {
		return event, corrupt(errInvalidRecord, "TopicCreated name is invalid")
	}
	count, err := cursor.u32()
	if err != nil {
		return event, err
	}
	if count == 0 || count > maxTopicPartitions {
		return event, corrupt(errInvalidRecord, "TopicCreated partition count is outside limits")
	}
	event.Partitions = make([]topicCreatedPartition, count)
	for index := range event.Partitions {
		partition, err := cursor.u32()
		if err != nil {
			return event, err
		}
		if partition != uint32(index) {
			return event, corrupt(errInvalidRecord, "TopicCreated partitions are not canonical")
		}
		config, err := decodePartitionConfig(&cursor, false)
		if err != nil {
			return event, err
		}
		anchor, err := decodeAnchor(&cursor)
		if err != nil {
			return event, err
		}
		event.Partitions[index] = topicCreatedPartition{Partition: partition, Config: config, Initial: anchor}
	}
	if cursor.remaining() != 0 {
		return event, corrupt(errInvalidRecord, "trailing TopicCreated payload bytes")
	}
	return event, nil
}

func encodeSystemEvent(eventType EventType, payload []byte) ([]byte, error) {
	if eventType == 0 || len(payload) > math.MaxUint32 {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("invalid system event"))
	}
	value := make([]byte, eventEnvelopeLen+len(payload))
	copy(value[0:4], eventMagic)
	binary.LittleEndian.PutUint16(value[4:6], uint16(eventType))
	binary.LittleEndian.PutUint16(value[6:8], 1)
	binary.LittleEndian.PutUint32(value[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(value[12:16], 0)
	copy(value[eventEnvelopeLen:], payload)
	return value, nil
}

func decodeSystemEvent(record api.Record, expectedTopic api.TopicID) (EventType, []byte, error) {
	if record.Topic != expectedTopic || record.Partition != 0 || record.Key != nil || len(record.Headers) != 0 || record.Value == nil {
		return 0, nil, corrupt(errInvalidRecord, "invalid system event record shape")
	}
	if len(record.Value) < eventEnvelopeLen || string(record.Value[:4]) != eventMagic {
		return 0, nil, corrupt(errInvalidRecord, "invalid system event envelope")
	}
	eventType := EventType(binary.LittleEndian.Uint16(record.Value[4:6]))
	version := binary.LittleEndian.Uint16(record.Value[6:8])
	payloadBytes := binary.LittleEndian.Uint32(record.Value[8:12])
	flags := binary.LittleEndian.Uint32(record.Value[12:16])
	if eventType == 0 || version == 0 {
		return 0, nil, corrupt(errInvalidRecord, "zero system event type or schema")
	}
	if version != 1 || flags != 0 {
		return 0, nil, unsupported("unsupported system event schema or flags")
	}
	if uint64(payloadBytes) != uint64(len(record.Value)-eventEnvelopeLen) {
		return 0, nil, corrupt(errInvalidRecord, "system event payload length mismatch")
	}
	if !eventBelongsToTopic(eventType, expectedTopic) {
		return 0, nil, corrupt(errInvalidRecord, "system event belongs to another log")
	}
	return eventType, record.Value[eventEnvelopeLen:], nil
}

func eventBelongsToTopic(eventType EventType, topic api.TopicID) bool {
	if topic == api.ClusterMetadataTopicID {
		return eventType == EventStoreInitialized || eventType == EventTopicCreated || eventType == EventPartitionLogStartAdvanced
	}
	if topic == api.ConsumerOffsetsTopicID {
		return eventType == EventGroupCreated || eventType == EventLocalAssignmentChanged || eventType == EventOffsetCommitted
	}
	return false
}

func validTopicName(name string) bool {
	if len(name) == 0 || len(name) > maxTopicNameLen || strings.HasPrefix(name, "__") || name == "." || name == ".." {
		return false
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') && !(character >= '0' && character <= '9') && character != '.' && character != '_' && character != '-' {
			return false
		}
	}
	return true
}
