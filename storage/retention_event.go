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

	"github.com/ayeshLK/immulog/api"
)

type retiredSegmentEvent struct {
	Base   uint64
	End    uint64
	Bytes  uint64
	Anchor eventAnchor
}

type partitionLogStartAdvancedEvent struct {
	StoreID                StoreID
	ExpectedCatalogNext    uint64
	Key                    topicKey
	ExpectedOldL           uint64
	NewL                   uint64
	EvaluatedDurableEnd    uint64
	RetainedAnchor         eventAnchor
	ReasonMask             uint8
	EvaluatedUnixMillis    int64
	EvaluatedRetainedBytes uint64
	Retired                []retiredSegmentEvent
}

func encodePartitionLogStartAdvancedPayload(event partitionLogStartAdvancedEvent) ([]byte, error) {
	if event.StoreID == (StoreID{}) || event.Key.topic.IsZero() || event.ExpectedOldL >= event.NewL || event.NewL > event.EvaluatedDurableEnd {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("invalid retention boundary transition"))
	}
	if event.ReasonMask == 0 || event.ReasonMask&^uint8(3) != 0 || len(event.Retired) == 0 || len(event.Retired) > maxTopicPartitions {
		return nil, errors.Join(api.ErrInvalidArgument, errors.New("invalid retention boundary reason or retirement list"))
	}
	data := make([]byte, 0, 160+len(event.Retired)*72)
	data = append(data, event.StoreID[:]...)
	data = appendU64(data, event.ExpectedCatalogNext)
	data = appendID(data, [16]byte(event.Key.topic))
	data = appendU32(data, event.Key.partition)
	data = appendU64(data, event.ExpectedOldL)
	data = appendU64(data, event.NewL)
	data = appendU64(data, event.EvaluatedDurableEnd)
	encodedAnchor, err := encodeAnchor(data, event.RetainedAnchor)
	if err != nil {
		return nil, err
	}
	data = encodedAnchor
	data = append(data, event.ReasonMask, 0, 0, 0)
	data = appendU64(data, uint64(event.EvaluatedUnixMillis))
	data = appendU64(data, event.EvaluatedRetainedBytes)
	data = appendU32(data, uint32(len(event.Retired)))
	var previous uint64
	for index, retired := range event.Retired {
		if retired.Base >= retired.End || (index > 0 && retired.Base <= previous) {
			return nil, errors.Join(api.ErrInvalidArgument, errors.New("retired segments are not canonical"))
		}
		data = appendU64(data, retired.Base)
		data = appendU64(data, retired.End)
		data = appendU64(data, retired.Bytes)
		data, err = encodeAnchor(data, retired.Anchor)
		if err != nil {
			return nil, err
		}
		previous = retired.Base
	}
	if len(data) > int(MaxRecordBytes)-RecordPrefixBytes-8-eventEnvelopeLen {
		return nil, errors.Join(api.ErrRecordTooLarge, errors.New("retention event is too large"))
	}
	return data, nil
}

func decodePartitionLogStartAdvancedPayload(data []byte) (partitionLogStartAdvancedEvent, error) {
	cursor := eventCursor{data: data}
	var event partitionLogStartAdvancedEvent
	storeID, err := cursor.id()
	if err != nil {
		return event, err
	}
	copy(event.StoreID[:], storeID[:])
	if event.StoreID == (StoreID{}) {
		return event, corrupt(errInvalidRecord, "retention event StoreID is zero")
	}
	if event.ExpectedCatalogNext, err = cursor.u64(); err != nil {
		return event, err
	}
	if event.Key, err = decodeTopicKey(&cursor); err != nil {
		return event, err
	}
	if event.ExpectedOldL, err = cursor.u64(); err != nil {
		return event, err
	}
	if event.NewL, err = cursor.u64(); err != nil {
		return event, err
	}
	if event.EvaluatedDurableEnd, err = cursor.u64(); err != nil {
		return event, err
	}
	if event.RetainedAnchor, err = decodeAnchor(&cursor); err != nil {
		return event, err
	}
	if event.ReasonMask, err = cursor.u8(); err != nil {
		return event, err
	}
	if event.ReasonMask == 0 || event.ReasonMask&^uint8(3) != 0 {
		return event, unsupported("unsupported retention event reason")
	}
	if err := expectReserved(&cursor); err != nil {
		return event, err
	}
	timestamp, err := cursor.u64()
	if err != nil {
		return event, err
	}
	event.EvaluatedUnixMillis = int64(timestamp)
	if event.EvaluatedRetainedBytes, err = cursor.u64(); err != nil {
		return event, err
	}
	count, err := cursor.u32()
	if err != nil {
		return event, err
	}
	if count == 0 || count > maxTopicPartitions {
		return event, corrupt(errInvalidRecord, "retention retirement count is outside limits")
	}
	event.Retired = make([]retiredSegmentEvent, count)
	var previous uint64
	for index := range event.Retired {
		retired := retiredSegmentEvent{}
		if retired.Base, err = cursor.u64(); err != nil {
			return event, err
		}
		if retired.End, err = cursor.u64(); err != nil {
			return event, err
		}
		if retired.Bytes, err = cursor.u64(); err != nil {
			return event, err
		}
		if retired.Anchor, err = decodeAnchor(&cursor); err != nil {
			return event, err
		}
		if retired.Base >= retired.End || (index > 0 && retired.Base <= previous) {
			return event, corrupt(errInvalidRecord, "retention retirement list is not canonical")
		}
		previous = retired.Base
		event.Retired[index] = retired
	}
	if event.ExpectedOldL >= event.NewL || event.NewL > event.EvaluatedDurableEnd || cursor.remaining() != 0 {
		return event, corrupt(errInvalidRecord, "invalid retention boundary transition")
	}
	return event, nil
}
