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

const (
	maxGroupIDBytes      = 255
	maxAssignmentMembers = 4096
	maxSubscriptionKeys  = 65536
)

func validateOffsetsEvent(eventType EventType, payload []byte, storeID StoreID) error {
	cursor := eventCursor{data: payload}
	encodedStore, err := cursor.id()
	if err != nil {
		return err
	}
	if StoreID(encodedStore) != storeID {
		return corrupt(errInvalidRecord, "offset event StoreID mismatch")
	}
	if _, err := cursor.u64(); err != nil { // catalogNext
		return err
	}
	switch eventType {
	case EventGroupCreated:
		return validateGroupCreated(&cursor)
	case EventLocalAssignmentChanged:
		return validateLocalAssignmentChanged(&cursor)
	case EventOffsetCommitted:
		return validateOffsetCommitted(&cursor)
	default:
		return corrupt(errInvalidRecord, "invalid offsets event type")
	}
}

func validateGroupCreated(cursor *eventCursor) error {
	if _, err := cursor.stringValue(maxGroupIDBytes); err != nil {
		return err
	}
	startMode, err := cursor.u8()
	if err != nil {
		return err
	}
	if err := expectReserved(cursor); err != nil {
		return err
	}
	count, err := cursor.u32()
	if err != nil {
		return err
	}
	if count > maxSubscriptionKeys {
		return corrupt(errInvalidRecord, "group explicit start count exceeds limit")
	}
	if (startMode == 1 || startMode == 2) && count != 0 {
		return corrupt(errInvalidRecord, "group start mode has explicit starts")
	}
	if startMode == 3 && count == 0 {
		return corrupt(errInvalidRecord, "explicit group start mode has no starts")
	}
	if startMode < 1 || startMode > 3 {
		return unsupported("unsupported group start mode")
	}
	var previous topicKey
	for index := uint32(0); index < count; index++ {
		key, err := decodeTopicKey(cursor)
		if err != nil {
			return err
		}
		if index > 0 && compareTopicKey(previous, key) >= 0 {
			return corrupt(errInvalidRecord, "group explicit starts are not canonical")
		}
		previous = key
		if _, err := cursor.u64(); err != nil {
			return err
		}
	}
	return finishEvent(cursor)
}

func validateLocalAssignmentChanged(cursor *eventCursor) error {
	if _, err := cursor.stringValue(maxGroupIDBytes); err != nil {
		return err
	}
	if _, err := requireID(cursor, "assignment instance ID"); err != nil {
		return err
	}
	expected, err := cursor.u64()
	if err != nil {
		return err
	}
	next, err := cursor.u64()
	if err != nil {
		return err
	}
	if expected == ^uint64(0) || next != expected+1 {
		return corrupt(errInvalidRecord, "assignment generation does not advance by one")
	}
	reason, err := cursor.u8()
	if err != nil {
		return err
	}
	if reason < 1 || reason > 6 {
		return unsupported("unsupported assignment-change reason")
	}
	if err := expectReserved(cursor); err != nil {
		return err
	}
	members, err := cursor.u32()
	if err != nil {
		return err
	}
	if members > maxAssignmentMembers {
		return corrupt(errInvalidRecord, "assignment member count exceeds limit")
	}
	type member struct{ session [16]byte }
	memberIDs := make(map[[16]byte]struct{}, members)
	var previousSession [16]byte
	totalSubscriptions := uint64(0)
	for index := uint32(0); index < members; index++ {
		session, err := requireID(cursor, "assignment session ID")
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
		totalSubscriptions += uint64(subscriptions)
		if totalSubscriptions > maxSubscriptionKeys {
			return corrupt(errInvalidRecord, "subscription key count exceeds limit")
		}
		var previousKey topicKey
		for subscription := uint32(0); subscription < subscriptions; subscription++ {
			key, err := decodeTopicKey(cursor)
			if err != nil {
				return err
			}
			if subscription > 0 && compareTopicKey(previousKey, key) >= 0 {
				return corrupt(errInvalidRecord, "member subscriptions are not canonical")
			}
			previousKey = key
		}
	}
	assignments, err := cursor.u32()
	if err != nil {
		return err
	}
	if uint64(assignments) > totalSubscriptions {
		return corrupt(errInvalidRecord, "assignment count exceeds subscribed keys")
	}
	var previousKey topicKey
	assigned := make(map[topicKey]struct{}, assignments)
	for index := uint32(0); index < assignments; index++ {
		key, err := decodeTopicKey(cursor)
		if err != nil {
			return err
		}
		if index > 0 && compareTopicKey(previousKey, key) >= 0 {
			return corrupt(errInvalidRecord, "assignments are not canonical")
		}
		previousKey = key
		owner, err := requireID(cursor, "assignment owner session ID")
		if err != nil {
			return err
		}
		if _, exists := memberIDs[owner]; !exists {
			return corrupt(errInvalidRecord, "assignment owner is not a member")
		}
		if _, duplicate := assigned[key]; duplicate {
			return corrupt(errInvalidRecord, "duplicate assignment key")
		}
		assigned[key] = struct{}{}
		if _, err := cursor.u64(); err != nil { // resumeNext
			return err
		}
		action, err := cursor.u8()
		if err != nil {
			return err
		}
		if action > 1 {
			return unsupported("unsupported assignment baseline action")
		}
		if err := expectReserved(cursor); err != nil {
			return err
		}
		initial, err := cursor.u64()
		if err != nil {
			return err
		}
		if action == 0 && initial != 0 {
			return corrupt(errInvalidRecord, "reused assignment baseline has initial offset")
		}
	}
	return finishEvent(cursor)
}

func validateOffsetCommitted(cursor *eventCursor) error {
	if _, err := cursor.stringValue(maxGroupIDBytes); err != nil {
		return err
	}
	if _, err := requireID(cursor, "commit instance ID"); err != nil {
		return err
	}
	if _, err := requireID(cursor, "commit member session ID"); err != nil {
		return err
	}
	if _, err := cursor.u64(); err != nil {
		return err
	}
	if _, err := decodeTopicKey(cursor); err != nil {
		return err
	}
	previousKind, err := cursor.u8()
	if err != nil {
		return err
	}
	if previousKind > 1 {
		return unsupported("unsupported commit previous-state kind")
	}
	if err := expectReserved(cursor); err != nil {
		return err
	}
	if _, err := cursor.u64(); err != nil {
		return err
	}
	if _, err := cursor.u64(); err != nil {
		return err
	}
	return finishEvent(cursor)
}

type topicKey struct {
	topic     api.TopicID
	partition uint32
}

func decodeTopicKey(cursor *eventCursor) (topicKey, error) {
	id, err := cursor.id()
	if err != nil {
		return topicKey{}, err
	}
	if api.TopicID(id).IsZero() {
		return topicKey{}, corrupt(errInvalidRecord, "event topic key ID is zero")
	}
	partition, err := cursor.u32()
	if err != nil {
		return topicKey{}, err
	}
	if partition > int32Max {
		return topicKey{}, corrupt(errInvalidRecord, "event topic key partition is outside v1 range")
	}
	return topicKey{topic: api.TopicID(id), partition: partition}, nil
}

func compareTopicKey(left, right topicKey) int {
	if comparison := bytes.Compare(left.topic[:], right.topic[:]); comparison != 0 {
		return comparison
	}
	if left.partition < right.partition {
		return -1
	}
	if left.partition > right.partition {
		return 1
	}
	return 0
}

func requireID(cursor *eventCursor, name string) ([16]byte, error) {
	id, err := cursor.id()
	if err != nil {
		return id, err
	}
	zero := [16]byte{}
	if id == zero {
		return id, corrupt(errInvalidRecord, name+" is zero")
	}
	return id, nil
}

func expectReserved(cursor *eventCursor) error {
	value, err := cursor.take(3)
	if err != nil {
		return err
	}
	if value[0] != 0 || value[1] != 0 || value[2] != 0 {
		return corrupt(errInvalidRecord, "nonzero event reserved bytes")
	}
	return nil
}

func finishEvent(cursor *eventCursor) error {
	if cursor.remaining() != 0 {
		return corrupt(errInvalidRecord, "trailing system event bytes")
	}
	return nil
}
