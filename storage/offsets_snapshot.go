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
	"sort"

	"github.com/ayeshLK/immulog/api"
)

const maxSnapshotBodyBytes = int(MaxRecordBytes) - 44

func encodeOffsetsSnapshotPayload(projection *offsetsProjection) ([]byte, uint64, error) {
	if projection == nil {
		return nil, 0, errors.New("offset projection is unavailable")
	}
	groupIDs := make([]string, 0, len(projection.groups))
	for groupID := range projection.groups {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Strings(groupIDs)
	if uint64(len(groupIDs)) > maxSnapshotEntries {
		return nil, 0, errors.Join(api.ErrResourceLimit, errors.New("offset snapshot group count exceeds bounds"))
	}
	payload := appendU32(nil, uint32(len(groupIDs)))
	entries := uint64(0)
	for _, groupID := range groupIDs {
		group := projection.groups[groupID]
		if group == nil || len(group.CreationBody) == 0 || len(group.CreationBody) > maxSnapshotBodyBytes {
			return nil, 0, errors.Join(api.ErrCorruptLog, errors.New("offset snapshot group body is invalid"))
		}
		creationEntries, err := countGroupBody(group.CreationBody)
		if err != nil {
			return nil, 0, err
		}
		assignmentEntries := uint64(0)
		if len(group.AssignmentBody) != 0 {
			if len(group.AssignmentBody) > maxSnapshotBodyBytes {
				return nil, 0, errors.Join(api.ErrCorruptLog, errors.New("offset snapshot assignment body is too large"))
			}
			assignmentEntries, err = countAssignmentBody(group.AssignmentBody)
			if err != nil {
				return nil, 0, err
			}
		}
		keys := make([]topicKey, 0, len(group.Progress))
		for key := range group.Progress {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return compareTopicKey(keys[i], keys[j]) < 0 })
		if uint64(len(keys)) > maxSnapshotEntries {
			return nil, 0, errors.Join(api.ErrResourceLimit, errors.New("offset snapshot progress count exceeds bounds"))
		}
		payload = appendU64(payload, group.CreationOffset)
		payload = appendU32(payload, uint32(len(group.CreationBody)))
		payload = append(payload, group.CreationBody...)
		payload = appendU64(payload, group.Generation)
		payload = appendU64(payload, group.AssignmentOffset)
		payload = appendU32(payload, uint32(len(group.AssignmentBody)))
		payload = append(payload, group.AssignmentBody...)
		payload = appendU32(payload, uint32(len(keys)))
		for _, key := range keys {
			progress := group.Progress[key]
			payload = appendID(payload, [16]byte(key.topic))
			payload = appendU32(payload, key.partition)
			payload = appendU64(payload, progress.InitialNext)
			if progress.HasCommit {
				payload = append(payload, 1, 0, 0, 0)
			} else {
				payload = append(payload, 0, 0, 0, 0)
			}
			payload = appendU64(payload, progress.CommittedNext)
		}
		entries += 1 + creationEntries + assignmentEntries + uint64(len(keys))
		if entries > maxSnapshotEntries || uint64(len(payload)) > maxSnapshotPayload {
			return nil, 0, errors.Join(api.ErrResourceLimit, errors.New("offset snapshot exceeds bounds"))
		}
	}
	return payload, entries, nil
}

func countGroupBody(data []byte) (uint64, error) {
	cursor := eventCursor{data: data}
	if _, err := cursor.id(); err != nil {
		return 0, err
	}
	if _, err := cursor.u64(); err != nil {
		return 0, err
	}
	if _, err := cursor.stringValue(maxGroupIDBytes); err != nil {
		return 0, err
	}
	mode, err := cursor.u8()
	if err != nil {
		return 0, err
	}
	if mode < 1 || mode > 3 {
		return 0, unsupported("unsupported group start mode")
	}
	if err := expectReserved(&cursor); err != nil {
		return 0, err
	}
	count, err := cursor.u32()
	if err != nil {
		return 0, err
	}
	for index := uint32(0); index < count; index++ {
		if _, err := decodeTopicKey(&cursor); err != nil {
			return 0, err
		}
		if _, err := cursor.u64(); err != nil {
			return 0, err
		}
	}
	if err := finishEvent(&cursor); err != nil {
		return 0, err
	}
	return uint64(count), nil
}

func countAssignmentBody(data []byte) (uint64, error) {
	cursor := eventCursor{data: data}
	if _, err := cursor.id(); err != nil {
		return 0, err
	}
	if _, err := cursor.u64(); err != nil {
		return 0, err
	}
	if _, err := cursor.stringValue(maxGroupIDBytes); err != nil {
		return 0, err
	}
	if _, err := requireID(&cursor, "assignment instance ID"); err != nil {
		return 0, err
	}
	if _, err := cursor.u64(); err != nil {
		return 0, err
	}
	if _, err := cursor.u64(); err != nil {
		return 0, err
	}
	if _, err := cursor.u8(); err != nil {
		return 0, err
	}
	if err := expectReserved(&cursor); err != nil {
		return 0, err
	}
	members, err := cursor.u32()
	if err != nil {
		return 0, err
	}
	entries := uint64(members)
	for member := uint32(0); member < members; member++ {
		if _, err := requireID(&cursor, "assignment session ID"); err != nil {
			return 0, err
		}
		subscriptions, err := cursor.u32()
		if err != nil {
			return 0, err
		}
		entries += uint64(subscriptions)
		for subscription := uint32(0); subscription < subscriptions; subscription++ {
			if _, err := decodeTopicKey(&cursor); err != nil {
				return 0, err
			}
		}
	}
	assignments, err := cursor.u32()
	if err != nil {
		return 0, err
	}
	entries += uint64(assignments)
	for assignment := uint32(0); assignment < assignments; assignment++ {
		if _, err := decodeTopicKey(&cursor); err != nil {
			return 0, err
		}
		if _, err := requireID(&cursor, "assignment owner session ID"); err != nil {
			return 0, err
		}
		if _, err := cursor.u64(); err != nil {
			return 0, err
		}
		if _, err := cursor.u8(); err != nil {
			return 0, err
		}
		if err := expectReserved(&cursor); err != nil {
			return 0, err
		}
		if _, err := cursor.u64(); err != nil {
			return 0, err
		}
	}
	if err := finishEvent(&cursor); err != nil {
		return 0, err
	}
	return entries, nil
}
