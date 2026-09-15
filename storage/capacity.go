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
	"math"

	"github.com/ayeshLK/immulog/api"
)

// defaultSystemLogCapacityBytes is a logical history budget, not a claim about
// available filesystem space. System history is never reclaimed by user-topic
// retention.
const defaultSystemLogCapacityBytes = uint64(1 << 30)

func systemLogAdmission(partition *Partition, incoming, limit uint64) error {
	if partition == nil || len(partition.segments) == 0 {
		return errors.Join(api.ErrSystemLogCapacity, errors.New("system-log partition is unavailable"))
	}
	if limit == 0 {
		return errors.Join(api.ErrSystemLogCapacity, errors.New("system-log history limit is unavailable"))
	}
	if active := partition.segments[len(partition.segments)-1]; active.records > 0 && uint64(active.size)+incoming > partition.options.SegmentBytes {
		if incoming > math.MaxUint64-uint64(SegmentHeaderBytes) {
			return errors.Join(api.ErrSystemLogCapacity, errors.New("system-log growth arithmetic overflow"))
		}
		incoming += uint64(SegmentHeaderBytes)
	}
	var used uint64
	for _, segment := range partition.segments {
		if segment.size < 0 || uint64(segment.size) > math.MaxUint64-used {
			return errors.Join(api.ErrSystemLogCapacity, errors.New("system-log size arithmetic overflow"))
		}
		used += uint64(segment.size)
	}
	// A lowered new-growth ceiling does not make an existing complete history
	// unreadable. Recovery calls this with no requested growth.
	if incoming == 0 {
		return nil
	}
	if used > limit || incoming > limit-used {
		return errors.Join(api.ErrSystemLogCapacity, errors.New("system-log history capacity exhausted"))
	}
	return nil
}

func systemEventBatchBytes(topic api.TopicID, base uint64, value []byte) (uint64, error) {
	encoded, err := EncodeBatch(api.RecordBatch{
		Topic: topic, Partition: 0, BaseOffset: base,
		Records: []api.Record{{Topic: topic, Partition: 0, Offset: base, Value: value}},
	})
	if err != nil {
		return 0, err
	}
	return uint64(len(encoded)), nil
}

func systemEventAdmission(partition *Partition, topic api.TopicID, base uint64, value []byte, limit uint64) error {
	bytes, err := systemEventBatchBytes(topic, base, value)
	if err != nil {
		return err
	}
	return systemLogAdmission(partition, bytes, limit)
}
