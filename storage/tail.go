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
	"sync"

	"github.com/ayeshLK/immulog/api"
)

const (
	maxTailSlots = uint32(256)
	maxTailBytes = uint64(256 << 20)
)

// tailBudget bounds the aggregate bytes held by all live partition tails in one
// Store. It is an acceleration-only budget: a failed reservation drops an offer.
type tailBudget struct {
	mu    sync.Mutex
	limit uint64
	bytes uint64
}

func newTailBudget(limit uint64) *tailBudget {
	return &tailBudget{limit: limit}
}

func (budget *tailBudget) tryReplaceLocked(removed, added uint64) bool {
	if removed > budget.bytes {
		return false
	}
	afterRemoval := budget.bytes - removed
	if added > budget.limit-afterRemoval {
		return false
	}
	budget.bytes = afterRemoval + added
	return true
}

func (budget *tailBudget) release(bytes uint64) {
	if budget == nil || bytes == 0 {
		return
	}
	budget.mu.Lock()
	if bytes >= budget.bytes {
		budget.bytes = 0
	} else {
		budget.bytes -= bytes
	}
	budget.mu.Unlock()
}

func (budget *tailBudget) used() uint64 {
	if budget == nil {
		return 0
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return budget.bytes
}

// partitionTail is a disposable, bounded cache of already-durable record
// batches. It has no authority over offsets or retained data: every miss falls
// back to the segment reader at the caller's requested next offset.
type partitionTail struct {
	mu     sync.Mutex
	slots  []tailSlot
	head   uint32
	count  uint32
	bytes  uint64
	limit  uint64
	budget *tailBudget
	closed bool
}

type tailSlot struct {
	batch *deliveredBatch
}

type deliveredBatch struct {
	topic      api.TopicID
	partition  uint32
	base       uint64
	end        uint64
	durableEnd uint64
	bytes      uint64
	records    []api.Record
}

func newPartitionTail(options PartitionOptions) (*partitionTail, error) {
	if options.TailSlots == 0 && options.TailBytes == 0 {
		return nil, nil
	}
	if options.TailSlots == 0 || options.TailBytes == 0 {
		return nil, invalidArgument(errInvalidBatch, "tail slots and bytes must be configured together")
	}
	if options.TailSlots > maxTailSlots || options.TailBytes > maxTailBytes {
		return nil, errors.Join(api.ErrResourceLimit, errors.New("tail configuration exceeds runtime limits"))
	}
	return &partitionTail{slots: make([]tailSlot, options.TailSlots), limit: options.TailBytes}, nil
}

func (tail *partitionTail) attachBudget(budget *tailBudget) {
	if tail == nil {
		return
	}
	tail.mu.Lock()
	defer tail.mu.Unlock()
	if tail.closed || tail.count != 0 {
		return
	}
	tail.budget = budget
}

// offer never waits for a reader or shared-budget holder. A contended lock or
// exhausted budget intentionally becomes a cache gap that Fetch resolves from
// durable segments.
func (tail *partitionTail) offer(topic api.TopicID, partition uint32, records []api.Record, durableEnd uint64) {
	if tail == nil || len(records) == 0 || !tail.mu.TryLock() {
		return
	}
	defer tail.mu.Unlock()
	if tail.closed {
		return
	}

	batch, ok := newDeliveredBatch(topic, partition, records, durableEnd)
	if !ok || batch.bytes > tail.limit {
		return
	}
	evictions, removed, ok := tail.evictionPlan(batch.bytes)
	if !ok {
		return
	}
	if tail.budget != nil {
		if !tail.budget.mu.TryLock() {
			return
		}
		defer tail.budget.mu.Unlock()
		if !tail.budget.tryReplaceLocked(removed, batch.bytes) {
			return
		}
	}
	tail.evict(evictions)
	index := (tail.head + tail.count) % uint32(len(tail.slots))
	tail.slots[index].batch = batch
	tail.count++
	tail.bytes += batch.bytes
}

func (tail *partitionTail) evictionPlan(incoming uint64) (uint32, uint64, bool) {
	if incoming > tail.limit || len(tail.slots) == 0 {
		return 0, 0, false
	}
	count, bytes := tail.count, tail.bytes
	var evictions uint32
	var removed uint64
	for count == uint32(len(tail.slots)) || bytes > tail.limit-incoming {
		if count == 0 {
			return 0, 0, false
		}
		batch := tail.slots[(tail.head+evictions)%uint32(len(tail.slots))].batch
		if batch == nil || batch.bytes > bytes {
			return 0, 0, false
		}
		bytes -= batch.bytes
		removed += batch.bytes
		count--
		evictions++
	}
	return evictions, removed, true
}

func (tail *partitionTail) evict(count uint32) {
	for count != 0 {
		slot := &tail.slots[tail.head]
		tail.bytes -= slot.batch.bytes
		slot.batch = nil
		tail.head = (tail.head + 1) % uint32(len(tail.slots))
		tail.count--
		count--
	}
}

func (tail *partitionTail) close() {
	if tail == nil {
		return
	}
	tail.mu.Lock()
	defer tail.mu.Unlock()
	if tail.closed {
		return
	}
	tail.closed = true
	released := tail.bytes
	for index := range tail.slots {
		tail.slots[index].batch = nil
	}
	tail.head = 0
	tail.count = 0
	tail.bytes = 0
	if tail.budget != nil {
		tail.budget.release(released)
	}
}

// clear drops cached batches after a retained-boundary transition. The cache is
// optional and must never make expired records observable.
func (tail *partitionTail) clear() {
	if tail == nil {
		return
	}
	tail.mu.Lock()
	defer tail.mu.Unlock()
	if tail.closed || tail.count == 0 {
		return
	}
	released := tail.bytes
	for index := range tail.slots {
		tail.slots[index].batch = nil
	}
	tail.head = 0
	tail.count = 0
	tail.bytes = 0
	if tail.budget != nil {
		tail.budget.release(released)
	}
}

func newDeliveredBatch(topic api.TopicID, partition uint32, records []api.Record, durableEnd uint64) (*deliveredBatch, bool) {
	if len(records) == 0 || uint64(len(records)) > uint64(MaxBatchRecords) {
		return nil, false
	}
	base := records[0].Offset
	if !validOffsetRange(base, uint32(len(records))) {
		return nil, false
	}
	end := base + uint64(len(records))
	if end > durableEnd {
		return nil, false
	}
	clone := make([]api.Record, len(records))
	var bytes uint64
	for index, record := range records {
		if record.Topic != topic || record.Partition != partition || record.Offset != base+uint64(index) {
			return nil, false
		}
		recordBytes, err := recordEncodedBytes(record)
		if err != nil {
			return nil, false
		}
		next, err := checkedAdd(bytes, recordBytes)
		if err != nil {
			return nil, false
		}
		bytes = next
		clone[index] = cloneRecord(record)
	}
	return &deliveredBatch{topic: topic, partition: partition, base: base, end: end, durableEnd: durableEnd, bytes: bytes, records: clone}, true
}

func (tail *partitionTail) fetch(topic api.TopicID, partition uint32, offset, durableEnd uint64, options api.FetchOptions) (api.FetchResult, bool, error) {
	if tail == nil || !tail.mu.TryLock() {
		return api.FetchResult{}, false, nil
	}
	defer tail.mu.Unlock()
	if tail.closed {
		return api.FetchResult{}, false, nil
	}

	for index := range tail.slots {
		batch := tail.slots[index].batch
		if !validDeliveredBatch(batch, topic, partition, offset, durableEnd) {
			continue
		}
		result := api.FetchResult{NextOffset: offset, Records: make([]api.Record, 0, minFetchCapacity(options.MaxRecords))}
		var resultBytes uint64
		for recordIndex := offset - batch.base; recordIndex < uint64(len(batch.records)); recordIndex++ {
			record := batch.records[recordIndex]
			recordBytes, err := recordEncodedBytes(record)
			if err != nil {
				return api.FetchResult{}, false, nil
			}
			if uint32(len(result.Records)) >= options.MaxRecords {
				return result, true, nil
			}
			if recordBytes > options.MaxBytes-resultBytes {
				if len(result.Records) == 0 {
					return api.FetchResult{NextOffset: offset}, true, errors.Join(api.ErrFetchLimitTooSmall, fmt.Errorf("record at offset %d needs %d bytes, limit is %d", record.Offset, recordBytes, options.MaxBytes))
				}
				return result, true, nil
			}
			result.Records = append(result.Records, cloneRecord(record))
			resultBytes += recordBytes
			result.NextOffset = record.Offset + 1
		}
		return result, true, nil
	}
	return api.FetchResult{}, false, nil
}

func validDeliveredBatch(batch *deliveredBatch, topic api.TopicID, partition uint32, offset, durableEnd uint64) bool {
	if batch == nil || batch.topic != topic || batch.partition != partition || batch.base > offset || offset >= batch.end || batch.end > batch.durableEnd || batch.durableEnd > durableEnd || len(batch.records) == 0 || uint64(len(batch.records)) > uint64(MaxBatchRecords) || !validOffsetRange(batch.base, uint32(len(batch.records))) || batch.end != batch.base+uint64(len(batch.records)) {
		return false
	}
	for index, record := range batch.records {
		if record.Topic != topic || record.Partition != partition || record.Offset != batch.base+uint64(index) {
			return false
		}
	}
	return true
}

func cloneRecord(record api.Record) api.Record {
	clone := record
	clone.Key = cloneBytes(record.Key)
	clone.Value = cloneBytes(record.Value)
	clone.Headers = make([]api.Header, len(record.Headers))
	for index, header := range record.Headers {
		clone.Headers[index] = api.Header{Name: header.Name, Value: cloneBytes(header.Value)}
	}
	return clone
}
