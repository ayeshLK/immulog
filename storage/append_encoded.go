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
	"io"
	"time"

	"github.com/ayeshLK/immulog/api"
)

func validHeaderCount(count int) bool {
	return count >= 0 && uint64(count) <= uint64(MaxRecordHeaders)
}

type tailPublication struct {
	topic      api.TopicID
	partition  uint32
	records    []api.Record
	durableEnd uint64
}

func (p *Partition) publishTail(publication *tailPublication) {
	if publication != nil {
		p.tail.offer(publication.topic, publication.partition, publication.records, publication.durableEnd)
	}
}

// appendEncodedLocked appends one already encoded batch. The caller owns p.mu
// and has already established whether this is a direct or admitted writer
// operation. A sync failure makes the partition unavailable and the outcome
// unknown; no later offset is allocated in this instance.
func (p *Partition) appendEncodedLocked(batch api.RecordBatch, encoded []byte) (uint64, *tailPublication, error) {
	if p.closed {
		return 0, nil, api.ErrClosed
	}
	if p.unavailable {
		return 0, nil, api.ErrPartitionUnavailable
	}
	if batch.Topic != p.topic || batch.Partition != p.partition || batch.BaseOffset != p.logEnd {
		return 0, nil, errors.Join(api.ErrInvalidArgument, errors.New("batch does not match partition next offset"))
	}
	if len(batch.Records) == 0 || uint32(len(batch.Records)) > p.options.BatchRecords || uint64(len(encoded)) > uint64(p.options.BatchBytes) {
		return 0, nil, errors.Join(api.ErrRecordTooLarge, errors.New("batch exceeds configured writer limits"))
	}
	for _, record := range batch.Records {
		recordBytes, err := recordEncodedBytes(record)
		if err != nil {
			return 0, nil, err
		}
		if recordBytes > uint64(p.options.RecordBytes) {
			return 0, nil, errors.Join(api.ErrRecordTooLarge, errors.New("record exceeds configured writer limit"))
		}
	}
	active := p.segments[len(p.segments)-1]
	if uint64(len(encoded))+uint64(active.size) > p.options.SegmentBytes {
		if active.records == 0 {
			return 0, nil, errors.Join(api.ErrRecordTooLarge, errors.New("batch cannot fit in configured segment"))
		}
		namespaceStarted := time.Now()
		rolled, err := createSegment(p.dir, p.topic, p.partition, p.logEnd)
		recordLatency(&p.namespaceOps, &p.namespaceNanos, &p.namespaceBuckets, time.Since(namespaceStarted))
		if err != nil {
			p.unavailable = true
			return 0, nil, errors.Join(api.ErrPartitionUnavailable, err)
		}
		// The prior segment is immutable after the new segment is published.
		// Its derived indexes are safe to checkpoint without affecting the
		// authoritative append below.
		_ = installSegmentIndexes(active, p.storeID, p.options.IndexStride)
		p.segments = append(p.segments, rolled)
		active = rolled
	}
	position := active.size
	writeStarted := time.Now()
	count, writeErr := fileWriteAt(active.file, encoded, position)
	recordLatency(&p.writeOps, &p.writeNanos, &p.writeBuckets, time.Since(writeStarted))
	if writeErr != nil || count != len(encoded) {
		p.unavailable = true
		return 0, nil, errors.Join(api.ErrAppendOutcomeUnknown, api.ErrPartitionUnavailable, writeErr, io.ErrShortWrite)
	}
	syncStarted := time.Now()
	syncErr := fileSync(active.file)
	recordLatency(&p.syncOps, &p.syncNanos, &p.syncBuckets, time.Since(syncStarted))
	if syncErr != nil {
		p.unavailable = true
		return 0, nil, errors.Join(api.ErrAppendOutcomeUnknown, api.ErrPartitionUnavailable, syncErr)
	}
	active.size += int64(len(encoded))
	active.end += uint64(len(batch.Records))
	active.records += uint64(len(batch.Records))
	active.batches = append(active.batches, batchInfo{
		base: batch.BaseOffset, position: position, bytes: uint32(len(encoded)),
		records: uint32(len(batch.Records)), maxTime: int64(binaryLittleEndianUint64(encoded[24:32])),
	})
	for _, record := range batch.Records {
		if record.Timestamp > active.maxTime {
			active.maxTime = record.Timestamp
		}
	}
	p.logEnd = active.end
	refreshSegmentIndexes(active, p.storeID, p.options.IndexStride)
	p.signalFetchWaitersLocked()
	if p.tail == nil {
		return batch.BaseOffset, nil, nil
	}
	return batch.BaseOffset, &tailPublication{topic: p.topic, partition: p.partition, records: batch.Records, durableEnd: p.logEnd}, nil
}
