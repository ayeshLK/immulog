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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ayeshLK/immulog/api"
)

const (
	defaultFetchRecords = uint32(1024)
	defaultFetchBytes   = uint64(4 << 20)
	maxFetchBytes       = uint64(MaxBatchBytes)
	maxFetchWaiters     = uint32(64)
)

func normalizeFetchOptions(options api.FetchOptions) (api.FetchOptions, error) {
	if options.MaxRecords == 0 {
		options.MaxRecords = defaultFetchRecords
	}
	if options.MaxRecords > MaxBatchRecords {
		return api.FetchOptions{}, errors.Join(api.ErrInvalidArgument, fmt.Errorf("fetch record limit %d exceeds %d", options.MaxRecords, MaxBatchRecords))
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultFetchBytes
	}
	if options.MaxBytes > maxFetchBytes {
		return api.FetchOptions{}, errors.Join(api.ErrInvalidArgument, fmt.Errorf("fetch byte limit %d exceeds %d", options.MaxBytes, maxFetchBytes))
	}
	if options.MaxWait < 0 || options.MaxWait > 24*time.Hour {
		return api.FetchOptions{}, errors.Join(api.ErrInvalidArgument, fmt.Errorf("fetch wait %s is outside [0s,24h]", options.MaxWait))
	}
	return options, nil
}

// Fetch returns a caller-owned, bounded prefix of durable records at offset.
// It validates and decodes one bounded storage batch at a time; result bytes are
// measured as v1 encoded record bytes. If the first record cannot fit, Fetch
// returns ErrFetchLimitTooSmall and no records.
func (p *Partition) Fetch(ctx context.Context, offset uint64, options api.FetchOptions) (result api.FetchResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return api.FetchResult{NextOffset: offset}, err
	}
	options, err = normalizeFetchOptions(options)
	if err != nil {
		return api.FetchResult{NextOffset: offset}, err
	}
	p.mu.RLock()
	defer func() {
		p.mu.RUnlock()
		if shouldFenceFetchError(err) {
			p.markIngressUnavailable(err)
		}
	}()
	if p.closed {
		return api.FetchResult{NextOffset: offset}, api.ErrClosed
	}
	if p.closing {
		return api.FetchResult{NextOffset: offset}, api.ErrClosing
	}
	if p.unavailable {
		return api.FetchResult{NextOffset: offset}, api.ErrPartitionUnavailable
	}
	if len(p.segments) == 0 || offset < p.segments[0].header.BaseOffset || offset > p.logEnd {
		return api.FetchResult{NextOffset: offset}, api.ErrOffsetOutOfRange
	}
	if result, hit, tailErr := p.tail.fetch(p.topic, p.partition, offset, p.logEnd, options); hit {
		return result, tailErr
	}
	result = api.FetchResult{NextOffset: offset, Records: make([]api.Record, 0, minFetchCapacity(options.MaxRecords))}
	var resultBytes uint64
	for _, segment := range p.segments {
		if offset >= segment.end {
			continue
		}
		position := int64(SegmentHeaderBytes)
		for _, hint := range segment.offsetIndex {
			if hint.base > offset {
				break
			}
			if hint.position > uint64(segment.size) {
				return api.FetchResult{NextOffset: offset}, corrupt(errInvalidBatch, "offset index position exceeds segment size")
			}
			position = int64(hint.position)
		}
		for position < segment.size {
			if err := ctx.Err(); err != nil {
				return api.FetchResult{NextOffset: offset}, err
			}
			var header [BatchHeaderBytes]byte
			if err := readAtFull(segment.file, header[:], position); err != nil {
				return api.FetchResult{NextOffset: offset}, fmt.Errorf("read batch header from %q: %w", segment.path, err)
			}
			length := binary.LittleEndian.Uint32(header[8:12])
			if length < uint32(BatchHeaderBytes)+BatchTrailerBytes || length > MaxBatchBytes || int64(length) > segment.size-position {
				return api.FetchResult{NextOffset: offset}, corrupt(errInvalidBatch, "batch length exceeds segment during fetch")
			}
			data := make([]byte, int(length))
			copy(data, header[:])
			if err := readAtFull(segment.file, data[BatchHeaderBytes:], position+int64(BatchHeaderBytes)); err != nil {
				return api.FetchResult{NextOffset: offset}, fmt.Errorf("read batch body from %q: %w", segment.path, err)
			}
			batch, err := DecodeBatch(data, p.topic, p.partition)
			if err != nil {
				return api.FetchResult{NextOffset: offset}, err
			}
			position += int64(length)
			for _, record := range batch.Records {
				if record.Offset < offset {
					continue
				}
				if uint32(len(result.Records)) >= options.MaxRecords {
					return result, nil
				}
				recordBytes, err := recordEncodedBytes(record)
				if err != nil {
					return api.FetchResult{NextOffset: offset}, err
				}
				if recordBytes > options.MaxBytes-resultBytes {
					if len(result.Records) == 0 {
						return api.FetchResult{NextOffset: offset}, errors.Join(api.ErrFetchLimitTooSmall, fmt.Errorf("record at offset %d needs %d bytes, limit is %d", record.Offset, recordBytes, options.MaxBytes))
					}
					return result, nil
				}
				result.Records = append(result.Records, record)
				resultBytes += recordBytes
				result.NextOffset = record.Offset + 1
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return api.FetchResult{NextOffset: offset}, err
	}
	return result, nil
}

func minFetchCapacity(limit uint32) int {
	if limit > 128 {
		return 128
	}
	return int(limit)
}

func (p *Partition) waitForData(ctx context.Context, offset uint64, maxWait time.Duration) error {
	if maxWait == 0 {
		return nil
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return api.ErrClosed
	}
	if p.closing {
		p.mu.Unlock()
		return api.ErrClosing
	}
	if p.unavailable {
		p.mu.Unlock()
		return api.ErrPartitionUnavailable
	}
	if len(p.segments) == 0 || offset < p.segments[0].header.BaseOffset || offset > p.logEnd {
		p.mu.Unlock()
		return api.ErrOffsetOutOfRange
	}
	if offset < p.logEnd {
		p.mu.Unlock()
		return nil
	}
	if p.fetchWaiters >= maxFetchWaiters {
		p.mu.Unlock()
		return api.ErrBackpressure
	}
	if p.fetchWake == nil {
		p.fetchWake = make(chan struct{})
	}
	wake := p.fetchWake
	p.fetchWaiters++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.fetchWaiters--
		p.mu.Unlock()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case <-wake:
		return nil
	}
}

func (p *Partition) signalFetchWaitersLocked() {
	if p.fetchWake != nil {
		close(p.fetchWake)
		p.fetchWake = nil
	}
}

func readAtFull(file *os.File, data []byte, position int64) error {
	if len(data) == 0 {
		return nil
	}
	count, err := fileReadAt(file, data, position)
	if count != len(data) {
		return errors.Join(err, io.ErrUnexpectedEOF)
	}
	return err
}

func shouldFenceFetchError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return !errors.Is(err, api.ErrInvalidArgument) && !errors.Is(err, api.ErrOffsetOutOfRange) && !errors.Is(err, api.ErrFetchLimitTooSmall) && !errors.Is(err, api.ErrClosed) && !errors.Is(err, api.ErrClosing) && !errors.Is(err, api.ErrPartitionUnavailable)
}

// Reader owns a stateful raw durable-read cursor. It has no consumer-group
// assignment and cannot commit shared consumer progress.
type Reader struct {
	partition *Partition
	operation sync.Mutex
	next      uint64
	closed    bool
}

// NewReader creates a raw reader positioned at start. The position is the next
// record offset to fetch and changes only after a successful Reader.Fetch or Seek.
func (p *Partition) NewReader(start uint64) (*Reader, error) {
	if err := p.validateReadOffset(start); err != nil {
		return nil, err
	}
	return &Reader{partition: p, next: start}, nil
}

func (p *Partition) validateReadOffset(offset uint64) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return api.ErrClosed
	}
	if p.closing {
		return api.ErrClosing
	}
	if p.unavailable {
		return api.ErrPartitionUnavailable
	}
	if len(p.segments) == 0 || offset < p.segments[0].header.BaseOffset || offset > p.logEnd {
		return api.ErrOffsetOutOfRange
	}
	return nil
}

// Fetch reads from the reader's next offset and advances that cursor only when
// its bounded result is successfully handed to the caller.
func (reader *Reader) Fetch(ctx context.Context, options api.FetchOptions) (api.FetchResult, error) {
	if !reader.operation.TryLock() {
		return api.FetchResult{}, api.ErrConcurrentOperation
	}
	defer reader.operation.Unlock()
	if reader.closed {
		return api.FetchResult{}, api.ErrClosed
	}
	result, err := reader.partition.Fetch(ctx, reader.next, options)
	if err != nil {
		return result, err
	}
	reader.next = result.NextOffset
	return result, nil
}

// Seek changes the next raw-reader offset after validating its retained range.
func (reader *Reader) Seek(offset uint64) error {
	if !reader.operation.TryLock() {
		return api.ErrConcurrentOperation
	}
	defer reader.operation.Unlock()
	if reader.closed {
		return api.ErrClosed
	}
	if err := reader.partition.validateReadOffset(offset); err != nil {
		return err
	}
	reader.next = offset
	return nil
}

// NextOffset returns the reader's current next offset.
func (reader *Reader) NextOffset() (uint64, error) {
	reader.operation.Lock()
	defer reader.operation.Unlock()
	if reader.closed {
		return 0, api.ErrClosed
	}
	return reader.next, nil
}

// Close releases the reader handle. It is safe to call repeatedly.
func (reader *Reader) Close() error {
	reader.operation.Lock()
	defer reader.operation.Unlock()
	reader.closed = true
	return nil
}
