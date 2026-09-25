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
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/ayeshLK/immulog/api"
)

const (
	defaultInFlightRecords  = uint32(1024)
	defaultAdmissionWaiters = uint32(64)
	requestDescriptorBytes  = uint64(128)
)

type appendResult struct {
	record api.Record
	err    error
}

type appendRequest struct {
	record api.Record
	charge uint64
	result chan appendResult
	disk   *diskReservation

	queued    bool
	selected  bool
	completed bool
	resultVal appendResult
	abandoned atomic.Bool
	waited    atomic.Bool
}

func (options *PartitionOptions) normalizeWriter() {
	if options.BatchBytes == 0 {
		options.BatchBytes = MaxBatchBytes
	}
	if options.BatchRecords == 0 {
		options.BatchRecords = MaxBatchRecords
	}
	if options.RecordBytes == 0 {
		maximum := uint32(MaxRecordBytes)
		minimumBatchBytes := uint32(BatchHeaderBytes) + BatchTrailerBytes + RecordPrefixBytes
		if options.BatchBytes > minimumBatchBytes && options.BatchBytes-minimumBatchBytes < maximum {
			maximum = options.BatchBytes - minimumBatchBytes
		}
		options.RecordBytes = maximum
	}
	if options.InFlightBytes == 0 {
		options.InFlightBytes = uint64(options.BatchBytes) * 2
	}
	if options.InFlightRecords == 0 {
		options.InFlightRecords = defaultInFlightRecords
	}
	if options.AdmissionWaiters == 0 {
		options.AdmissionWaiters = defaultAdmissionWaiters
	}
}

func (options PartitionOptions) validateWriter() error {
	minimumBatchBytes := uint32(BatchHeaderBytes) + BatchTrailerBytes + RecordPrefixBytes
	if options.BatchBytes < minimumBatchBytes || options.BatchBytes > MaxBatchBytes {
		return errors.Join(api.ErrInvalidArgument, errors.New("batch byte limit is outside v1 limits"))
	}
	if options.BatchRecords == 0 || options.BatchRecords > MaxBatchRecords {
		return errors.Join(api.ErrInvalidArgument, errors.New("batch record limit is outside v1 limits"))
	}
	if options.RecordBytes < RecordPrefixBytes || options.RecordBytes > MaxRecordBytes {
		return errors.Join(api.ErrInvalidArgument, errors.New("record byte limit is outside v1 limits"))
	}
	if options.InFlightBytes == 0 || options.InFlightRecords == 0 || options.AdmissionWaiters == 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("writer admission limits must be positive"))
	}
	if options.InFlightRecords > maxInFlightRecords {
		return errors.Join(api.ErrResourceLimit, errors.New("in-flight record limit exceeds ingress capacity"))
	}
	if options.BatchLinger < 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("batch linger cannot be negative"))
	}
	return nil
}

func checkedAdd(left, right uint64) (uint64, error) {
	if right > math.MaxUint64-left {
		return 0, errors.Join(api.ErrResourceLimit, errors.New("writer size arithmetic overflow"))
	}
	return left + right, nil
}

func recordEncodedBytes(record api.Record) (uint64, error) {
	if !validHeaderCount(len(record.Headers)) {
		return 0, errors.Join(api.ErrInvalidArgument, errors.New("record header count exceeds v1 limit"))
	}
	size := uint64(RecordPrefixBytes) + 4 + uint64(len(record.Key)) + 4 + uint64(len(record.Value))
	for _, header := range record.Headers {
		if !validHeaderName(header.Name) {
			return 0, errors.Join(api.ErrInvalidArgument, errors.New("header name is not valid UTF-8 or is too large"))
		}
		var err error
		size, err = checkedAdd(size, 8+uint64(len(header.Name))+uint64(len(header.Value)))
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func batchEncodedBytes(records []api.Record) (uint64, error) {
	if len(records) == 0 || len(records) > int(MaxBatchRecords) {
		return 0, errors.Join(api.ErrInvalidArgument, errors.New("record count is outside v1 limits"))
	}
	size := uint64(BatchHeaderBytes) + BatchTrailerBytes
	for _, record := range records {
		recordSize, err := recordEncodedBytes(record)
		if err != nil {
			return 0, err
		}
		size, err = checkedAdd(size, recordSize)
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func (p *Partition) requestCharge(request api.AppendRequest) (uint64, error) {
	if request.Topic != p.topic || request.Partition != p.partition {
		return 0, errors.Join(api.ErrInvalidArgument, errors.New("append request does not match partition"))
	}
	record := api.Record{Topic: p.topic, Partition: p.partition, Key: request.Key, Value: request.Value, Headers: request.Headers}
	recordBytes, err := recordEncodedBytes(record)
	if err != nil {
		return 0, err
	}
	if recordBytes > uint64(p.options.RecordBytes) {
		return 0, errors.Join(api.ErrRecordTooLarge, errors.New("append request exceeds configured record limit"))
	}
	batchBytes, err := checkedAdd(uint64(BatchHeaderBytes)+BatchTrailerBytes, recordBytes)
	if err != nil {
		return 0, err
	}
	if batchBytes > uint64(p.options.BatchBytes) {
		return 0, errors.Join(api.ErrRecordTooLarge, errors.New("append request exceeds configured batch limit"))
	}
	charge, err := checkedAdd(requestDescriptorBytes, recordBytes)
	if err != nil {
		return 0, err
	}
	return charge, nil
}

func (p *Partition) copyRequest(request api.AppendRequest) api.Record {
	headers := make([]api.Header, len(request.Headers))
	for index, header := range request.Headers {
		headers[index] = api.Header{Name: header.Name, Value: cloneBytes(header.Value)}
	}
	return api.Record{
		Topic:     p.topic,
		Partition: p.partition,
		Timestamp: time.Now().UnixMilli(),
		Key:       cloneBytes(request.Key),
		Value:     cloneBytes(request.Value),
		Headers:   headers,
	}
}

func (p *Partition) signalQueueLocked() {
	close(p.queueWake)
	p.queueWake = make(chan struct{})
}

func (p *Partition) signalSpaceLocked() {
	close(p.spaceWake)
	p.spaceWake = make(chan struct{})
}

func (p *Partition) releaseAdmissionLocked(request *appendRequest) {
	if request.charge <= p.admittedBytes {
		p.admittedBytes -= request.charge
	} else {
		p.admittedBytes = 0
	}
	if p.admittedRecords > 0 {
		p.admittedRecords--
	}
	p.signalSpaceLocked()
}

func (p *Partition) releasePublicationLocked() {
	if p.publicationCredits > 0 {
		p.publicationCredits--
	}
}

func (p *Partition) releaseReservationLocked(request *appendRequest) {
	p.releasePublicationLocked()
	p.releaseAdmissionLocked(request)
}

func (p *Partition) signalPublisherLocked() {
	close(p.publisherWake)
	p.publisherWake = make(chan struct{})
}

func (p *Partition) finishPublisher() {
	p.queueMu.Lock()
	if p.publishing > 0 {
		p.publishing--
	}
	p.signalPublisherLocked()
	p.queueMu.Unlock()
}

func (p *Partition) waitForPublishersLocked() {
	for p.publishing != 0 {
		wake := p.publisherWake
		p.queueMu.Unlock()
		<-wake
		p.queueMu.Lock()
	}
}

func (p *Partition) admit(ctx context.Context, request *appendRequest) error {
	for {
		p.queueMu.Lock()
		if p.store != nil && p.store.closing.Load() {
			p.queueMu.Unlock()
			return api.ErrClosing
		}
		if p.queueClosed {
			p.queueMu.Unlock()
			return api.ErrClosed
		}
		if p.writerUnavailable {
			p.queueMu.Unlock()
			return api.ErrPartitionUnavailable
		}
		if p.queueClosing {
			p.queueMu.Unlock()
			return api.ErrClosing
		}
		if request.charge > p.options.InFlightBytes {
			p.queueMu.Unlock()
			return errors.Join(api.ErrResourceLimit, errors.New("append request exceeds writer admission capacity"))
		}
		if p.admittedRecords < p.options.InFlightRecords && p.admittedBytes <= p.options.InFlightBytes-request.charge && p.publicationCredits < p.ingress.capacity {
			p.admittedBytes += request.charge
			p.admittedRecords++
			p.publicationCredits++
			p.queueMu.Unlock()
			return nil
		}
		if p.waiting >= p.options.AdmissionWaiters {
			p.queueMu.Unlock()
			return api.ErrBackpressure
		}
		p.waiting++
		request.waited.Store(true)
		space := p.spaceWake
		closing := p.closingSignal
		p.queueMu.Unlock()

		select {
		case <-ctx.Done():
			p.queueMu.Lock()
			if p.waiting > 0 {
				p.waiting--
			}
			p.queueMu.Unlock()
			return ctx.Err()
		case <-space:
			p.queueMu.Lock()
			if p.waiting > 0 {
				p.waiting--
			}
			p.queueMu.Unlock()
		case <-closing:
			p.queueMu.Lock()
			if p.waiting > 0 {
				p.waiting--
			}
			closed := p.queueClosed
			p.queueMu.Unlock()
			if closed {
				return api.ErrClosed
			}
			return api.ErrClosing
		}
	}
}

func (p *Partition) enqueue(ctx context.Context, request *appendRequest) error {
	p.queueMu.Lock()
	if p.queueClosed {
		p.releaseReservationLocked(request)
		p.queueMu.Unlock()
		return api.ErrClosed
	}
	if p.writerUnavailable {
		p.releaseReservationLocked(request)
		p.queueMu.Unlock()
		return api.ErrPartitionUnavailable
	}
	if p.queueClosing || p.ingress.sealed.Load() {
		p.releaseReservationLocked(request)
		p.queueMu.Unlock()
		return api.ErrClosing
	}
	p.publishing++
	p.queueMu.Unlock()
	defer p.finishPublisher()

	if err := p.ingress.publish(ctx, request); err != nil {
		p.queueMu.Lock()
		p.releaseReservationLocked(request)
		p.queueMu.Unlock()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		p.markIngressUnavailable(err)
		return errors.Join(api.ErrPartitionUnavailable, err)
	}
	return nil
}

func (p *Partition) complete(request *appendRequest, result appendResult) {
	p.queueMu.Lock()
	if request.completed {
		p.queueMu.Unlock()
		return
	}
	request.completed = true
	request.selected = false
	request.resultVal = result
	p.recordAppendOutcome(result.err)
	p.releaseAdmissionLocked(request)
	disk := request.disk
	request.disk = nil
	p.queueMu.Unlock()
	disk.release()
	select {
	case request.result <- result:
	default:
	}
}

func (p *Partition) waitResult(ctx context.Context, request *appendRequest) (api.Record, error) {
	select {
	case result := <-request.result:
		return result.record, result.err
	case <-ctx.Done():
		p.queueMu.Lock()
		if request.completed {
			result := request.resultVal
			p.queueMu.Unlock()
			return result.record, result.err
		}
		request.abandoned.Store(true)
		p.queueMu.Unlock()
		return api.Record{}, errors.Join(api.ErrAppendOutcomeUnknown, ctx.Err())
	}
}

// Append reserves bounded payload and publication capacity before copying caller
// input. Once published to ingress, cancellation is an unknown outcome because
// the terminal writer may still durably append the request.
func (p *Partition) Append(ctx context.Context, request api.AppendRequest) (api.Record, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		p.recordAdmissionOutcome(err)
		return api.Record{}, err
	}
	if p.store != nil && p.store.closing.Load() {
		err := api.ErrClosing
		p.recordAdmissionOutcome(err)
		return api.Record{}, err
	}
	charge, err := p.requestCharge(request)
	if err != nil {
		return api.Record{}, err
	}
	recordBytes, err := recordEncodedBytes(api.Record{Topic: p.topic, Partition: p.partition, Key: request.Key, Value: request.Value, Headers: request.Headers})
	if err != nil {
		return api.Record{}, err
	}
	diskBytes, diskInodes, err := estimateDiskBatchGrowth(uint64(BatchHeaderBytes) + BatchTrailerBytes + recordBytes)
	if err != nil {
		return api.Record{}, err
	}
	disk, err := p.reserveDisk(diskBytes, diskInodes)
	if err != nil {
		p.recordAdmissionOutcome(err)
		return api.Record{}, err
	}
	prepared := &appendRequest{charge: charge, result: make(chan appendResult, 1), disk: disk}
	admissionStarted := time.Now()
	err = p.admit(ctx, prepared)
	admissionElapsed := time.Since(admissionStarted)
	if prepared.waited.Load() {
		saturatingIncrement(&p.admissionWaits)
		saturatingAdd(&p.admissionWaitNanos, uint64(admissionElapsed))
	}
	if err != nil {
		p.recordAdmissionOutcome(err)
		disk.release()
		return api.Record{}, err
	}
	if err := ctx.Err(); err != nil {
		p.queueMu.Lock()
		p.releaseReservationLocked(prepared)
		p.queueMu.Unlock()
		disk.release()
		p.recordAdmissionOutcome(err)
		return api.Record{}, err
	}
	prepared.record = p.copyRequest(request)
	if err := p.enqueue(ctx, prepared); err != nil {
		disk.release()
		p.recordAdmissionOutcome(err)
		return api.Record{}, err
	}
	return p.waitResult(ctx, prepared)
}
func (p *Partition) writeRequests(requests []*appendRequest) {
	p.mu.Lock()
	if p.unavailable {
		p.mu.Unlock()
		p.markIngressUnavailable(api.ErrPartitionUnavailable)
		for _, request := range requests {
			p.complete(request, appendResult{err: api.ErrPartitionUnavailable})
		}
		return
	}
	base := p.logEnd
	records := make([]api.Record, len(requests))
	for index, request := range requests {
		record := request.record
		record.Topic = p.topic
		record.Partition = p.partition
		record.Offset = base + uint64(index)
		records[index] = record
	}
	batch := api.RecordBatch{Topic: p.topic, Partition: p.partition, BaseOffset: base, Records: records}
	encoded, err := EncodeBatch(batch)
	if err == nil && uint64(len(encoded)) > uint64(p.options.BatchBytes) {
		err = errors.Join(api.ErrRecordTooLarge, errors.New("formed batch exceeds configured byte limit"))
	}
	var publication *tailPublication
	if err == nil {
		_, publication, err = p.appendEncodedLocked(batch, encoded)
	}
	p.mu.Unlock()
	p.publishTail(publication)
	if err != nil {
		if errors.Is(err, api.ErrPartitionUnavailable) {
			p.markIngressUnavailable(err)
		}
		unknown := errors.Is(err, api.ErrAppendOutcomeUnknown)
		for _, request := range requests {
			resultErr := err
			if !unknown && errors.Is(err, api.ErrPartitionUnavailable) {
				resultErr = api.ErrPartitionUnavailable
			}
			p.complete(request, appendResult{err: resultErr})
		}
		return
	}
	for index, request := range requests {
		p.complete(request, appendResult{record: records[index]})
	}
}

func (p *Partition) startWriter() error {
	p.queueMu.Lock()
	p.queueWake = make(chan struct{})
	p.spaceWake = make(chan struct{})
	p.closingSignal = make(chan struct{})
	p.closeDone = make(chan struct{})
	p.publisherWake = make(chan struct{})
	p.queueMu.Unlock()
	ingress, err := newPartitionIngress(p)
	if err != nil {
		return err
	}
	p.queueMu.Lock()
	p.ingress = ingress
	p.writerDone = ingress.done
	p.queueMu.Unlock()
	return nil
}

func (p *Partition) closeWriter() error {
	p.queueMu.Lock()
	if p.queueClosed {
		err := p.closeErr
		p.queueMu.Unlock()
		return err
	}
	if p.queueClosing {
		done := p.closeDone
		p.queueMu.Unlock()
		<-done
		p.queueMu.Lock()
		err := p.closeErr
		p.queueMu.Unlock()
		return err
	}
	p.queueClosing = true
	if p.ingress != nil {
		p.ingress.sealed.Store(true)
	}
	close(p.closingSignal)
	p.signalQueueLocked()
	p.signalSpaceLocked()
	p.waitForPublishersLocked()
	ingress := p.ingress
	p.queueMu.Unlock()

	p.mu.Lock()
	p.closing = true
	p.signalFetchWaitersLocked()
	p.mu.Unlock()
	p.tail.close()

	var closeErr error
	if ingress != nil {
		ingress.close()
		<-ingress.done
		ingress.drainPending(p, api.ErrPartitionUnavailable)
	}
	p.mu.Lock()
	if !p.closed {
		for _, segment := range p.segments {
			// Sealed segments were checkpointed when they rolled; rewriting
			// every sidecar here costs four fsyncs per segment and made close
			// scale with the whole log.
			if !segment.indexDirty {
				continue
			}
			_ = installSegmentIndexes(segment, p.storeID, p.options.IndexStride)
		}
		p.closed = true
		p.signalFetchWaitersLocked()
		for _, segment := range p.segments {
			if segment.file != nil {
				closeErr = errors.Join(closeErr, segment.file.Close())
				segment.file = nil
			}
			p.forgetSegmentFile(segment.path)
		}
	}
	p.mu.Unlock()

	p.queueMu.Lock()
	p.closeErr = errors.Join(p.closeErr, closeErr)
	closeErr = p.closeErr
	p.queueClosed = true
	close(p.closeDone)
	p.queueMu.Unlock()
	return closeErr
}

func (p *Partition) writerDiagnostics() string {
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	return fmt.Sprintf("queued=%d admitted_records=%d admitted_bytes=%d publication_credits=%d publishers=%d waiting=%d", len(p.queue), p.admittedRecords, p.admittedBytes, p.publicationCredits, p.publishing, p.waiting)
}
