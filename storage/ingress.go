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
	"sync/atomic"
	"time"

	"github.com/ayeshLK/immulog/api"
	disruptor "github.com/ayeshLK/lib-disruptor"
)

const (
	maxInFlightRecords = uint32(1 << 16)
	maxIngressSlots    = uint32(1 << 17)
)

// ingressEvent is a reusable ring slot. The request is cleared before the
// event is handed back to the ring so a slot never retains caller-owned data.
type ingressEvent struct {
	request *appendRequest
}

// partitionIngress is the only adapter over the pinned disruptor dependency.
// A producer reserves admission before copying input, then publishes a request
// through the context-aware ring claim path.
type partitionIngress struct {
	ring      *disruptor.RingBuffer[*ingressEvent]
	processor *disruptor.BatchProcessor[*ingressEvent]
	capacity  uint32
	sealed    atomic.Bool
	done      chan struct{}
}

type ingressHandler struct {
	partition *Partition
	requests  []*appendRequest
}

func (handler *ingressHandler) Handle(event *ingressEvent, _ int64, endOfBatch bool) error {
	request := event.request
	event.request = nil
	if request == nil {
		return errors.New("ingress event had no request")
	}

	handler.partition.queueMu.Lock()
	handler.partition.releasePublicationLocked()
	handler.partition.queueMu.Unlock()
	handler.requests = append(handler.requests, request)
	if !endOfBatch {
		return nil
	}
	if handler.partition.options.BatchLinger > 0 {
		time.Sleep(handler.partition.options.BatchLinger)
	}
	requests := handler.requests
	handler.partition.writeIngressBatches(requests)
	for index := range requests {
		requests[index] = nil
	}
	handler.requests = nil
	return nil
}

func (handler *ingressHandler) failPending(cause error) {
	requests := handler.requests
	handler.requests = nil
	if len(requests) == 0 {
		return
	}
	resultErr := errors.Join(api.ErrAppendOutcomeUnknown, api.ErrPartitionUnavailable, cause)
	for _, request := range requests {
		handler.partition.complete(request, appendResult{err: resultErr})
	}
}

func newPartitionIngress(partition *Partition) (*partitionIngress, error) {
	capacity, err := ingressCapacity(partition.options.InFlightRecords)
	if err != nil {
		return nil, err
	}
	ring, err := disruptor.New(
		int64(capacity),
		disruptor.MultiProducer,
		func() *ingressEvent { return new(ingressEvent) },
		disruptor.BlockingWait(),
		disruptor.WithProducerWait(disruptor.ProducerWaitBlocking),
	)
	if err != nil {
		return nil, fmt.Errorf("create partition ingress ring: %w", err)
	}
	handler := &ingressHandler{partition: partition}
	processor, err := disruptor.NewBatchProcessor(
		ring,
		ring.NewBarrier(),
		handler.Handle,
		disruptor.WithMaxBatchSize(int64(capacity)),
	)
	if err != nil {
		ring.Close()
		return nil, fmt.Errorf("create partition ingress processor: %w", err)
	}
	ring.AddGatingSequences(processor.Sequence())
	ingress := &partitionIngress{
		ring:      ring,
		processor: processor,
		capacity:  capacity,
		done:      make(chan struct{}),
	}
	go func() {
		defer close(ingress.done)
		if err := processor.Run(context.Background()); err != nil && !errors.Is(err, disruptor.ErrClosed) {
			handler.failPending(err)
			partition.markIngressUnavailable(fmt.Errorf("ingress processor failed: %w", err))
			ingress.close()
			partition.queueMu.Lock()
			partition.waitForPublishersLocked()
			partition.queueMu.Unlock()
			ingress.drainPending(partition, err)
		}
	}()
	return ingress, nil
}

func ingressCapacity(limit uint32) (uint32, error) {
	if limit == 0 || limit > maxInFlightRecords {
		return 0, fmt.Errorf("in-flight record limit %d is outside the supported range", limit)
	}
	needed := limit * 2 // active handler range plus the next admitted window
	if needed > maxIngressSlots {
		return 0, fmt.Errorf("ingress slot requirement %d exceeds %d", needed, maxIngressSlots)
	}
	capacity := uint32(1)
	for capacity < needed {
		capacity <<= 1
	}
	return capacity, nil
}

func (ingress *partitionIngress) publish(ctx context.Context, request *appendRequest) error {
	sequence, err := ingress.ring.Next(ctx)
	if err != nil {
		return err
	}
	defer ingress.ring.PublishSequence(sequence)
	ingress.ring.Get(sequence).request = request
	return nil
}

func (ingress *partitionIngress) close() {
	ingress.sealed.Store(true)
	ingress.ring.Close()
}

func (ingress *partitionIngress) drainPending(partition *Partition, cause error) {
	lower := ingress.processor.Sequence().Load() + 1
	upper := ingress.ring.Cursor()
	if lower > upper {
		return
	}
	resultErr := errors.Join(api.ErrAppendOutcomeUnknown, api.ErrPartitionUnavailable, cause)
	for sequence := lower; sequence <= upper; sequence++ {
		event := ingress.ring.Get(sequence)
		request := event.request
		event.request = nil
		if request == nil {
			continue
		}
		partition.queueMu.Lock()
		partition.releasePublicationLocked()
		partition.queueMu.Unlock()
		partition.complete(request, appendResult{err: resultErr})
	}
}

func (partition *Partition) writeIngressBatches(requests []*appendRequest) {
	batch := make([]*appendRequest, 0, partition.options.BatchRecords)
	batchBytes := uint64(BatchHeaderBytes) + BatchTrailerBytes
	for _, request := range requests {
		recordBytes, err := recordEncodedBytes(request.record)
		if err != nil {
			partition.complete(request, appendResult{err: err})
			continue
		}
		if len(batch) != 0 && (uint32(len(batch)) == partition.options.BatchRecords || batchBytes+recordBytes > uint64(partition.options.BatchBytes)) {
			partition.writeRequests(batch)
			batch = make([]*appendRequest, 0, partition.options.BatchRecords)
			batchBytes = uint64(BatchHeaderBytes) + BatchTrailerBytes
		}
		batch = append(batch, request)
		batchBytes += recordBytes
	}
	if len(batch) != 0 {
		partition.writeRequests(batch)
	}
}

func (partition *Partition) markIngressUnavailable(cause error) {
	partition.mu.Lock()
	partition.unavailable = true
	partition.signalFetchWaitersLocked()
	partition.mu.Unlock()
	partition.queueMu.Lock()
	partition.writerUnavailable = true
	partition.closeErr = fmt.Errorf("partition ingress failed: %w", cause)
	partition.signalQueueLocked()
	partition.signalSpaceLocked()
	partition.queueMu.Unlock()
}
