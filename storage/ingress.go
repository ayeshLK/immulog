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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	disruptor "github.com/smarty/go-disruptor"
)

const (
	maxInFlightRecords = uint32(1 << 16)
	maxIngressSlots    = uint32(1 << 17)
)

// partitionIngress is the only adapter over the pinned disruptor dependency.
// A producer reserves admission before copying input, then uses TryReserve so
// no operation can block after it has claimed a ring sequence.
type partitionIngress struct {
	disruptor disruptor.Disruptor
	slots     []*appendRequest
	mask      int64
	capacity  uint32
	wait      *ingressWait
	sealed    atomic.Bool
	done      chan struct{}
}

type ingressHandler struct {
	partition *Partition
	ingress   *partitionIngress
}

func (handler ingressHandler) Handle(lowerSequence, upperSequence int64) {
	handler.partition.consumeIngress(handler.ingress, lowerSequence, upperSequence)
}

func newPartitionIngress(partition *Partition) (*partitionIngress, error) {
	capacity, err := ingressCapacity(partition.options.InFlightRecords)
	if err != nil {
		return nil, err
	}
	wait := newIngressWait()
	ingress := &partitionIngress{
		slots:    make([]*appendRequest, capacity),
		mask:     int64(capacity - 1),
		capacity: capacity,
		wait:     wait,
		done:     make(chan struct{}),
	}
	runtime, err := disruptor.New(
		disruptor.Options.BufferCapacity(capacity),
		disruptor.Options.WriterCount(2),
		disruptor.Options.WaitStrategy(wait),
		disruptor.Options.NewHandlerGroup(ingressHandler{partition: partition, ingress: ingress}),
	)
	if err != nil {
		return nil, fmt.Errorf("create partition ingress: %w", err)
	}
	ingress.disruptor = runtime
	go func() {
		defer close(ingress.done)
		runtime.Listen()
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

func (ingress *partitionIngress) publish(request *appendRequest) error {
	var sequence int64
	for {
		sequence = ingress.disruptor.TryReserve(1)
		if sequence >= 0 {
			break
		}
		runtime.Gosched()
	}
	ingress.slots[sequence&ingress.mask] = request
	ingress.disruptor.Commit(sequence, sequence)
	ingress.wait.notify()
	return nil
}

func (ingress *partitionIngress) take(sequence int64) *appendRequest {
	index := sequence & ingress.mask
	request := ingress.slots[index]
	ingress.slots[index] = nil
	return request
}

func (ingress *partitionIngress) close() error {
	ingress.sealed.Store(true)
	ingress.wait.close()
	return ingress.disruptor.Close()
}

// ingressWait keeps the terminal processor and any unexpected slow-path
// reservation asleep until a producer publishes or shutdown begins.
type ingressWait struct {
	mu     sync.Mutex
	wake   chan struct{}
	closed bool
}

func newIngressWait() *ingressWait {
	return &ingressWait{wake: make(chan struct{}, 1)}
}

func (wait *ingressWait) Gate(int64)    { wait.await() }
func (wait *ingressWait) Idle(int64)    { wait.await() }
func (wait *ingressWait) Reserve(int64) { wait.await() }

func (wait *ingressWait) await() {
	<-wait.wake
}

func (wait *ingressWait) notify() {
	wait.mu.Lock()
	defer wait.mu.Unlock()
	if wait.closed {
		return
	}
	select {
	case wait.wake <- struct{}{}:
	default:
	}
}

func (wait *ingressWait) close() {
	wait.mu.Lock()
	defer wait.mu.Unlock()
	if wait.closed {
		return
	}
	wait.closed = true
	close(wait.wake)
}

func (partition *Partition) consumeIngress(ingress *partitionIngress, lowerSequence, upperSequence int64) {
	defer func() {
		if recovered := recover(); recovered != nil {
			partition.markIngressUnavailable(fmt.Errorf("ingress handler panic: %v", recovered))
		}
	}()
	requests := make([]*appendRequest, 0, upperSequence-lowerSequence+1)
	for sequence := lowerSequence; sequence <= upperSequence; sequence++ {
		request := ingress.take(sequence)
		if request == nil {
			partition.markIngressUnavailable(fmt.Errorf("ingress sequence %d had no request", sequence))
			continue
		}
		partition.queueMu.Lock()
		partition.releasePublicationLocked()
		partition.queueMu.Unlock()
		requests = append(requests, request)
	}
	if len(requests) == 0 {
		return
	}
	if partition.options.BatchLinger > 0 {
		time.Sleep(partition.options.BatchLinger)
	}
	partition.writeIngressBatches(requests)
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
