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
	"sync"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
	disruptor "github.com/ayeshLK/lib-disruptor"
)

func waitForIngressState(t *testing.T, partition *Partition, ready func(admitted, credits, publishers, waiting uint32) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		partition.queueMu.Lock()
		admitted := partition.admittedRecords
		credits := partition.publicationCredits
		publishers := partition.publishing
		waiting := partition.waiting
		partition.queueMu.Unlock()
		if ready(admitted, credits, publishers, waiting) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for ingress state")
}

func TestBatchLingerRejectsNegativeDuration(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.OpenPartition(testTopic(), 0, PartitionOptions{BatchLinger: -time.Nanosecond}); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("negative batch linger error = %v, want ErrInvalidArgument", err)
	}
}

func TestIngressEnqueueCancellationWhileRingIsFull(t *testing.T) {
	ring, err := disruptor.New(
		int64(1),
		disruptor.MultiProducer,
		func() *ingressEvent { return new(ingressEvent) },
		disruptor.BlockingWait(),
		disruptor.WithProducerWait(disruptor.ProducerWaitBlocking),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()
	gate := disruptor.NewSequence(disruptor.InitialSequence)
	ring.AddGatingSequences(gate)
	ingress := &partitionIngress{ring: ring, capacity: 1}
	sequence, err := ring.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ring.Get(sequence).request = &appendRequest{}
	ring.PublishSequence(sequence)
	if remaining := ring.RemainingCapacity(); remaining != 0 {
		t.Fatalf("remaining ring capacity = %d, want 0", remaining)
	}
	partition := &Partition{
		ingress:            ingress,
		admittedRecords:    1,
		publicationCredits: 1,
		publisherWake:      make(chan struct{}),
		spaceWake:          make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- partition.enqueue(ctx, &appendRequest{}) }()
	waitForIngressState(t, partition, func(_, _, publishers, _ uint32) bool { return publishers == 1 })
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled ingress enqueue = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled ingress enqueue did not return")
	}
	partition.queueMu.Lock()
	defer partition.queueMu.Unlock()
	if partition.writerUnavailable {
		t.Fatal("context cancellation fenced the partition")
	}
	if partition.admittedRecords != 0 || partition.publicationCredits != 0 || partition.publishing != 0 {
		t.Fatalf("canceled enqueue retained state: admitted=%d publication=%d publishers=%d", partition.admittedRecords, partition.publicationCredits, partition.publishing)
	}
}

func TestIngressHandlerFailureCompletesPendingRequest(t *testing.T) {
	partition := &Partition{spaceWake: make(chan struct{})}
	handler := &ingressHandler{partition: partition}
	request := &appendRequest{result: make(chan appendResult, 1)}
	if err := handler.Handle(&ingressEvent{request: request}, 0, false); err != nil {
		t.Fatal(err)
	}
	handler.failPending(errors.New("processor stopped"))
	result := <-request.result
	if !errors.Is(result.err, api.ErrAppendOutcomeUnknown) {
		t.Fatalf("failed pending request = %v, want unknown outcome", result.err)
	}
	if !errors.Is(result.err, api.ErrPartitionUnavailable) {
		t.Fatalf("failed pending request = %v, want partition unavailable", result.err)
	}
}

func TestIngressDrainsPendingRingRequests(t *testing.T) {
	ring, err := disruptor.New(
		int64(4),
		disruptor.MultiProducer,
		func() *ingressEvent { return new(ingressEvent) },
		disruptor.BlockingWait(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()
	processor, err := disruptor.NewBatchProcessor(
		ring,
		ring.NewBarrier(),
		func(*ingressEvent, int64, bool) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	ingress := &partitionIngress{ring: ring, processor: processor, capacity: 4}
	partition := &Partition{
		admittedRecords:    2,
		publicationCredits: 2,
		spaceWake:          make(chan struct{}),
	}
	requests := []*appendRequest{
		{result: make(chan appendResult, 1)},
		{result: make(chan appendResult, 1)},
	}
	for index, request := range requests {
		sequence, err := ring.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ring.Get(sequence).request = request
		ring.PublishSequence(sequence)
		if sequence != int64(index) {
			t.Fatalf("published sequence = %d, want %d", sequence, index)
		}
	}

	ingress.drainPending(partition, errors.New("processor stopped"))
	for index, request := range requests {
		result := <-request.result
		if !errors.Is(result.err, api.ErrAppendOutcomeUnknown) || !errors.Is(result.err, api.ErrPartitionUnavailable) {
			t.Fatalf("drained request %d = %v, want unknown partition-unavailable outcome", index, result.err)
		}
		if event := ring.Get(int64(index)); event.request != nil {
			t.Fatalf("ring slot %d retains request after drain", index)
		}
	}
	partition.queueMu.Lock()
	defer partition.queueMu.Unlock()
	if partition.admittedRecords != 0 || partition.publicationCredits != 0 {
		t.Fatalf("drained state retained: admitted=%d publication=%d", partition.admittedRecords, partition.publicationCredits)
	}
}

func TestIngressSaturationBoundsWaiters(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{
		BatchLinger:      250 * time.Millisecond,
		InFlightBytes:    1024,
		InFlightRecords:  1,
		AdmissionWaiters: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	appendAsync := func(value string) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte(value)})
			result <- err
		}()
		return result
	}
	first := appendAsync("first")
	waitForIngressState(t, partition, func(admitted, _, _, _ uint32) bool { return admitted == 1 })
	second := appendAsync("second")
	waitForIngressState(t, partition, func(_, _, _, waiting uint32) bool { return waiting == 1 })
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("rejected")}); !errors.Is(err, api.ErrBackpressure) {
		t.Fatalf("append beyond waiter capacity = %v, want ErrBackpressure", err)
	}
	if err := <-first; err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second append: %v", err)
	}
}

func TestIngressBatchTimeoutCollectsPublishedRequests(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{
		BatchLinger:     500 * time.Millisecond,
		BatchRecords:    8,
		InFlightBytes:   4096,
		InFlightRecords: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	appendAsync := func(value string) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte(value)})
			result <- err
		}()
		return result
	}

	first := appendAsync("first")
	waitForIngressState(t, partition, func(admitted, credits, publishers, _ uint32) bool {
		return admitted == 1 && credits == 1 && publishers == 0
	})
	second := appendAsync("second")
	if err := <-first; err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second append: %v", err)
	}
	if end, err := partition.EndOffset(); err != nil || end != 2 {
		t.Fatalf("end offset = (%d, %v), want (2, nil)", end, err)
	}
	if operations := partition.Stats().SyncLatency.Operations; operations != 1 {
		t.Fatalf("sync operations = %d, want one timed ingress batch", operations)
	}
}

func TestIngressClearsSlotsAndTransfersCredits(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{
		BatchRecords:    2,
		InFlightBytes:   4096,
		InFlightRecords: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if partition.ingress.capacity != 8 {
		t.Fatalf("ingress capacity = %d, want active-plus-next power of two 8", partition.ingress.capacity)
	}
	const appends = 32
	var group sync.WaitGroup
	errorsCh := make(chan error, appends)
	for index := 0; index < appends; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			_, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte{byte(index)}})
			errorsCh <- err
		}(index)
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	partition.queueMu.Lock()
	admitted, credits, publishers := partition.admittedRecords, partition.publicationCredits, partition.publishing
	partition.queueMu.Unlock()
	if admitted != 0 || credits != 0 || publishers != 0 {
		t.Fatalf("ingress credits after drain = admitted:%d publication:%d publishers:%d", admitted, credits, publishers)
	}
	for index := uint32(0); index < partition.ingress.capacity; index++ {
		if request := partition.ingress.ring.Get(int64(index)).request; request != nil {
			t.Fatalf("ingress slot %d retains request after drain", index)
		}
	}
	if end, err := partition.EndOffset(); err != nil || end != appends {
		t.Fatalf("end offset = (%d, %v), want (%d, nil)", end, err, appends)
	}
}

func TestIngressCancellationAfterPublicationIsUnknown(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{
		BatchLinger:     250 * time.Millisecond,
		InFlightBytes:   1024,
		InFlightRecords: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := partition.Append(ctx, api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("published")})
		result <- err
	}()
	waitForIngressState(t, partition, func(admitted, credits, _, _ uint32) bool { return admitted == 1 && credits == 0 })
	cancel()
	if err := <-result; !errors.Is(err, api.ErrAppendOutcomeUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled published append = %v, want unknown context cancellation", err)
	}
	waitForIngressState(t, partition, func(admitted, _, _, _ uint32) bool { return admitted == 0 })
	if end, err := partition.EndOffset(); err != nil || end != 1 {
		t.Fatalf("end offset after unknown append = (%d, %v), want (1, nil)", end, err)
	}
}

func TestIngressWriteFailureFencesNewAppends(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	topic := testTopic()
	partition, err := store.OpenPartition(topic, 0, PartitionOptions{
		InFlightBytes:   1024,
		InFlightRecords: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	partition.mu.RLock()
	activePath := partition.segments[len(partition.segments)-1].path
	partition.mu.RUnlock()
	plan.failOnceExact(filesystemWriteAt, activePath, errors.New("injected ingress write failure"))
	result := make(chan error, 1)
	go func() {
		_, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("fails")})
		result <- err
	}()
	if err := <-result; !errors.Is(err, api.ErrAppendOutcomeUnknown) || !errors.Is(err, api.ErrPartitionUnavailable) {
		t.Fatalf("failed append error = %v, want unknown partition-unavailable outcome", err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: topic, Partition: 0, Value: []byte("fenced")}); !errors.Is(err, api.ErrPartitionUnavailable) {
		t.Fatalf("append after writer failure = %v, want ErrPartitionUnavailable", err)
	}
}
