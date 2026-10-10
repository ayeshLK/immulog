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

// Command restart demonstrates the application responsibilities around an
// unknown append outcome, process restart, and at-least-once delivery.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

var errReconciliationRequired = errors.New("application reconciliation required")

const reconciliationTimeout = 30 * time.Second

type report struct {
	ReconciledOffset uint64
	FirstDelivery    int
	Redelivery       int
	AppliedEffects   int
	FinalDelivery    int
}

func main() {
	dir, err := os.MkdirTemp("", "immulog-restart-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	result, err := run(context.Background(), dir)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("reconciled offset=%d first-delivery=%d redelivery=%d applied-effects=%d final-delivery=%d",
		result.ReconciledOffset, result.FirstDelivery, result.Redelivery, result.AppliedEffects, result.FinalDelivery)
}

func run(ctx context.Context, dir string) (report, error) {
	var result report
	store, err := storage.Open(dir)
	if err != nil {
		return result, err
	}
	descriptor, err := store.CreateTopic("orders", 1, storage.PartitionOptions{})
	if err != nil {
		_ = store.Close()
		return result, err
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		return result, err
	}

	request := api.AppendRequest{
		Topic:     descriptor.ID,
		Partition: 0,
		Key:       []byte("order-123"), // application-owned identity for reconciliation
		Value:     []byte(`{"status":"created"}`),
	}
	record, appendErr := partitions[0].Append(ctx, request)
	if appendErr != nil {
		if !errors.Is(appendErr, api.ErrAppendOutcomeUnknown) {
			_ = store.Close()
			return result, appendErr
		}
		// The terminal writer may still be finishing an admitted append. Close
		// the store before reopening so reconciliation observes the authority
		// selected by startup recovery rather than racing the writer.
		if err := store.Close(); err != nil {
			return result, err
		}
		recoveryCtx, cancel := context.WithTimeout(context.Background(), reconciliationTimeout)
		defer cancel()
		ctx = recoveryCtx
		store, err = storage.Open(dir)
		if err != nil {
			return result, err
		}
		partitions, err = store.OpenTopic(descriptor.Name)
		if err != nil {
			_ = store.Close()
			return result, err
		}
		record, appendErr = reconcileAppend(ctx, partitions[0], request, appendErr)
	}
	if appendErr != nil {
		_ = store.Close()
		return result, appendErr
	}
	result.ReconciledOffset = record.Offset

	consumer, err := store.OpenConsumer(ctx, "billing", descriptor.ID, 0, api.ConsumerOptions{
		Start: api.GroupStartEarliest,
	})
	if err != nil {
		_ = store.Close()
		return result, err
	}
	first, err := consumer.Poll(ctx, api.FetchOptions{MaxRecords: 10})
	if err != nil {
		_ = consumer.Close()
		_ = store.Close()
		return result, err
	}
	result.FirstDelivery = len(first.Records)
	// Simulate a process stop after the side effect but before Commit.
	effects := make(map[string]struct{})
	applyIdempotently(effects, first.Records)
	result.AppliedEffects = len(effects)
	if err := consumer.Close(); err != nil {
		_ = store.Close()
		return result, err
	}
	if err := store.Close(); err != nil {
		return result, err
	}

	store, err = storage.Open(dir)
	if err != nil {
		return result, err
	}
	if _, err = store.OpenTopic(descriptor.Name); err != nil {
		_ = store.Close()
		return result, err
	}
	consumer, err = store.OpenConsumer(ctx, "billing", descriptor.ID, 0, api.ConsumerOptions{
		Start: api.GroupStartEarliest,
	})
	if err != nil {
		_ = store.Close()
		return result, err
	}
	second, err := consumer.Poll(ctx, api.FetchOptions{MaxRecords: 10})
	if err != nil {
		_ = consumer.Close()
		_ = store.Close()
		return result, err
	}
	result.Redelivery = len(second.Records)
	applyIdempotently(effects, second.Records)
	if err := consumer.Commit(ctx, second.NextOffset); err != nil {
		_ = consumer.Close()
		_ = store.Close()
		return result, err
	}
	if err := consumer.Close(); err != nil {
		_ = store.Close()
		return result, err
	}
	if err := store.Close(); err != nil {
		return result, err
	}

	store, err = storage.Open(dir)
	if err != nil {
		return result, err
	}
	defer store.Close()
	consumer, err = store.OpenConsumer(ctx, "billing", descriptor.ID, 0, api.ConsumerOptions{
		Start: api.GroupStartEarliest,
	})
	if err != nil {
		return result, err
	}
	defer consumer.Close()
	final, err := consumer.Poll(ctx, api.FetchOptions{MaxRecords: 10})
	if err != nil {
		return result, err
	}
	result.FinalDelivery = len(final.Records)
	return result, nil
}

// reconcileAppend finds an already durable record by an application-owned key.
// Call this after writers are quiesced (normally after closing and reopening the
// store) so the scan observes the authoritative recovery boundary. A missing
// match remains an unknown outcome: callers must choose a retry policy that is
// safe for their application rather than blindly appending again.
func reconcileAppend(_ context.Context, partition *storage.Partition, request api.AppendRequest, appendErr error) (api.Record, error) {
	if !errors.Is(appendErr, api.ErrAppendOutcomeUnknown) {
		return api.Record{}, appendErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), reconciliationTimeout)
	defer cancel()
	stats := partition.Stats()
	for offset := stats.LogStartOffset; offset < stats.DurableEnd; {
		result, err := partition.Fetch(ctx, offset, api.FetchOptions{MaxRecords: 1, MaxBytes: 64 << 20})
		if err != nil {
			return api.Record{}, fmt.Errorf("reconcile append: %w", err)
		}
		for _, record := range result.Records {
			if bytes.Equal(record.Key, request.Key) {
				return record, nil
			}
		}
		if result.NextOffset <= offset {
			break
		}
		offset = result.NextOffset
	}
	return api.Record{}, fmt.Errorf("%w: key %q was not found", errReconciliationRequired, request.Key)
}

func applyIdempotently(effects map[string]struct{}, records []api.Record) {
	for _, record := range records {
		effects[string(record.Key)] = struct{}{}
	}
}
