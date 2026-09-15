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
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestSystemHistoryLimitsRefuseNewGrowthButPermitRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("history-limit", 1, PartitionOptions{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	stats, err := store.Stats()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	catalogBytes := stats.Catalog.LogicalLogBytes
	offsetsBytes := stats.ConsumerOffsets.LogicalLogBytes
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenWithOptions(dir, StoreOptions{
		MaxCatalogHistoryBytes: 1,
		MaxOffsetsHistoryBytes: offsetsBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("catalog-refused", 1, PartitionOptions{}); !errors.Is(err, api.ErrSystemLogCapacity) {
		_ = store.Close()
		t.Fatalf("catalog growth error = %v, want ErrSystemLogCapacity", err)
	}
	if _, err := store.OpenConsumer(context.Background(), "offsets-refused", descriptor.ID, 0, api.ConsumerOptions{}); !errors.Is(err, api.ErrSystemLogCapacity) {
		_ = store.Close()
		t.Fatalf("offsets growth error = %v, want ErrSystemLogCapacity", err)
	}
	after, err := store.Stats()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if after.Catalog.LogicalLogBytes != catalogBytes || after.ConsumerOffsets.LogicalLogBytes != offsetsBytes || after.Catalog.DurableEnd != stats.Catalog.DurableEnd || after.ConsumerOffsets.DurableEnd != stats.ConsumerOffsets.DurableEnd {
		_ = store.Close()
		t.Fatalf("refused growth changed system history: before=%#v after=%#v", stats, after)
	}
	if after.CatalogHistoryLimit != 1 || after.CatalogHistoryRemaining != 0 || after.OffsetsHistoryLimit != offsetsBytes || after.OffsetsHistoryRemaining != 0 {
		_ = store.Close()
		t.Fatalf("history capacity stats = %#v", after)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if store, err = Open(dir); err != nil {
		t.Fatalf("reopen after refused growth: %v", err)
	}
	defer store.Close()
	if topics, err := store.ListTopics(); err != nil || len(topics) != 1 || topics[0].ID != descriptor.ID {
		t.Fatalf("catalog after refused growth = %#v, %v", topics, err)
	}
}

func TestSystemHistoryLimitAppliesDuringStoreInitialization(t *testing.T) {
	dir := t.TempDir()
	if _, err := OpenWithOptions(dir, StoreOptions{MaxCatalogHistoryBytes: 1}); !errors.Is(err, api.ErrSystemLogCapacity) {
		t.Fatalf("initialization capacity error = %v, want ErrSystemLogCapacity", err)
	}
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("recover after refused initialization: %v", err)
	}
	defer store.Close()
}
