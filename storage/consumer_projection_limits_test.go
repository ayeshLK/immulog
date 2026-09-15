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

func TestConsumerProjectionLimitsRefuseNewDurableGrowth(t *testing.T) {
	store, err := OpenWithOptions(t.TempDir(), StoreOptions{MaxConsumerGroups: 1, MaxConsumerProgressKeys: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("consumer-limits", 2, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenConsumer(context.Background(), "one", descriptor.ID, 0, api.ConsumerOptions{}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if before.ConsumerGroups != 1 || before.ConsumerProgressKeys != 1 || before.MaxConsumerGroups != 1 || before.MaxConsumerProgressKeys != 1 {
		t.Fatalf("consumer capacity stats = %#v", before)
	}
	if _, err := store.OpenConsumer(context.Background(), "two", descriptor.ID, 0, api.ConsumerOptions{}); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("group growth error = %v, want ErrResourceLimit", err)
	}
	if _, err := store.OpenConsumer(context.Background(), "one", descriptor.ID, 1, api.ConsumerOptions{}); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("progress-key growth error = %v, want ErrResourceLimit", err)
	}
	after, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.ConsumerGroups != 1 || after.ConsumerProgressKeys != 1 || after.ConsumerOffsets.DurableEnd != before.ConsumerOffsets.DurableEnd {
		t.Fatalf("refused consumer growth changed projection: before=%#v after=%#v", before, after)
	}
}

func TestConsumerProjectionLimitsRejectOversizedRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenWithOptions(dir, StoreOptions{MaxConsumerGroups: 2, MaxConsumerProgressKeys: 2})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("consumer-recovery-limits", 2, PartitionOptions{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.OpenConsumer(context.Background(), "one", descriptor.ID, 0, api.ConsumerOptions{}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.OpenConsumer(context.Background(), "two", descriptor.ID, 1, api.ConsumerOptions{}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithOptions(dir, StoreOptions{MaxConsumerGroups: 1, MaxConsumerProgressKeys: 2}); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("group recovery limit error = %v, want ErrResourceLimit", err)
	}
	if _, err := OpenWithOptions(dir, StoreOptions{MaxConsumerGroups: 2, MaxConsumerProgressKeys: 1}); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("progress-key recovery limit error = %v, want ErrResourceLimit", err)
	}
}
