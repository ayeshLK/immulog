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
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestStoreOperatingLimitsRefuseCatalogGrowthAndOversizedRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("one", 2, PartitionOptions{}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("two", 1, PartitionOptions{}); !errors.Is(err, api.ErrResourceLimit) {
		_ = store.Close()
		t.Fatalf("topic-limit error = %v, want ErrResourceLimit", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 0, MaxUserPartitions: 1}); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("recovery partition-limit error = %v, want ErrResourceLimit", err)
	}
}

func TestStoreOperatingLimitsRefusePartitionGrowthBeforeTopicPreparation(t *testing.T) {
	store, err := OpenWithOptions(t.TempDir(), StoreOptions{MaxTopics: 2, MaxUserPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("too-many-parts", 2, PartitionOptions{}); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("partition-limit error = %v, want ErrResourceLimit", err)
	}
	if topics, err := store.ListTopics(); err != nil || len(topics) != 0 {
		t.Fatalf("catalog after refused topic = %#v, %v", topics, err)
	}
}

func TestStoreActivePartitionLimitRejectsTopicOpenAtomically(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 2, MaxOpenPartitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("two-active", 2, PartitionOptions{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenWithOptions(dir, StoreOptions{MaxTopics: 1, MaxUserPartitions: 2, MaxOpenPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.OpenTopic(descriptor.Name); !errors.Is(err, api.ErrResourceLimit) {
		t.Fatalf("active-partition open error = %v, want ErrResourceLimit", err)
	}
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.OpenPartitions != 0 {
		t.Fatalf("open partitions after refused topic open = %d, want 0", stats.OpenPartitions)
	}
}
