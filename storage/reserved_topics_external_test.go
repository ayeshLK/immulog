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

package storage_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

func TestReservedTopicIDCopiesDoNotAffectStorageIdentity(t *testing.T) {
	dir := t.TempDir()
	alteredCatalog := api.ClusterMetadataTopicID()
	alteredCatalog[0] = 0x7f
	alteredOffsets := api.ConsumerOffsetsTopicID()
	alteredOffsets[0] = 0x7f

	store, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []api.TopicID{api.ClusterMetadataTopicID(), api.ConsumerOffsetsTopicID()} {
		if _, err := store.OpenPartition(topic, 0, storage.PartitionOptions{}); !errors.Is(err, api.ErrInvalidArgument) {
			_ = store.Close()
			t.Fatalf("reserved partition error = %v, want ErrInvalidArgument", err)
		}
	}
	liveCopy := api.ConsumerOffsetsTopicID()
	liveCopy[1] = 0x3f
	if alteredCatalog == api.ClusterMetadataTopicID() || alteredOffsets == api.ConsumerOffsetsTopicID() || liveCopy == api.ConsumerOffsetsTopicID() {
		_ = store.Close()
		t.Fatal("caller mutation changed a canonical reserved topic ID")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	assertSystemSegmentTopic(t, dir, "cluster-metadata", api.ClusterMetadataTopicID())
	assertSystemSegmentTopic(t, dir, "consumer-offsets", api.ConsumerOffsetsTopicID())

	store, err = storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, topic := range []api.TopicID{api.ClusterMetadataTopicID(), api.ConsumerOffsetsTopicID()} {
		if _, err := store.OpenPartition(topic, 0, storage.PartitionOptions{}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("reopened reserved partition error = %v, want ErrInvalidArgument", err)
		}
	}
}

func assertSystemSegmentTopic(t *testing.T, dir, systemLog string, want api.TopicID) {
	t.Helper()
	path := filepath.Join(dir, "system", systemLog, "0", "00000000000000000000.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < int(storage.SegmentHeaderBytes) {
		t.Fatalf("system segment length = %d, want at least %d", len(data), storage.SegmentHeaderBytes)
	}
	header, err := storage.DecodeSegmentHeader(data[:storage.SegmentHeaderBytes], want, 0)
	if err != nil {
		t.Fatal(err)
	}
	if header.Topic != want {
		t.Fatalf("system segment topic = %v, want %v", header.Topic, want)
	}
}
