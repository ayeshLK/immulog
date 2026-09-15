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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
)

func TestBootstrapPersistsStoreInitializedAndCatalog(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, clusterMetadataDir, "00000000000000000000.log")); err != nil {
		t.Fatalf("catalog log: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, consumerOffsetsDir, "00000000000000000000.log")); err != nil {
		t.Fatalf("offsets log: %v", err)
	}
	descriptor, err := store.CreateTopic("orders", 2, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.Name != "orders" || len(descriptor.Partitions) != 2 || descriptor.ID.IsZero() {
		t.Fatalf("unexpected descriptor: %#v", descriptor)
	}
	partitions, err := store.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	request := api.AppendRequest{Topic: descriptor.ID, Partition: 1, Value: []byte("created")}
	record, err := partitions[1].Append(context.Background(), request)
	if err != nil || record.Offset != 0 {
		t.Fatalf("catalog-created partition append = (%#v, %v), want offset zero", record, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := store.DescribeTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ID != descriptor.ID || len(recovered.Partitions) != 2 {
		t.Fatalf("recovered descriptor = %#v, want %#v", recovered, descriptor)
	}
	if _, err := store.CreateTopic("orders", 2, PartitionOptions{}); !errors.Is(err, api.ErrTopicExists) {
		t.Fatalf("duplicate topic error = %v, want ErrTopicExists", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUninitializedDirectoryWithAuthoritativeDataIsRejected(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unexpected.log"), []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("open error = %v, want ErrCorruptLog", err)
	}
}

func TestMetadataUnavailableClosesCatalogGates(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.metadataUnavailable = true
	if _, err := store.CreateTopic("unavailable", 1, PartitionOptions{}); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("CreateTopic error = %v", err)
	}
	if _, err := store.DescribeTopic("unavailable"); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("DescribeTopic error = %v", err)
	}
	if _, err := store.ListTopics(); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("ListTopics error = %v", err)
	}
	if _, err := store.OpenTopic("unavailable"); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("OpenTopic error = %v", err)
	}
	if _, err := store.OpenPartition(testTopic(), 0, PartitionOptions{}); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("OpenPartition error = %v", err)
	}
	if err := store.SaveSnapshots(); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("SaveSnapshots error = %v", err)
	}
}
func TestCatalogOwnedCancellationReturnsUnknownAndCompletes(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
		_ = store.Close()
	}()
	store.catalogAppend = func(batch api.RecordBatch) (uint64, error) {
		select {
		case <-entered:
		default:
			close(entered)
			<-release
		}
		return store.catalog.AppendBatch(batch)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := store.CreateTopicContext(ctx, "eventually-published", 1, PartitionOptions{})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("catalog append did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, api.ErrMetadataOutcomeUnknown) {
			t.Fatalf("CreateTopicContext cancellation error = %v, want ErrMetadataOutcomeUnknown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CreateTopicContext did not return after cancellation")
	}
	close(release)
	released = true
	if _, err := store.CreateTopic("following", 1, PartitionOptions{}); err != nil {
		t.Fatalf("create topic after cancelled caller: %v", err)
	}
	if _, err := store.DescribeTopic("eventually-published"); err != nil {
		t.Fatalf("durable catalog work was not published: %v", err)
	}
}

func TestUnknownCatalogAppendOutcomeClosesMetadataGates(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.catalogAppend = func(api.RecordBatch) (uint64, error) {
		return 0, errors.Join(api.ErrAppendOutcomeUnknown, errors.New("injected catalog sync failure"))
	}
	if _, err := store.CreateTopic("uncertain", 1, PartitionOptions{}); !errors.Is(err, api.ErrAppendOutcomeUnknown) {
		t.Fatalf("CreateTopic unknown-outcome error = %v", err)
	}
	if _, err := store.DescribeTopic("uncertain"); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("DescribeTopic after unknown outcome = %v", err)
	}
	if _, err := store.ListTopics(); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("ListTopics after unknown outcome = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen with prepared topic storage: %v", err)
	}
	descriptor, err := store.CreateTopic("published", 1, PartitionOptions{})
	if err != nil {
		t.Fatalf("create topic after uncertain outcome: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen with catalog and prepared topic storage: %v", err)
	}
	defer store.Close()
	if _, err := store.DescribeTopic(descriptor.Name); err != nil {
		t.Fatalf("describe published topic after recovery: %v", err)
	}
}

func TestDurableButUnacknowledgedCatalogCreationReplays(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.catalogAppend = func(batch api.RecordBatch) (uint64, error) {
		offset, err := store.catalog.AppendBatch(batch)
		if err != nil {
			return offset, err
		}
		return offset, errors.Join(api.ErrAppendOutcomeUnknown, errors.New("injected lost catalog append response"))
	}
	if _, err := store.CreateTopic("durable-uncertain", 1, PartitionOptions{}); !errors.Is(err, api.ErrAppendOutcomeUnknown) {
		t.Fatalf("CreateTopic unknown-outcome error = %v", err)
	}
	if _, err := store.DescribeTopic("durable-uncertain"); !errors.Is(err, api.ErrMetadataUnavailable) {
		t.Fatalf("DescribeTopic after durable unknown outcome = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen durable but unacknowledged topic: %v", err)
	}
	defer store.Close()
	descriptor, err := store.DescribeTopic("durable-uncertain")
	if err != nil {
		t.Fatalf("describe durable but unacknowledged topic: %v", err)
	}
	if descriptor.Name != "durable-uncertain" || len(descriptor.Partitions) != 1 {
		t.Fatalf("recovered descriptor = %#v", descriptor)
	}
}
