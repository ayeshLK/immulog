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
	"sync"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestCatalogPreflightRejectsMissingPartitionStorage(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("preflight", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	partitionDir := filepath.Join(dir, "topics", descriptor.ID.String(), "0")
	if err := os.RemoveAll(partitionDir); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("missing catalog partition open error = %v", err)
	}
}

func TestCatalogPreflightRejectsUnexpectedPartitionFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("preflight-file", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	partitionDir := filepath.Join(dir, "topics", descriptor.ID.String(), "0")
	if err := os.WriteFile(filepath.Join(partitionDir, "unexpected.data"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("unexpected partition file open error = %v", err)
	}
}

func TestConcurrentTopicCreationPublishesOneCatalogName(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const attempts = 8
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.CreateTopic("concurrent", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	successes := 0
	duplicates := 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, api.ErrTopicExists) {
			duplicates++
		} else {
			t.Fatalf("concurrent creation error = %v", err)
		}
	}
	if successes != 1 || duplicates != attempts-1 {
		t.Fatalf("concurrent creation results: successes=%d duplicates=%d", successes, duplicates)
	}
}

func TestCatalogAdmissionHonorsContextCancellation(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.catalogAdmission <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.CreateTopicContext(ctx, "cancelled", 1, PartitionOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("catalog cancellation error = %v", err)
	}
	<-store.catalogAdmission
}

func TestCatalogPreflightRejectsOrphanWhenCatalogIsOwned(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("owned", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(dir, "topics", "ffffffffffffffffffffffffffffffff", "0")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("orphan catalog preflight error = %v", err)
	}
}

func TestCatalogPreflightRejectsCompleteCorruptBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("preflight-complete-corrupt", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partitions[0].AppendBatch(testBatch(descriptor.ID, 0, 0, "durable")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "topics", descriptor.ID.String(), "0", "00000000000000000000.log")
	before, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	before[len(before)-5] ^= 1
	if err := os.WriteFile(logPath, before, 0o644); err != nil {
		t.Fatal(err)
	}
	corruptBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("complete corrupt catalog batch open error = %v", err)
	}
	after, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(corruptBytes) {
		t.Fatal("catalog preflight modified a complete corrupt batch")
	}
}

func TestCatalogPreflightAllowsVerifiedIncompleteFinalBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("preflight-incomplete-tail", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partitions[0].AppendBatch(testBatch(descriptor.ID, 0, 0, "durable")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "topics", descriptor.ID.String(), "0", "00000000000000000000.log")
	unknown, err := EncodeBatch(testBatch(descriptor.ID, 0, 1, "unknown"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(unknown[:BatchHeaderBytes]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partitions, err = store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := partitions[0].EndOffset(); err != nil || got != 1 {
		t.Fatalf("recovered catalog partition end offset = (%d, %v), want (1, nil)", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSystemPreflightPreservesCompleteCorruptCatalogBatch(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, clusterMetadataDir, "00000000000000000000.log")
	corruptBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	corruptBytes[len(corruptBytes)-5] ^= 1
	if err := os.WriteFile(logPath, corruptBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("corrupt catalog preflight error = %v", err)
	}
	after, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(corruptBytes) {
		t.Fatal("system preflight modified a complete corrupt catalog batch")
	}
}

func TestSystemPreflightAllowsVerifiedIncompleteCatalogTail(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, clusterMetadataDir, "00000000000000000000.log")
	original, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := EncodeBatch(testBatch(api.ClusterMetadataTopicID, 0, 1, "unknown"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(unknown[:BatchHeaderBytes]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Size(), original.Size(); got != want {
		t.Fatalf("recovered catalog size = %d, want %d", got, want)
	}
}
