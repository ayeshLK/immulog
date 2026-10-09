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

func TestCompactSystemLogsPreservesProjectionsAndContinuesAbsoluteOffsets(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenWithOptions(dir, StoreOptions{MaxCatalogHistoryBytes: 4096, MaxOffsetsHistoryBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("before-compaction", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	consumerOptions := api.ConsumerOptions{Start: api.GroupStartEarliest, Fetch: api.FetchOptions{MaxRecords: 1, MaxBytes: 1024}}
	consumer, err := store.OpenConsumer(context.Background(), "checkpointed", descriptor.ID, 0, consumerOptions)
	if err != nil {
		t.Fatal(err)
	}
	polled, err := consumer.Poll(context.Background(), api.FetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Commit(context.Background(), polled.NextOffset); err != nil {
		t.Fatal(err)
	}
	catalogNext, offsetsNext := store.catalogState.revision, store.offsetsState.revision
	before, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.SystemLogMaintenance.Generation != 1 || after.SystemLogMaintenance.LastSuccess.IsZero() || after.SystemLogMaintenance.LastFailure != "" {
		t.Fatalf("maintenance stats = %#v", after.SystemLogMaintenance)
	}
	if after.Catalog.DurableEnd != catalogNext || after.ConsumerOffsets.DurableEnd != offsetsNext {
		t.Fatalf("suffix offsets = catalog %d offsets %d, want %d/%d", after.Catalog.DurableEnd, after.ConsumerOffsets.DurableEnd, catalogNext, offsetsNext)
	}
	if after.Catalog.LogicalLogBytes >= before.Catalog.LogicalLogBytes || after.ConsumerOffsets.LogicalLogBytes >= before.ConsumerOffsets.LogicalLogBytes {
		t.Fatalf("compaction did not restore logical headroom: before=%#v after=%#v", before, after)
	}
	second, err := store.CreateTopic("after-compaction", 1, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID.IsZero() || store.catalogState.revision != catalogNext+1 {
		t.Fatalf("catalog revision after continued write = %d", store.catalogState.revision)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	if store.offsetsState.revision != offsetsNext+1 {
		t.Fatalf("offsets revision after continued write = %d", store.offsetsState.revision)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenWithOptions(dir, StoreOptions{MaxCatalogHistoryBytes: 4096, MaxOffsetsHistoryBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.metadataGeneration != 1 || store.catalogState.revision != catalogNext+1 || store.offsetsState.revision != offsetsNext+1 {
		t.Fatalf("reopened generation/revisions = %d/%d/%d", store.metadataGeneration, store.catalogState.revision, store.offsetsState.revision)
	}
	if _, err := store.DescribeTopic("before-compaction"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DescribeTopic("after-compaction"); err != nil {
		t.Fatal(err)
	}
	resumed, err := store.OpenConsumer(context.Background(), "checkpointed", descriptor.ID, 0, consumerOptions)
	if err != nil {
		t.Fatal(err)
	}
	result, err := resumed.Poll(context.Background(), api.FetchOptions{})
	if err != nil || len(result.Records) != 0 || result.NextOffset != 1 {
		t.Fatalf("resumed consumer = %#v, %v", result, err)
	}
	wantGeneration := store.offsetsState.groups["checkpointed"].Generation
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenWithOptions(dir, StoreOptions{MaxCatalogHistoryBytes: 4096, MaxOffsetsHistoryBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	group := store.offsetsState.groups["checkpointed"]
	if store.metadataGeneration != 2 || group == nil || group.Generation != wantGeneration || group.Progress[topicKey{topic: descriptor.ID, partition: 0}].CommittedNext != 1 {
		t.Fatalf("second-generation consumer projection = %#v", group)
	}
}

func TestCommittedSystemGenerationCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("corrupt-checkpoint", 1, PartitionOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(generationPartitionDir(dir, 1, true), checkpointName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("open corrupt committed generation = %v, want ErrCorruptLog", err)
	}
}

func TestCommittedSystemGenerationMissingSuffixFailsWithoutRepair(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	segmentPath := store.catalog.segments[0].path
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(segmentPath); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("open missing committed suffix = %v, want ErrCorruptLog", err)
	}
	if _, err := os.Stat(segmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery recreated missing authoritative suffix: %v", err)
	}
}

func TestCompactSystemLogsHonorsCanceledContext(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.CompactSystemLogs(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CompactSystemLogs canceled error = %v", err)
	}
	if store.metadataGeneration != 0 {
		t.Fatalf("canceled compaction published generation %d", store.metadataGeneration)
	}
}

func TestCompactSystemLogsPublicationFailuresFenceUntilReopen(t *testing.T) {
	tests := []struct {
		name string
		arm  func(*filesystemFaultPlan, string)
	}{
		{
			name: "active-manifest-rename",
			arm: func(plan *filesystemFaultPlan, _ string) {
				plan.failOnce(filesystemRename, filepath.Join(metadataRootDir, activeManifestName), errors.New("injected active manifest rename failure"))
			},
		},
		{
			name: "active-manifest-directory-sync",
			arm: func(plan *filesystemFaultPlan, dir string) {
				plan.failOnceExactAfter(filesystemSync, metadataRoot(dir), 1, errors.New("injected active manifest directory sync failure"))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := &filesystemFaultPlan{}
			installFilesystemFault(t, plan)
			dir := t.TempDir()
			store, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err := store.CreateTopic("before-fault", 1, PartitionOptions{}); err != nil {
				t.Fatal(err)
			}
			test.arm(plan, dir)
			err = store.CompactSystemLogs(context.Background())
			if !errors.Is(err, api.ErrMetadataOutcomeUnknown) || !errors.Is(err, api.ErrCommitOutcomeUnknown) {
				t.Fatalf("compaction error = %v, want both unknown-outcome sentinels", err)
			}
			if !plan.triggered() {
				t.Fatal("publication fault was not triggered")
			}
			if _, err := store.CreateTopic("fenced", 1, PartitionOptions{}); !errors.Is(err, api.ErrMetadataUnavailable) {
				t.Fatalf("metadata mutation after publication fault = %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen after publication fault: %v", err)
			}
			defer reopened.Close()
			if _, err := reopened.DescribeTopic("before-fault"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactSystemLogsFailureBeforeAuthoritySwitchKeepsLegacyWritable(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	plan.failOnce(filesystemRename, filepath.Join(metadataGenerationsDir, "00000000000000000001", generationManifestName), errors.New("injected generation manifest failure"))
	if err := store.CompactSystemLogs(context.Background()); err == nil || errors.Is(err, api.ErrMetadataOutcomeUnknown) || errors.Is(err, api.ErrCommitOutcomeUnknown) {
		t.Fatalf("pre-switch compaction error = %v, want known failure", err)
	}
	if _, err := store.CreateTopic("legacy-still-writable", 1, PartitionOptions{}); err != nil {
		t.Fatalf("legacy authority was fenced after known pre-switch failure: %v", err)
	}
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.SystemLogMaintenance.PendingCleanupFiles == 0 {
		t.Fatal("unpublished generation was not reported as cleanup debt")
	}
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, err = store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.SystemLogMaintenance.Generation != 2 || stats.SystemLogMaintenance.PendingCleanupFiles != 0 {
		t.Fatalf("maintenance after stale-generation cleanup = %#v", stats.SystemLogMaintenance)
	}
}

func TestCompactSystemLogsCanPublishRepeatedGenerations(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("generation-one", 1, PartitionOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshots(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("generation-two", 1, PartitionOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.metadataGeneration != 2 {
		t.Fatalf("metadata generation = %d, want 2", store.metadataGeneration)
	}
	if err := store.SaveSnapshots(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.metadataGeneration != 2 {
		t.Fatalf("reopened metadata generation = %d, want 2", reopened.metadataGeneration)
	}
	if diagnostics := reopened.SnapshotDiagnostics(); diagnostics.Catalog != nil || diagnostics.Offsets != nil {
		t.Fatalf("generation snapshot diagnostics = %#v", diagnostics)
	}
	for _, name := range []string{"generation-one", "generation-two"} {
		if _, err := reopened.DescribeTopic(name); err != nil {
			t.Fatalf("describe %q: %v", name, err)
		}
	}
}

func TestCompactSystemLogsReportsAndRetriesCleanupDebt(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	plan.failOnce(filesystemRemove, filepath.FromSlash(clusterMetadataDir), errors.New("injected old catalog cleanup failure"))
	if err := store.CompactSystemLogs(context.Background()); err == nil {
		t.Fatal("compaction unexpectedly hid cleanup failure")
	}
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.SystemLogMaintenance.Generation != 1 || stats.SystemLogMaintenance.PendingCleanupFiles == 0 || stats.SystemLogMaintenance.PendingCleanupBytes == 0 {
		t.Fatalf("cleanup debt stats = %#v", stats.SystemLogMaintenance)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, err = store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.SystemLogMaintenance.PendingCleanupFiles != 0 || stats.SystemLogMaintenance.PendingCleanupBytes != 0 {
		t.Fatalf("cleanup debt after retry = %#v", stats.SystemLogMaintenance)
	}
}

func TestCompactSystemLogsPreservesRetentionBoundaryAndCleanupInventory(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	store, partition, descriptor := openRetentionFaultFixture(t)
	dir := store.rootPath
	retiredPath := filepath.Join(partition.dir, "00000000000000000000.log")
	plan.failOnceExact(filesystemRemove, retiredPath, errors.New("injected retired segment cleanup failure"))
	if err := store.runRetentionAt(context.Background(), time.Now()); err == nil {
		t.Fatal("retention unexpectedly hid cleanup failure")
	}
	if len(store.catalogState.topicsByID[descriptor.ID].retired[0]) == 0 {
		t.Fatal("retention cleanup inventory was not projected")
	}
	if err := store.CompactSystemLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.catalogState.topicsByID[descriptor.ID].retired[0]) == 0 {
		t.Fatal("compaction dropped live retention cleanup inventory")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.DescribeTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Partitions[0].RetainedL != 4 {
		t.Fatalf("retained L after compacted reopen = %d, want 4", got.Partitions[0].RetainedL)
	}
	if _, err := os.Stat(retiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired cleanup debt after reopen = %v, want reconciled", err)
	}
}

func TestCompactSystemLogsBoundsRepeatedOffsetsChurn(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenWithOptions(dir, StoreOptions{MaxCatalogHistoryBytes: 1024, MaxOffsetsHistoryBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("bounded-offset-churn", 1, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	options := api.ConsumerOptions{Start: api.GroupStartEarliest}
	for generation := uint64(1); generation <= 12; generation++ {
		consumer, err := store.OpenConsumer(context.Background(), "bounded", descriptor.ID, 0, options)
		if err != nil {
			t.Fatalf("open consumer generation %d: %v", generation, err)
		}
		if err := consumer.Close(); err != nil {
			t.Fatalf("close consumer generation %d: %v", generation, err)
		}
		if err := store.CompactSystemLogs(context.Background()); err != nil {
			t.Fatalf("compact generation %d: %v", generation, err)
		}
		stats, err := store.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.ConsumerOffsets.LogicalLogBytes != uint64(SegmentHeaderBytes) || stats.OffsetsHistoryRemaining != 1024-uint64(SegmentHeaderBytes) {
			t.Fatalf("offset suffix after generation %d = %#v", generation, stats.ConsumerOffsets)
		}
	}
	entries, err := os.ReadDir(filepath.Join(metadataRoot(dir), metadataGenerationsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "00000000000000000012" {
		t.Fatalf("generation directories after churn = %#v", entries)
	}
}
