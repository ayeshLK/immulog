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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
)

func TestRetentionRemoveFailurePreservesBoundaryAndReportsDebt(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	store, partition, descriptor := openRetentionFaultFixture(t)
	retiredPath := filepath.Join(partition.dir, "00000000000000000000.log")
	plan.failOnceExact(filesystemRemove, retiredPath, errors.New("injected retired log removal failure"))
	if err := store.runRetentionAt(context.Background(), time.Now()); err == nil {
		t.Fatal("retention unexpectedly succeeded")
	}
	if !plan.triggered() {
		t.Fatal("retired log removal fault was not triggered")
	}
	updated, err := store.DescribeTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Partitions[0].RetainedL != 4 {
		t.Fatalf("retained L = %d, want 4", updated.Partitions[0].RetainedL)
	}
	if _, err := partition.Fetch(context.Background(), 0, api.FetchOptions{}); !errors.Is(err, api.ErrOffsetOutOfRange) {
		t.Fatalf("fetch expired offset error = %v, want ErrOffsetOutOfRange", err)
	}
	if _, err := os.Stat(retiredPath); err != nil {
		t.Fatalf("retired artifact stat = %v, want artifact retained for retry", err)
	}
	stats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Cleanup.PendingSegments == 0 || stats.Cleanup.PendingBytes == 0 {
		t.Fatalf("cleanup debt after failed removal = %#v", stats.Cleanup)
	}
	if err := store.RunRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(retiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired artifact after cleanup retry = %v, want not exist", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionDirectorySyncFailurePreservesBoundary(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	store, partition, descriptor := openRetentionFaultFixture(t)
	plan.failOnceExactAfter(filesystemSync, partition.dir, 1, errors.New("injected retention directory sync failure"))
	if err := store.runRetentionAt(context.Background(), time.Now()); err == nil {
		t.Fatal("retention unexpectedly succeeded")
	}
	if !plan.triggered() {
		t.Fatal("retention directory sync fault was not triggered")
	}
	updated, err := store.DescribeTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Partitions[0].RetainedL != 4 {
		t.Fatalf("retained L = %d, want 4", updated.Partitions[0].RetainedL)
	}
	if stats, err := store.Stats(); err != nil {
		t.Fatal(err)
	} else if stats.Cleanup.PendingSegments != 0 || stats.Cleanup.PendingBytes != 0 {
		t.Fatalf("cleanup debt after directory sync failure = %#v, want no artifacts", stats.Cleanup)
	}
	if err := store.RunRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func openRetentionFaultFixture(t *testing.T) (*Store, *Partition, TopicDescriptor) {
	t.Helper()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		RecordBytes: 512, BatchBytes: 600, BatchRecords: 1, SegmentBytes: 700,
		RetentionSizeEnabled: true, RetentionBytes: 0, RetentionCheck: time.Hour,
	}
	descriptor, err := store.CreateTopic("fault-retained", 1, options)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partition := partitions[0]
	for offset := uint64(0); offset < 4; offset++ {
		record, err := partition.Append(context.Background(), api.AppendRequest{
			Topic: descriptor.ID, Partition: 0, Value: bytes.Repeat([]byte("x"), 400),
		})
		if err != nil || record.Offset != offset {
			_ = store.Close()
			t.Fatalf("append %d = (%#v, %v)", offset, record, err)
		}
	}
	return store, partition, descriptor
}
