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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

type filesystemOperation string

const (
	filesystemMkdirAll  filesystemOperation = "mkdir-all"
	filesystemOpen      filesystemOperation = "open"
	filesystemOpenFile  filesystemOperation = "open-file"
	filesystemCreateTmp filesystemOperation = "create-temp"
	filesystemReadDir   filesystemOperation = "read-dir"
	filesystemStat      filesystemOperation = "stat"
	filesystemReadFile  filesystemOperation = "read-file"
	filesystemRename    filesystemOperation = "rename"
	filesystemRemove    filesystemOperation = "remove"
	filesystemWrite     filesystemOperation = "write"
	filesystemWriteAt   filesystemOperation = "write-at"
	filesystemReadAt    filesystemOperation = "read-at"
	filesystemFileStat  filesystemOperation = "file-stat"
	filesystemSync      filesystemOperation = "sync"
	filesystemClose     filesystemOperation = "close"
	filesystemTruncate  filesystemOperation = "truncate"
)

type filesystemFaultAction struct {
	err        error
	shortWrite bool
}

type filesystemFaultPlan struct {
	mu        sync.Mutex
	armed     bool
	operation filesystemOperation
	path      string
	exact     bool
	skip      int
	action    filesystemFaultAction
}

func (plan *filesystemFaultPlan) failOnce(operation filesystemOperation, path string, err error) {
	plan.mu.Lock()
	plan.armed = true
	plan.operation = operation
	plan.path = path
	plan.exact = false
	plan.skip = 0
	plan.action = filesystemFaultAction{err: err}
	plan.mu.Unlock()
}

func (plan *filesystemFaultPlan) failOnceExact(operation filesystemOperation, path string, err error) {
	plan.mu.Lock()
	plan.armed = true
	plan.operation = operation
	plan.path = path
	plan.exact = true
	plan.skip = 0
	plan.action = filesystemFaultAction{err: err}
	plan.mu.Unlock()
}

func (plan *filesystemFaultPlan) failOnceExactAfter(operation filesystemOperation, path string, skip int, err error) {
	plan.mu.Lock()
	plan.armed = true
	plan.operation = operation
	plan.path = path
	plan.exact = true
	plan.skip = skip
	plan.action = filesystemFaultAction{err: err}
	plan.mu.Unlock()
}

func (plan *filesystemFaultPlan) shortWriteOnce(operation filesystemOperation, path string) {
	plan.mu.Lock()
	plan.armed = true
	plan.operation = operation
	plan.path = path
	plan.exact = false
	plan.skip = 0
	plan.action = filesystemFaultAction{shortWrite: true}
	plan.mu.Unlock()
}

func (plan *filesystemFaultPlan) take(operation filesystemOperation, path string) (filesystemFaultAction, bool) {
	plan.mu.Lock()
	defer plan.mu.Unlock()
	pathMatches := plan.path == "" || plan.exact && path == plan.path || !plan.exact && strings.Contains(path, plan.path)
	if !plan.armed || plan.operation != operation || !pathMatches {
		return filesystemFaultAction{}, false
	}
	if plan.skip > 0 {
		plan.skip--
		return filesystemFaultAction{}, false
	}
	plan.armed = false
	return plan.action, true
}

func (plan *filesystemFaultPlan) triggered() bool {
	plan.mu.Lock()
	defer plan.mu.Unlock()
	return !plan.armed
}

func installFilesystemFault(t *testing.T, plan *filesystemFaultPlan) {
	t.Helper()
	base := operatingSystemFileSystem()
	faulted := base
	faulted.mkdirAll = func(path string, permission os.FileMode) error {
		if action, ok := plan.take(filesystemMkdirAll, path); ok && action.err != nil {
			return action.err
		}
		return base.mkdirAll(path, permission)
	}
	faulted.open = func(path string) (*os.File, error) {
		if action, ok := plan.take(filesystemOpen, path); ok && action.err != nil {
			return nil, action.err
		}
		return base.open(path)
	}
	faulted.openFile = func(path string, flags int, permission os.FileMode) (*os.File, error) {
		if action, ok := plan.take(filesystemOpenFile, path); ok && action.err != nil {
			return nil, action.err
		}
		return base.openFile(path, flags, permission)
	}
	faulted.createTmp = func(directory, pattern string) (*os.File, error) {
		if action, ok := plan.take(filesystemCreateTmp, filepath.Join(directory, pattern)); ok && action.err != nil {
			return nil, action.err
		}
		return base.createTmp(directory, pattern)
	}
	faulted.readDir = func(path string) ([]os.DirEntry, error) {
		if action, ok := plan.take(filesystemReadDir, path); ok && action.err != nil {
			return nil, action.err
		}
		return base.readDir(path)
	}
	faulted.stat = func(path string) (os.FileInfo, error) {
		if action, ok := plan.take(filesystemStat, path); ok && action.err != nil {
			return nil, action.err
		}
		return base.stat(path)
	}
	faulted.readFile = func(path string) ([]byte, error) {
		if action, ok := plan.take(filesystemReadFile, path); ok && action.err != nil {
			return nil, action.err
		}
		return base.readFile(path)
	}
	faulted.rename = func(oldPath, newPath string) error {
		if action, ok := plan.take(filesystemRename, oldPath+" -> "+newPath); ok && action.err != nil {
			return action.err
		}
		return base.rename(oldPath, newPath)
	}
	faulted.remove = func(path string) error {
		if action, ok := plan.take(filesystemRemove, path); ok && action.err != nil {
			return action.err
		}
		return base.remove(path)
	}
	faulted.write = func(file *os.File, data []byte) (int, error) {
		if action, ok := plan.take(filesystemWrite, file.Name()); ok {
			if action.err != nil {
				return 0, action.err
			}
			if action.shortWrite && len(data) != 0 {
				return base.write(file, data[:len(data)-1])
			}
		}
		return base.write(file, data)
	}
	faulted.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		if action, ok := plan.take(filesystemWriteAt, file.Name()); ok {
			if action.err != nil {
				return 0, action.err
			}
			if action.shortWrite && len(data) != 0 {
				return base.writeAt(file, data[:len(data)-1], offset)
			}
		}
		return base.writeAt(file, data, offset)
	}
	faulted.readAt = func(file *os.File, data []byte, offset int64) (int, error) {
		if action, ok := plan.take(filesystemReadAt, file.Name()); ok && action.err != nil {
			return 0, action.err
		}
		return base.readAt(file, data, offset)
	}
	faulted.statFile = func(file *os.File) (os.FileInfo, error) {
		if action, ok := plan.take(filesystemFileStat, file.Name()); ok && action.err != nil {
			return nil, action.err
		}
		return base.statFile(file)
	}
	faulted.sync = func(file *os.File) error {
		if action, ok := plan.take(filesystemSync, file.Name()); ok && action.err != nil {
			return action.err
		}
		return base.sync(file)
	}
	faulted.close = func(file *os.File) error {
		if action, ok := plan.take(filesystemClose, file.Name()); ok && action.err != nil {
			return action.err
		}
		return base.close(file)
	}
	faulted.truncate = func(file *os.File, size int64) error {
		if action, ok := plan.take(filesystemTruncate, file.Name()); ok && action.err != nil {
			return action.err
		}
		return base.truncate(file, size)
	}

	fileSystemMu.Lock()
	previous := fileSystem
	fileSystem = faulted
	fileSystemMu.Unlock()
	t.Cleanup(func() {
		fileSystemMu.Lock()
		fileSystem = previous
		fileSystemMu.Unlock()
	})
}

func TestBootstrapDirectorySyncFailureReleasesOwnership(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir := t.TempDir()
	plan.failOnceExact(filesystemSync, dir, errors.New("injected bootstrap directory sync failure"))
	if _, err := Open(dir); err == nil {
		t.Fatal("open unexpectedly succeeded")
	}
	if !plan.triggered() {
		t.Fatal("bootstrap directory sync fault was not triggered")
	}
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); err != nil {
		t.Fatalf("persistent lock after failed open = %v", err)
	}
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func openFaultPartition(t *testing.T, options PartitionOptions) (string, *Store, *Partition) {
	t.Helper()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(testTopic(), 0, options)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	return dir, store, partition
}

func TestAppendPersistenceFaultsFencePartition(t *testing.T) {
	tests := []struct {
		name      string
		operation filesystemOperation
	}{
		{name: "write-at", operation: filesystemWriteAt},
		{name: "sync", operation: filesystemSync},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := &filesystemFaultPlan{}
			installFilesystemFault(t, plan)
			_, store, partition := openFaultPartition(t, PartitionOptions{})
			defer store.Close()
			base, err := partition.EndOffset()
			if err != nil {
				t.Fatal(err)
			}
			plan.failOnce(test.operation, partition.segments[len(partition.segments)-1].path, fmt.Errorf("injected %s failure", test.name))
			if _, err := partition.AppendBatch(testBatch(partition.topic, partition.partition, base, "faulted")); !errors.Is(err, api.ErrAppendOutcomeUnknown) || !errors.Is(err, api.ErrPartitionUnavailable) {
				t.Fatalf("append error = %v, want unknown unavailable outcome", err)
			}
			if _, err := partition.AppendBatch(testBatch(partition.topic, partition.partition, base, "after-fault")); !errors.Is(err, api.ErrPartitionUnavailable) {
				t.Fatalf("append after persistence fault = %v, want partition unavailable", err)
			}
		})
	}
}

func TestShortAppendWriteRecoversPermittedTail(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir, store, partition := openFaultPartition(t, PartitionOptions{})
	defer store.Close()
	base, err := partition.EndOffset()
	if err != nil {
		t.Fatal(err)
	}
	plan.shortWriteOnce(filesystemWriteAt, partition.segments[len(partition.segments)-1].path)
	if _, err := partition.AppendBatch(testBatch(partition.topic, partition.partition, base, "short-write")); !errors.Is(err, api.ErrAppendOutcomeUnknown) {
		t.Fatalf("short write error = %v, want unknown outcome", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	partition, err = reopened.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := partition.EndOffset(); err != nil || got != base {
		t.Fatalf("recovered end offset = (%d, %v), want (%d, nil)", got, err, base)
	}
	if _, err := partition.AppendBatch(testBatch(partition.topic, partition.partition, base, "after-recovery")); err != nil {
		t.Fatalf("append after permitted tail recovery = %v", err)
	}
}

func TestSegmentRollWriteFailurePreservesPriorSegment(t *testing.T) {
	topic := testTopic()
	first := testBatch(topic, 0, 0, strings.Repeat("a", 120))
	encoded, err := EncodeBatch(first)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		SegmentBytes: uint64(SegmentHeaderBytes) + uint64(len(encoded)) + 1,
		BatchBytes:   4096,
		BatchRecords: 8,
		RecordBytes:  1024,
	}
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir, store, partition := openFaultPartition(t, options)
	if _, err := partition.AppendBatch(first); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	plan.failOnce(filesystemWrite, ".segment-", errors.New("injected segment header write failure"))
	if _, err := partition.AppendBatch(testBatch(topic, 0, 1, "roll")); !errors.Is(err, api.ErrPartitionUnavailable) {
		t.Fatalf("roll error = %v, want partition unavailable", err)
	}
	if got := len(partition.segments); got != 1 {
		t.Fatalf("segment count after failed roll = %d, want 1", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	partition, err = reopened.OpenPartition(topic, 0, options)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := partition.EndOffset(); err != nil || got != 1 {
		t.Fatalf("recovered end offset = (%d, %v), want (1, nil)", got, err)
	}
}

func TestSegmentRollPublicationFaultsPreservePriorRecord(t *testing.T) {
	topic := testTopic()
	first := testBatch(topic, 0, 0, strings.Repeat("a", 120))
	encoded, err := EncodeBatch(first)
	if err != nil {
		t.Fatal(err)
	}
	options := PartitionOptions{
		SegmentBytes: uint64(SegmentHeaderBytes) + uint64(len(encoded)) + 1,
		BatchBytes:   4096,
		BatchRecords: 8,
		RecordBytes:  1024,
	}
	tests := []struct {
		name      string
		operation filesystemOperation
		path      string
		exact     bool
		segments  int
	}{
		{name: "header-write", operation: filesystemWrite, path: ".segment-", segments: 1},
		{name: "header-sync", operation: filesystemSync, path: ".segment-", segments: 1},
		{name: "rename", operation: filesystemRename, path: ".segment-", segments: 1},
		{name: "directory-sync", operation: filesystemSync, exact: true, segments: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := &filesystemFaultPlan{}
			installFilesystemFault(t, plan)
			dir, store, partition := openFaultPartition(t, options)
			if _, err := partition.AppendBatch(first); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			if test.exact {
				plan.failOnceExact(test.operation, partition.dir, errors.New("injected segment directory sync failure"))
			} else {
				plan.failOnce(test.operation, test.path, fmt.Errorf("injected segment %s failure", test.name))
			}
			if _, err := partition.AppendBatch(testBatch(topic, 0, 1, "roll")); !errors.Is(err, api.ErrPartitionUnavailable) {
				t.Fatalf("roll error = %v, want partition unavailable", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			partition, err = reopened.OpenPartition(topic, 0, options)
			if err != nil {
				t.Fatal(err)
			}
			segments, err := discoverSegments(partition.dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(segments) != test.segments {
				t.Fatalf("recovered segment count = %d, want %d", len(segments), test.segments)
			}
			records, err := partition.Read(0, 2)
			if err != nil || len(records) != 1 || records[0].Offset != 0 || string(records[0].Value) != strings.Repeat("a", 120) {
				t.Fatalf("recovered prior record = %#v, %v", records, err)
			}
			if _, err := partition.AppendBatch(testBatch(topic, 0, 1, "after-recovery")); err != nil {
				t.Fatalf("append after failed roll recovery = %v", err)
			}
		})
	}
}

func TestSnapshotPublicationFaultsKeepPreviousCache(t *testing.T) {
	tests := []struct {
		name      string
		operation filesystemOperation
		path      string
	}{
		{name: "write", operation: filesystemWrite, path: ".projection-snapshot-"},
		{name: "sync", operation: filesystemSync, path: ".projection-snapshot-"},
		{name: "rename", operation: filesystemRename, path: ".projection-snapshot-"},
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
			if err := store.SaveSnapshots(); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			path := filepath.Join(dir, clusterMetadataDir, snapshotPathName)
			before, err := os.ReadFile(path)
			if err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			plan.failOnce(test.operation, test.path, fmt.Errorf("injected snapshot %s failure", test.name))
			if err := store.SaveSnapshots(); err == nil {
				t.Fatal("snapshot save unexpectedly succeeded")
			}
			if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, before) {
				t.Fatalf("previous snapshot changed after failed replacement: err=%v", err)
			}
			diagnostics := store.SnapshotDiagnostics()
			if diagnostics.Catalog == nil {
				t.Fatal("catalog snapshot failure was not reported")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotDirectorySyncFailureRemainsRecoverable(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshots(); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	plan.failOnceExact(filesystemSync, filepath.Join(dir, clusterMetadataDir), errors.New("injected snapshot directory sync failure"))
	if err := store.SaveSnapshots(); err == nil {
		t.Fatal("snapshot save unexpectedly succeeded")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if diagnostics := reopened.SnapshotDiagnostics(); diagnostics.Catalog != nil {
		t.Fatalf("reopened snapshot diagnostics = %#v, want no diagnostic", diagnostics)
	}
}
