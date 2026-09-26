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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

const (
	persistenceCrashHelperEnvironment    = "IMMULOG_PERSISTENCE_CRASH_HELPER"
	persistenceCrashDirectoryEnvironment = "IMMULOG_PERSISTENCE_CRASH_DIR"
	persistenceCrashScenarioEnvironment  = "IMMULOG_PERSISTENCE_CRASH_SCENARIO"
)

func TestPersistenceBoundaryCrashRecovery(t *testing.T) {
	if os.Getenv(persistenceCrashHelperEnvironment) == "1" {
		runPersistenceBoundaryCrashHelper()
		return
	}

	tests := []struct {
		name   string
		setup  func(*testing.T) string
		verify func(*testing.T, string)
	}{
		{name: "append-write", setup: setupDirectBaseline, verify: verifyAppendWriteCrash},
		{name: "append-sync", setup: setupDirectBaseline, verify: verifyAppendSyncCrash},
		{name: "segment-write", setup: setupSegmentRoll, verify: func(t *testing.T, dir string) { verifySegmentRollCrash(t, dir, 1) }},
		{name: "segment-sync", setup: setupSegmentRoll, verify: func(t *testing.T, dir string) { verifySegmentRollCrash(t, dir, 1) }},
		{name: "segment-rename", setup: setupSegmentRoll, verify: func(t *testing.T, dir string) { verifySegmentRollCrash(t, dir, 2) }},
		{name: "segment-directory-sync", setup: setupSegmentRoll, verify: func(t *testing.T, dir string) { verifySegmentRollCrash(t, dir, 2) }},
		{name: "bootstrap-write", setup: setupEmptyDirectory, verify: verifyOpenableStore},
		{name: "bootstrap-sync", setup: setupEmptyDirectory, verify: verifyOpenableStore},
		{name: "catalog-sync", setup: setupInitializedStore, verify: verifyCreatedTopic},
		{name: "topic-preparation-sync", setup: setupInitializedStore, verify: verifyNoCreatedTopic},
		{name: "snapshot-rename", setup: setupSnapshot, verify: verifyOpenableStore},
		{name: "snapshot-directory-sync", setup: setupSnapshot, verify: verifyOpenableStore},
		{name: "retention-catalog-sync", setup: setupRetention, verify: verifyRetentionRecovery},
		{name: "retention-cleanup-close", setup: setupRetention, verify: verifyRetentionRecovery},
		{name: "recovery-truncate", setup: setupIncompleteTail, verify: verifyRecoveredBaseline},
		{name: "recovery-sync", setup: setupIncompleteTail, verify: verifyRecoveredBaseline},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := test.setup(t)
			command := exec.Command(os.Args[0], "-test.run", "^TestPersistenceBoundaryCrashRecovery$")
			command.Env = append(os.Environ(),
				persistenceCrashHelperEnvironment+"=1",
				persistenceCrashDirectoryEnvironment+"="+dir,
				persistenceCrashScenarioEnvironment+"="+test.name,
			)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("crash helper exited successfully, output=%s", output)
			}
			exitErr, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatalf("crash helper error = %T %v, output=%s", err, err, output)
			}
			if !crashTerminationExpected(exitErr) {
				t.Fatalf("crash helper status = %#v, output=%s", exitErr.ProcessState, output)
			}
			test.verify(t, dir)
		})
	}
}

func runPersistenceBoundaryCrashHelper() {
	dir := os.Getenv(persistenceCrashDirectoryEnvironment)
	scenario := os.Getenv(persistenceCrashScenarioEnvironment)
	if dir == "" || scenario == "" {
		crashHelperFailure("persistence crash helper environment is incomplete")
	}
	switch scenario {
	case "append-write", "append-sync":
		runAppendCrashHelper(dir, scenario)
	case "segment-write", "segment-sync", "segment-rename", "segment-directory-sync":
		runSegmentRollCrashHelper(dir, scenario)
	case "bootstrap-write", "bootstrap-sync":
		runBootstrapCrashHelper(dir, scenario)
	case "catalog-sync", "topic-preparation-sync":
		runCatalogCrashHelper(dir, scenario)
	case "snapshot-rename", "snapshot-directory-sync":
		runSnapshotCrashHelper(dir, scenario)
	case "retention-catalog-sync", "retention-cleanup-close":
		runRetentionCrashHelper(dir, scenario)
	case "recovery-truncate", "recovery-sync":
		runRecoveryCrashHelper(dir, scenario)
	default:
		crashHelperFailure("unknown persistence crash scenario " + scenario)
	}
}

func runAppendCrashHelper(dir, scenario string) {
	store, err := Open(dir)
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open append crash directory: %v", err))
	}
	partition, err := store.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open append crash partition: %v", err))
	}
	active := partition.segments[len(partition.segments)-1]
	operation := filesystemWriteAt
	if scenario == "append-sync" {
		operation = filesystemSync
	}
	installCrashAfterFilesystem(operation, active.path, true, 0)
	if _, err := partition.AppendBatch(testBatch(testTopic(), 0, 1, scenario)); err != nil {
		crashHelperFailure(fmt.Sprintf("append crash boundary: %v", err))
	}
	crashHelperFailure("append crash boundary was not reached")
}

func runSegmentRollCrashHelper(dir, scenario string) {
	store, err := Open(dir)
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open segment crash directory: %v", err))
	}
	partition, err := store.OpenPartition(testTopic(), 0, segmentRollCrashOptions())
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open segment crash partition: %v", err))
	}
	operation := filesystemWrite
	path := ".segment-"
	exact := false
	switch scenario {
	case "segment-sync":
		operation = filesystemSync
	case "segment-rename":
		operation = filesystemRename
	case "segment-directory-sync":
		operation = filesystemSync
		path = partition.dir
		exact = true
	}
	installCrashAfterFilesystem(operation, path, exact, 0)
	if _, err := partition.AppendBatch(testBatch(testTopic(), 0, 1, "roll-crash")); err != nil {
		crashHelperFailure(fmt.Sprintf("segment roll crash boundary: %v", err))
	}
	crashHelperFailure("segment roll crash boundary was not reached")
}

func runBootstrapCrashHelper(dir, scenario string) {
	path := filepath.Join(dir, clusterMetadataDir, "00000000000000000000.log")
	operation := filesystemWriteAt
	if scenario == "bootstrap-sync" {
		operation = filesystemSync
	}
	installCrashAfterFilesystem(operation, path, true, 0)
	store, err := Open(dir)
	if err != nil {
		crashHelperFailure(fmt.Sprintf("bootstrap crash boundary: %v", err))
	}
	_ = store.Close()
	crashHelperFailure("bootstrap crash boundary was not reached")
}

func runCatalogCrashHelper(dir, scenario string) {
	store, err := Open(dir)
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open catalog crash directory: %v", err))
	}
	if scenario == "catalog-sync" {
		path := store.catalog.segments[len(store.catalog.segments)-1].path
		installCrashAfterFilesystem(filesystemSync, path, true, 0)
	} else {
		installCrashAfterFilesystem(filesystemSync, topicPreparationMarker, false, 0)
	}
	if _, err := store.CreateTopic("boundary-topic", 1, PartitionOptions{}); err != nil {
		crashHelperFailure(fmt.Sprintf("catalog crash boundary: %v", err))
	}
	crashHelperFailure("catalog crash boundary was not reached")
}

func runSnapshotCrashHelper(dir, scenario string) {
	store, err := Open(dir)
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open snapshot crash directory: %v", err))
	}
	if scenario == "snapshot-rename" {
		installCrashAfterFilesystem(filesystemRename, ".projection-snapshot-", false, 0)
	} else {
		installCrashAfterFilesystem(filesystemSync, store.catalog.dir, true, 0)
	}
	if err := store.SaveSnapshots(); err != nil {
		crashHelperFailure(fmt.Sprintf("snapshot crash boundary: %v", err))
	}
	crashHelperFailure("snapshot crash boundary was not reached")
}

func runRetentionCrashHelper(dir, scenario string) {
	store, err := Open(dir)
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open retention crash directory: %v", err))
	}
	if scenario == "retention-catalog-sync" {
		path := store.catalog.segments[len(store.catalog.segments)-1].path
		installCrashAfterFilesystem(filesystemSync, path, true, 0)
	} else {
		partition, err := store.OpenTopic("fault-retained")
		if err != nil || len(partition) != 1 {
			crashHelperFailure(fmt.Sprintf("open retention crash partition: %v", err))
		}
		// Recovery released the sealed segment's descriptor to the store's
		// bounded cache immediately, so read it back once to give retention's
		// cleanup close a real, cached handle to close.
		if _, err := partition[0].Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 1}); err != nil {
			crashHelperFailure(fmt.Sprintf("prime retention crash segment cache: %v", err))
		}
		installCrashAfterFilesystem(filesystemClose, partition[0].segments[0].path, true, 0)
	}
	if err := store.RunRetention(context.Background()); err != nil {
		crashHelperFailure(fmt.Sprintf("retention crash boundary: %v", err))
	}
	crashHelperFailure("retention crash boundary was not reached")
}

func runRecoveryCrashHelper(dir, scenario string) {
	store, err := Open(dir)
	if err != nil {
		crashHelperFailure(fmt.Sprintf("open recovery crash directory: %v", err))
	}
	path := crashUserLogPath(dir)
	operation := filesystemTruncate
	if scenario == "recovery-sync" {
		operation = filesystemSync
	}
	installCrashAfterFilesystem(operation, path, true, 0)
	if _, err := store.OpenPartition(testTopic(), 0, PartitionOptions{}); err != nil {
		crashHelperFailure(fmt.Sprintf("recovery crash boundary: %v", err))
	}
	crashHelperFailure("recovery crash boundary was not reached")
}

func crashHelperFailure(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}

func installCrashAfterFilesystem(operation filesystemOperation, path string, exact bool, skip int) {
	if exact {
		path = canonicalFaultPath(path)
	}
	base := operatingSystemFileSystem()
	var mu sync.Mutex
	crash := func(actual string) {
		mu.Lock()
		actualPath := actual
		if exact {
			actualPath = canonicalFaultPath(actual)
		}
		matches := path == "" || exact && actualPath == path || !exact && strings.Contains(actual, path)
		if !matches || skip > 0 {
			if matches {
				skip--
			}
			mu.Unlock()
			return
		}
		mu.Unlock()
		if err := terminateCrashHelper(); err != nil {
			panic(err)
		}
	}
	faulted := base
	switch operation {
	case filesystemWrite:
		faulted.write = func(file *os.File, data []byte) (int, error) {
			count, err := base.write(file, data)
			if err == nil {
				crash(file.Name())
			}
			return count, err
		}
	case filesystemWriteAt:
		faulted.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
			count, err := base.writeAt(file, data, offset)
			if err == nil {
				crash(file.Name())
			}
			return count, err
		}
	case filesystemSync:
		faulted.sync = func(file *os.File) error {
			err := base.sync(file)
			if err == nil {
				crash(file.Name())
			}
			return err
		}
	case filesystemClose:
		faulted.close = func(file *os.File) error {
			err := base.close(file)
			if err == nil {
				crash(file.Name())
			}
			return err
		}
	case filesystemRename:
		faulted.rename = func(oldPath, newPath string) error {
			err := base.rename(oldPath, newPath)
			if err == nil {
				crash(oldPath + " -> " + newPath)
			}
			return err
		}
	case filesystemTruncate:
		faulted.truncate = func(file *os.File, size int64) error {
			err := base.truncate(file, size)
			if err == nil {
				crash(file.Name())
			}
			return err
		}
	default:
		panic("unsupported persistence crash operation " + string(operation))
	}
	fileSystemMu.Lock()
	fileSystem = faulted
	fileSystemMu.Unlock()
}

func setupEmptyDirectory(t *testing.T) string {
	return t.TempDir()
}

func setupInitializedStore(t *testing.T) string {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func setupDirectBaseline(t *testing.T) string {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(testTopic(), 0, 0, "baseline")); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func setupSegmentRoll(t *testing.T) string {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(testTopic(), 0, segmentRollCrashOptions())
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(testTopic(), 0, 0, strings.Repeat("a", 120))); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func setupSnapshot(t *testing.T) string {
	dir := setupInitializedStore(t)
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshots(); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func setupRetention(t *testing.T) string {
	store, _, _ := openRetentionFaultFixture(t)
	dir := store.RootPath()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func setupIncompleteTail(t *testing.T) string {
	dir := setupDirectBaseline(t)
	path := crashUserLogPath(dir)
	encoded, err := EncodeBatch(testBatch(testTopic(), 0, 1, "incomplete"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(encoded[:len(encoded)-1]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func verifyAppendWriteCrash(t *testing.T, dir string) {
	verifyCrashedRecords(t, dir, []string{"baseline", "append-write"})
}

func verifyAppendSyncCrash(t *testing.T, dir string) {
	verifyCrashedRecords(t, dir, []string{"baseline", "append-sync"})
}

func verifyOpenableStore(t *testing.T, dir string) {
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func verifyCreatedTopic(t *testing.T, dir string) {
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topics, err := store.ListTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0].Name != "boundary-topic" {
		t.Fatalf("topics after catalog crash = %#v, want boundary-topic", topics)
	}
	partitions, err := store.OpenTopic("boundary-topic")
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 1 {
		t.Fatalf("created topic partitions = %d, want 1", len(partitions))
	}
	if end, err := partitions[0].EndOffset(); err != nil || end != 0 {
		t.Fatalf("created topic end = (%d, %v), want (0, nil)", end, err)
	}
}

func verifyNoCreatedTopic(t *testing.T, dir string) {
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topics, err := store.ListTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 0 {
		t.Fatalf("topics after preparation crash = %#v, want none", topics)
	}
}

func verifySegmentRollCrash(t *testing.T, dir string, wantSegments int) {
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	partition, err := store.OpenPartition(testTopic(), 0, segmentRollCrashOptions())
	if err != nil {
		t.Fatal(err)
	}
	segments, err := discoverSegments(partition.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != wantSegments {
		t.Fatalf("segments after roll crash = %d, want %d", len(segments), wantSegments)
	}
	records, err := partition.Read(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Offset != 0 || string(records[0].Value) != strings.Repeat("a", 120) {
		t.Fatalf("records after roll crash = %#v, want baseline", records)
	}
}

func verifyRetentionRecovery(t *testing.T, dir string) {
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	partitions, err := store.OpenTopic("fault-retained")
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 1 {
		t.Fatalf("retained partitions = %d, want 1", len(partitions))
	}
	if end, err := partitions[0].EndOffset(); err != nil || end != 4 {
		t.Fatalf("retained end = (%d, %v), want (4, nil)", end, err)
	}
	if _, err := partitions[0].Fetch(context.Background(), 0, api.FetchOptions{}); !errors.Is(err, api.ErrOffsetOutOfRange) {
		t.Fatalf("expired fetch error = %v, want ErrOffsetOutOfRange", err)
	}
	retiredPath := filepath.Join(partitions[0].dir, "00000000000000000000.log")
	if _, err := os.Stat(retiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired path after recovery = %v, want not exist", err)
	}
}

func verifyRecoveredBaseline(t *testing.T, dir string) {
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	partition, err := store.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	records, err := partition.Read(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Offset != 0 || !bytes.Equal(records[0].Value, []byte("baseline")) {
		t.Fatalf("records after recovery crash = %#v, want baseline", records)
	}
	data, err := os.ReadFile(crashUserLogPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeBatch(testBatch(testTopic(), 0, 0, "baseline"))
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int(SegmentHeaderBytes) + len(encoded)
	if len(data) != wantSize {
		t.Fatalf("recovered log size = %d, want %d", len(data), wantSize)
	}
}

func TestRecoveryTruncateFailureRefusesRecovery(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir := setupIncompleteTail(t)
	path := crashUserLogPath(dir)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	plan.failOnceExact(filesystemTruncate, path, errors.New("injected recovery truncate failure"))
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenPartition(testTopic(), 0, PartitionOptions{}); err == nil {
		_ = store.Close()
		t.Fatal("recovery unexpectedly succeeded")
	}
	if !plan.triggered() {
		_ = store.Close()
		t.Fatal("recovery truncate fault was not triggered")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("failed recovery truncate changed bytes: before=%x after=%x", before, after)
	}
}

func TestRecoverySyncFailureRequiresRetry(t *testing.T) {
	plan := &filesystemFaultPlan{}
	installFilesystemFault(t, plan)
	dir := setupIncompleteTail(t)
	path := crashUserLogPath(dir)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := EncodeBatch(testBatch(testTopic(), 0, 1, "incomplete"))
	if err != nil {
		t.Fatal(err)
	}
	want := before[:len(before)-len(partial)+1]
	plan.failOnceExact(filesystemSync, path, errors.New("injected recovered segment sync failure"))
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenPartition(testTopic(), 0, PartitionOptions{}); err == nil {
		_ = store.Close()
		t.Fatal("recovery unexpectedly succeeded")
	}
	if !plan.triggered() {
		_ = store.Close()
		t.Fatal("recovery sync fault was not triggered")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, want) {
		t.Fatalf("recovery sync failure left bytes = %x, want %x", after, want)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := reopened.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		_ = reopened.Close()
		t.Fatal(err)
	}
	records, err := partition.Read(0, 2)
	if err != nil || len(records) != 1 || !bytes.Equal(records[0].Value, []byte("baseline")) {
		_ = reopened.Close()
		t.Fatalf("retry recovery records = %#v, %v", records, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func crashUserLogPath(dir string) string {
	return filepath.Join(dir, "topics", testTopic().String(), "0", "00000000000000000000.log")
}

func segmentRollCrashOptions() PartitionOptions {
	first := testBatch(testTopic(), 0, 0, strings.Repeat("a", 120))
	encoded, _ := EncodeBatch(first)
	return PartitionOptions{
		SegmentBytes: uint64(SegmentHeaderBytes) + uint64(len(encoded)) + 1,
		BatchBytes:   4096,
		BatchRecords: 8,
		RecordBytes:  1024,
	}
}
