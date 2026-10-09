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
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ayeshLK/immulog/api"
)

// TestColdCopyRestorePreservesAuthoritativeState exercises the documented
// backup procedure using only public Store and Partition lifecycle behavior.
// The copy is made after Close, so LOCK and every authoritative file are
// copied together rather than selected segments or projection caches.
func TestColdCopyRestorePreservesAuthoritativeState(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	restoreDir := filepath.Join(root, "restore")
	store, err := Open(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	orders, err := store.CreateTopic("orders", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := store.CreateTopic("retained", 1, PartitionOptions{
		RecordBytes: 512, BatchBytes: 600, BatchRecords: 1, SegmentBytes: 700,
		RetentionSizeEnabled: true, RetentionBytes: 0, RetentionCheck: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ordersPartition, err := store.OpenPartition(orders.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	retainedPartition, err := store.OpenPartition(retained.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"zero", "one", "two"} {
		if _, err := ordersPartition.Append(context.Background(), api.AppendRequest{Topic: orders.ID, Partition: 0, Value: []byte(value)}); err != nil {
			t.Fatal(err)
		}
	}
	for offset := 0; offset < 4; offset++ {
		if _, err := retainedPartition.Append(context.Background(), api.AppendRequest{Topic: retained.ID, Partition: 0, Value: []byte("expired")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RunRetention(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := store.DescribeTopic(retained.Name); err != nil {
		t.Fatal(err)
	} else if got.Partitions[0].RetainedL != 4 {
		t.Fatalf("retained boundary before backup = %d, want 4", got.Partitions[0].RetainedL)
	}
	if _, err := retainedPartition.Append(context.Background(), api.AppendRequest{Topic: retained.ID, Partition: 0, Value: []byte("kept")}); err != nil {
		t.Fatal(err)
	}

	consumerOptions := api.ConsumerOptions{Start: api.GroupStartEarliest, Fetch: api.FetchOptions{MaxRecords: 2, MaxBytes: 1024}}
	consumer, err := store.OpenConsumer(context.Background(), "orders-reader", orders.ID, 0, consumerOptions)
	if err != nil {
		t.Fatal(err)
	}
	polled, err := consumer.Poll(context.Background(), api.FetchOptions{})
	if err != nil || len(polled.Records) != 2 || polled.NextOffset != 2 {
		t.Fatalf("initial consumer poll = %#v, %v", polled, err)
	}
	if err := consumer.Commit(context.Background(), polled.NextOffset); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshots(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if err := copyDirectory(sourceDir, restoreDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "LOCK")); err != nil {
		t.Fatalf("restored LOCK = %v", err)
	}
	restored, err := Open(restoreDir)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()

	if diagnostics := restored.SnapshotDiagnostics(); diagnostics.Catalog != nil || diagnostics.Offsets != nil {
		t.Fatalf("restored snapshot diagnostics = %#v", diagnostics)
	}
	restoredOrders, err := restored.OpenTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	result, err := restoredOrders[0].Fetch(context.Background(), 0, api.FetchOptions{MaxRecords: 10, MaxBytes: 4096})
	if err != nil || len(result.Records) != 3 {
		t.Fatalf("restored records = %#v, %v", result, err)
	}
	if string(result.Records[0].Value) != "zero" || string(result.Records[2].Value) != "two" {
		t.Fatalf("restored record values = %#v", result.Records)
	}
	restoredRetained, err := restored.OpenTopic("retained")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restoredRetained[0].Fetch(context.Background(), 0, api.FetchOptions{}); !errors.Is(err, api.ErrOffsetOutOfRange) {
		t.Fatalf("restored expired fetch = %v, want ErrOffsetOutOfRange", err)
	}
	kept, err := restoredRetained[0].Fetch(context.Background(), 4, api.FetchOptions{})
	if err != nil || len(kept.Records) != 1 || string(kept.Records[0].Value) != "kept" {
		t.Fatalf("restored retained record = %#v, %v", kept, err)
	}
	if described, err := restored.DescribeTopic("retained"); err != nil {
		t.Fatal(err)
	} else if described.Partitions[0].RetainedL != 4 {
		t.Fatalf("restored retained boundary = %d, want 4", described.Partitions[0].RetainedL)
	}

	resumed, err := restored.OpenConsumer(context.Background(), "orders-reader", orders.ID, 0, consumerOptions)
	if err != nil {
		t.Fatal(err)
	}
	resumedResult, err := resumed.Poll(context.Background(), api.FetchOptions{})
	if err != nil || len(resumedResult.Records) != 1 || resumedResult.Records[0].Offset != 2 {
		t.Fatalf("resumed consumer = %#v, %v", resumedResult, err)
	}
	replacement, err := restored.OpenConsumer(context.Background(), "orders-reader", orders.ID, 0, consumerOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.Poll(context.Background(), api.FetchOptions{}); !errors.Is(err, api.ErrAssignmentLost) {
		t.Fatalf("fenced restored consumer = %v, want ErrAssignmentLost", err)
	}
	replayed, err := replacement.Poll(context.Background(), api.FetchOptions{})
	if err != nil || len(replayed.Records) != 1 || replayed.Records[0].Offset != 2 {
		t.Fatalf("replayed uncommitted record = %#v, %v", replayed, err)
	}
	if err := replacement.Commit(context.Background(), replayed.NextOffset); err != nil {
		t.Fatal(err)
	}
}

func copyDirectory(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return copyFile(path, target, info.Mode())
	})
}

func copyFile(source, destination string, mode fs.FileMode) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	return os.WriteFile(destination, data, mode.Perm())
}
