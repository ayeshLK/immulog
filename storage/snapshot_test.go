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
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestProjectionSnapshotsPublishAndCorruptCachesFallBack(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("payments", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshots(); err != nil {
		t.Fatal(err)
	}
	if diagnostics := store.SnapshotDiagnostics(); diagnostics.Catalog != nil || diagnostics.Offsets != nil {
		t.Fatalf("snapshot publication diagnostics = %#v", diagnostics)
	}
	catalogSnapshot := filepath.Join(dir, clusterMetadataDir, snapshotPathName)
	if _, err := os.Stat(catalogSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(catalogSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(catalogSnapshot, data, 0o644); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics := store.SnapshotDiagnostics(); diagnostics.Catalog == nil {
		t.Fatal("corrupt catalog snapshot was not reported in diagnostics")
	}
	if _, err := store.DescribeTopic("payments"); err != nil {
		t.Fatalf("catalog replay with corrupt snapshot: %v", err)
	}
	if _, err := store.OpenPartition(api.ClusterMetadataTopicID, 0, PartitionOptions{}); !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("reserved partition error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
