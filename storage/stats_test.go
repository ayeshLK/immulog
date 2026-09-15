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
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestStoreAndPartitionStatsAreBoundedSnapshots(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("stats", 1, PartitionOptions{TailSlots: 2, TailBytes: 4096})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := partitions[0].Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("stats")}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	partitionStats := partitions[0].Stats()
	if partitionStats.Topic != descriptor.ID || partitionStats.Partition != 0 || partitionStats.LogStartOffset != 0 || partitionStats.DurableEnd != 1 || partitionStats.RetainedSegments != 1 {
		_ = store.Close()
		t.Fatalf("partition stats = %#v", partitionStats)
	}
	if partitionStats.LogicalLogBytes < uint64(SegmentHeaderBytes) || partitionStats.TailEntries != 1 || partitionStats.TailBytes == 0 || partitionStats.AppendAcked != 1 || partitionStats.AppendKnownUnwritten != 0 || partitionStats.AppendUnknown != 0 {
		_ = store.Close()
		t.Fatalf("partition resource stats = %#v", partitionStats)
	}
	storeStats, err := store.Stats()
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if storeStats.OpenPartitions != 1 || storeStats.TailBytesUsed != partitionStats.TailBytes || storeStats.TailBytesLimit == 0 || storeStats.Catalog.DurableEnd < 2 {
		_ = store.Close()
		t.Fatalf("store stats = %#v", storeStats)
	}
	if storeStats.DiskPressure.SafetyBytes == 0 || storeStats.DiskPressure.UserStopBytes == 0 || storeStats.DiskPressure.ResumeBytes == 0 || storeStats.DiskPressure.SampleAge < 0 {
		_ = store.Close()
		t.Fatalf("disk pressure stats = %#v", storeStats.DiskPressure)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closedStats, err := store.Stats()
	if !errors.Is(err, api.ErrClosed) || !closedStats.Closed || closedStats.Closing {
		t.Fatalf("stats after Close = %#v, %v; want closed status and ErrClosed", closedStats, err)
	}
}
