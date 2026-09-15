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
	"time"

	"github.com/ayeshLK/immulog/api"
)

func testDiskOptions() StoreOptions {
	return StoreOptions{
		DiskSafetyBytes: 10, DiskUserStopBytes: 20, DiskResumeBytes: 30,
		DiskSafetyInodes: 2, DiskUserStopInodes: 4, DiskResumeInodes: 6,
	}
}

func TestDiskPressureLedgerSharesDebitsAndHysteresis(t *testing.T) {
	options := testDiskOptions()
	ledger, err := newDiskPressureLedger(options, func() (diskObservation, error) {
		return diskObservation{availableBytes: 100, availableInodes: 20, inodesKnown: true, capturedAt: time.Now()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := ledger.reserve(diskReservationUser, 40, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.reserve(diskReservationUser, 50, 1); !errors.Is(err, api.ErrDiskPressure) {
		t.Fatalf("shared-debit refusal = %v, want ErrDiskPressure", err)
	}
	first.release()
	second, err := ledger.reserve(diskReservationUser, 50, 1)
	if err != nil {
		t.Fatal(err)
	}
	second.release()

	ledger.probe = func() (diskObservation, error) {
		return diskObservation{availableBytes: 19, availableInodes: 20, inodesKnown: true, capturedAt: time.Now()}, nil
	}
	if _, err := ledger.reserve(diskReservationUser, 0, 0); !errors.Is(err, api.ErrDiskPressure) {
		t.Fatalf("pressured user admission = %v, want ErrDiskPressure", err)
	}
	control, err := ledger.reserve(diskReservationControl, 5, 1)
	if err != nil {
		t.Fatalf("protected control admission = %v", err)
	}
	control.release()
	stats := ledger.snapshot()
	if !stats.Pressured || stats.AvailableBytes != 19 || stats.ReservedBytes != 0 || !stats.InodesKnown {
		t.Fatalf("ledger stats = %#v", stats)
	}
	if stats.HighWaterReservedBytes < 50 || stats.HighWaterReservedInodes < 2 {
		t.Fatalf("reservation high-water marks = %#v", stats)
	}

	ledger.probe = func() (diskObservation, error) {
		return diskObservation{availableBytes: 31, availableInodes: 7, inodesKnown: true, capturedAt: time.Now()}, nil
	}
	resumed, err := ledger.reserve(diskReservationUser, 1, 1)
	if err != nil {
		t.Fatalf("resumed user admission = %v", err)
	}
	resumed.release()
	if ledger.snapshot().Pressured {
		t.Fatal("fresh resume-floor observation did not clear pressure")
	}
}

// TestControlAppendsUseControlLedgerUnderUserStopPressure verifies that under
// disk pressure that fails user growth, already-authorized control commands
// (offset commits) still succeed because system partitions reserve from the
// protected control headroom (§7.9).
func TestControlAppendsUseControlLedgerUnderUserStopPressure(t *testing.T) {
	options := StoreOptions{
		DiskSafetyBytes: 100, DiskUserStopBytes: 1000, DiskResumeBytes: 1500,
		DiskSafetyInodes: 5, DiskUserStopInodes: 10, DiskResumeInodes: 15,
	}
	store, err := OpenWithOptions(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	abundant := func() (diskObservation, error) {
		return diskObservation{availableBytes: 100_000, availableInodes: 100, inodesKnown: true, capturedAt: time.Now()}, nil
	}
	store.disk.mu.Lock()
	store.disk.probe = abundant
	store.disk.mu.Unlock()

	descriptor, err := store.CreateTopic("disk-pressure-control", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(descriptor.ID, 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{[]byte("seed"), []byte("second")} {
		if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: payload}); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := store.OpenConsumer(context.Background(), "disk-pressure-control", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	result, err := consumer.Poll(context.Background(), api.FetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 2 {
		t.Fatalf("poll returned %d records, want 2", len(result.Records))
	}
	firstCommit := result.Records[0].Offset + 1
	secondCommit := result.NextOffset

	// Swap in a pressured observation: currentBytes 900 sits between the
	// safety (100) and user-stop (1000) floors. User growth is refused,
	// control growth is admitted.
	pressured := func() (diskObservation, error) {
		return diskObservation{availableBytes: 900, availableInodes: 15, inodesKnown: true, capturedAt: time.Now()}, nil
	}
	store.disk.mu.Lock()
	store.disk.probe = pressured
	store.disk.reservedBytes = 0
	store.disk.reservedInodes = 0
	store.disk.mu.Unlock()

	if _, err := partition.Append(context.Background(), api.AppendRequest{Topic: descriptor.ID, Partition: 0, Value: []byte("blocked")}); !errors.Is(err, api.ErrDiskPressure) {
		t.Fatalf("user append under pressure = %v, want ErrDiskPressure", err)
	}
	if err := consumer.Commit(context.Background(), firstCommit); err != nil {
		t.Fatalf("commit under user-stop pressure = %v, want success via control headroom", err)
	}
	stats := store.disk.snapshot()
	if !stats.Pressured {
		t.Fatalf("ledger not pressured after user-stop probe: %#v", stats)
	}

	// Tighten the probe below the safety floor. The control ledger must now
	// refuse the commit; before wiring, the offsets AppendBatch skipped disk
	// admission entirely and the commit would have succeeded regardless.
	starved := func() (diskObservation, error) {
		return diskObservation{availableBytes: 50, availableInodes: 4, inodesKnown: true, capturedAt: time.Now()}, nil
	}
	store.disk.mu.Lock()
	store.disk.probe = starved
	store.disk.reservedBytes = 0
	store.disk.reservedInodes = 0
	store.disk.mu.Unlock()

	if err := consumer.Commit(context.Background(), secondCommit); !errors.Is(err, api.ErrDiskPressure) {
		t.Fatalf("commit under safety-floor breach = %v, want ErrDiskPressure via control ledger", err)
	}
}

func TestDiskPressureLedgerRejectsProbeFailure(t *testing.T) {
	options := testDiskOptions()
	probeErr := errors.New("probe failed")
	ledger, err := newDiskPressureLedger(options, func() (diskObservation, error) {
		return diskObservation{availableBytes: 100, availableInodes: 20, inodesKnown: true, capturedAt: time.Now()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger.probe = func() (diskObservation, error) { return diskObservation{}, probeErr }
	if _, err := ledger.reserve(diskReservationUser, 1, 1); !errors.Is(err, api.ErrDiskPressure) || !errors.Is(err, probeErr) {
		t.Fatalf("probe failure = %v, want ErrDiskPressure and probe cause", err)
	}
	stats := ledger.snapshot()
	if !stats.Pressured || !stats.ProbeFailed {
		t.Fatalf("probe failure stats = %#v", stats)
	}
}
