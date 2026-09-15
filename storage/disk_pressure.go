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
	"fmt"
	"sync"
	"time"

	"github.com/ayeshLK/immulog/api"
)

type diskObservation struct {
	availableBytes  uint64
	availableInodes uint64
	inodesKnown     bool
	capturedAt      time.Time
}

type diskReservationKind uint8

const (
	diskReservationUser diskReservationKind = iota
	diskReservationControl
)

// DiskPressureStats is a bounded observation of the Store-wide filesystem
// admission ledger. Values are advisory: external writers and filesystem
// quotas can still cause a later write to fail.
type DiskPressureStats struct {
	AvailableBytes          uint64
	AvailableInodes         uint64
	InodesKnown             bool
	ReservedBytes           uint64
	ReservedInodes          uint64
	HighWaterReservedBytes  uint64
	HighWaterReservedInodes uint64
	SafetyBytes             uint64
	UserStopBytes           uint64
	ResumeBytes             uint64
	SafetyInodes            uint64
	UserStopInodes          uint64
	ResumeInodes            uint64
	Pressured               bool
	ProbeFailed             bool
	SampleAge               time.Duration
}

type diskPressureLedger struct {
	mu sync.Mutex

	probe func() (diskObservation, error)

	safetyBytes, userStopBytes, resumeBytes         uint64
	safetyInodes, userStopInodes, resumeInodes      uint64
	reservedBytes, reservedInodes                   uint64
	highWaterReservedBytes, highWaterReservedInodes uint64
	observation                                     diskObservation
	hasObservation, pressured, probeFailed          bool
	nextProbe, appliedProbe                         uint64
}

type diskReservation struct {
	ledger *diskPressureLedger
	bytes  uint64
	inodes uint64
	once   sync.Once
}

func (reservation *diskReservation) release() {
	if reservation == nil || reservation.ledger == nil {
		return
	}
	reservation.once.Do(func() {
		ledger := reservation.ledger
		ledger.mu.Lock()
		if reservation.bytes <= ledger.reservedBytes {
			ledger.reservedBytes -= reservation.bytes
		} else {
			ledger.reservedBytes = 0
		}
		if reservation.inodes <= ledger.reservedInodes {
			ledger.reservedInodes -= reservation.inodes
		} else {
			ledger.reservedInodes = 0
		}
		ledger.mu.Unlock()
	})
}

func newDiskPressureLedger(options StoreOptions, probe func() (diskObservation, error)) (*diskPressureLedger, error) {
	if probe == nil {
		return nil, errors.Join(api.ErrDiskPressure, errors.New("filesystem capacity probe is unavailable"))
	}
	ledger := &diskPressureLedger{
		probe:       probe,
		safetyBytes: options.DiskSafetyBytes, userStopBytes: options.DiskUserStopBytes, resumeBytes: options.DiskResumeBytes,
		safetyInodes: options.DiskSafetyInodes, userStopInodes: options.DiskUserStopInodes, resumeInodes: options.DiskResumeInodes,
	}
	if _, err := ledger.reserve(diskReservationControl, 0, 0); err != nil {
		return nil, err
	}
	return ledger, nil
}

func (ledger *diskPressureLedger) reserve(kind diskReservationKind, bytes, inodes uint64) (*diskReservation, error) {
	ledger.mu.Lock()
	ledger.nextProbe++
	probeID := ledger.nextProbe
	probe := ledger.probe
	ledger.mu.Unlock()

	observation, probeErr := probe()
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if probeErr != nil {
		if probeID >= ledger.appliedProbe {
			ledger.appliedProbe = probeID
			ledger.probeFailed = true
			ledger.pressured = true
		}
		return nil, errors.Join(api.ErrDiskPressure, fmt.Errorf("filesystem capacity probe: %w", probeErr))
	}
	if observation.capturedAt.IsZero() {
		observation.capturedAt = time.Now()
	}
	if probeID >= ledger.appliedProbe {
		ledger.appliedProbe = probeID
		ledger.observation = observation
		ledger.hasObservation = true
		ledger.probeFailed = false
	}
	if !ledger.hasObservation {
		ledger.pressured = true
		return nil, errors.Join(api.ErrDiskPressure, errors.New("filesystem capacity observation is unavailable"))
	}

	currentBytes := remainingDiskCapacity(ledger.observation.availableBytes, ledger.reservedBytes)
	currentInodes := remainingDiskCapacity(ledger.observation.availableInodes, ledger.reservedInodes)
	if ledger.observation.inodesKnown && (currentInodes < ledger.userStopInodes || currentBytes < ledger.userStopBytes) {
		ledger.pressured = true
	} else if !ledger.observation.inodesKnown && currentBytes < ledger.userStopBytes {
		ledger.pressured = true
	}
	if ledger.pressured && ledger.canResume(currentBytes, currentInodes) {
		ledger.pressured = false
	}

	afterBytes := remainingDiskCapacity(currentBytes, bytes)
	afterInodes := remainingDiskCapacity(currentInodes, inodes)
	if kind == diskReservationUser {
		if ledger.pressured || afterBytes < ledger.userStopBytes || ledger.observation.inodesKnown && afterInodes < ledger.userStopInodes {
			return nil, ledger.refusal(bytes, inodes, "user growth")
		}
	} else if afterBytes < ledger.safetyBytes || ledger.observation.inodesKnown && afterInodes < ledger.safetyInodes {
		return nil, ledger.refusal(bytes, inodes, "control growth")
	}
	if bytes > ^uint64(0)-ledger.reservedBytes || inodes > ^uint64(0)-ledger.reservedInodes {
		return nil, errors.Join(api.ErrDiskPressure, errors.New("disk reservation arithmetic overflow"))
	}
	ledger.reservedBytes += bytes
	ledger.reservedInodes += inodes
	if ledger.reservedBytes > ledger.highWaterReservedBytes {
		ledger.highWaterReservedBytes = ledger.reservedBytes
	}
	if ledger.reservedInodes > ledger.highWaterReservedInodes {
		ledger.highWaterReservedInodes = ledger.reservedInodes
	}
	return &diskReservation{ledger: ledger, bytes: bytes, inodes: inodes}, nil
}

func (ledger *diskPressureLedger) canResume(bytes, inodes uint64) bool {
	if bytes < ledger.resumeBytes {
		return false
	}
	return !ledger.observation.inodesKnown || inodes >= ledger.resumeInodes
}

func (ledger *diskPressureLedger) refusal(bytes, inodes uint64, kind string) error {
	if ledger.observation.inodesKnown {
		return errors.Join(api.ErrDiskPressure, fmt.Errorf("%s needs %d bytes and %d inodes; available after reservations is %d bytes and %d inodes", kind, bytes, inodes, remainingDiskCapacity(ledger.observation.availableBytes, ledger.reservedBytes), remainingDiskCapacity(ledger.observation.availableInodes, ledger.reservedInodes)))
	}
	return errors.Join(api.ErrDiskPressure, fmt.Errorf("%s needs %d bytes; available after reservations is %d bytes (inode capacity is unavailable)", kind, bytes, remainingDiskCapacity(ledger.observation.availableBytes, ledger.reservedBytes)))
}

func (ledger *diskPressureLedger) snapshot() DiskPressureStats {
	if ledger == nil {
		return DiskPressureStats{}
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stats := DiskPressureStats{
		AvailableBytes: ledger.observation.availableBytes, AvailableInodes: ledger.observation.availableInodes, InodesKnown: ledger.observation.inodesKnown,
		ReservedBytes: ledger.reservedBytes, ReservedInodes: ledger.reservedInodes,
		HighWaterReservedBytes: ledger.highWaterReservedBytes, HighWaterReservedInodes: ledger.highWaterReservedInodes,
		SafetyBytes: ledger.safetyBytes, UserStopBytes: ledger.userStopBytes, ResumeBytes: ledger.resumeBytes,
		SafetyInodes: ledger.safetyInodes, UserStopInodes: ledger.userStopInodes, ResumeInodes: ledger.resumeInodes,
		Pressured: ledger.pressured, ProbeFailed: ledger.probeFailed,
	}
	if ledger.hasObservation {
		stats.SampleAge = time.Since(ledger.observation.capturedAt)
	}
	return stats
}

func remainingDiskCapacity(available, debit uint64) uint64 {
	if debit >= available {
		return 0
	}
	return available - debit
}

func estimateDiskBatchGrowth(encodedBytes uint64) (uint64, uint64, error) {
	overhead := uint64(SegmentHeaderBytes) + 2*(uint64(IndexHeaderBytes)+uint64(OffsetIndexEntryBytes)+uint64(TimeIndexEntryBytes))
	if encodedBytes > ^uint64(0)-overhead {
		return 0, 0, errors.Join(api.ErrDiskPressure, errors.New("disk growth estimate overflows"))
	}
	// A roll can create a segment and two indexes while an index replacement
	// temporarily owns an additional inode.
	return encodedBytes + overhead, 4, nil
}

func (store *Store) reserveDiskUser(bytes, inodes uint64) (*diskReservation, error) {
	if store == nil || store.disk == nil {
		return nil, nil
	}
	return store.disk.reserve(diskReservationUser, bytes, inodes)
}

func (store *Store) reserveDiskControl(bytes, inodes uint64) (*diskReservation, error) {
	if store == nil || store.disk == nil {
		return nil, nil
	}
	return store.disk.reserve(diskReservationControl, bytes, inodes)
}

// isSystemPartition reports whether this partition owns one of the two
// reserved system logs. Callers use it to route disk pressure to the control
// class and to skip the store-wide closing gate for already-reserved commands.
func (p *Partition) isSystemPartition() bool {
	if p == nil {
		return false
	}
	return p.topic == api.ClusterMetadataTopicID || p.topic == api.ConsumerOffsetsTopicID
}

// reserveDisk selects the disk-pressure class for this partition. System
// partitions draw from the protected control headroom so that fencing,
// commits, and retention boundary events can complete after user growth has
// been stopped (§7.9).
func (p *Partition) reserveDisk(bytes, inodes uint64) (*diskReservation, error) {
	if p == nil || p.store == nil {
		return nil, nil
	}
	if p.isSystemPartition() {
		return p.store.reserveDiskControl(bytes, inodes)
	}
	return p.store.reserveDiskUser(bytes, inodes)
}
