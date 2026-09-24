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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ayeshLK/immulog/api"
)

const (
	snapshotMagic                 = "IELSNP00"
	snapshotHeaderBytes           = 128
	snapshotTrailerBytes          = 32
	maxSnapshotFileBytes          = uint64(256 * 1024 * 1024)
	maxSnapshotPayload            = maxSnapshotFileBytes - snapshotHeaderBytes - snapshotTrailerBytes
	maxSnapshotEntries            = uint64(1_048_576)
	snapshotKindCatalog           = uint16(1)
	snapshotKindOffsets           = uint16(2)
	snapshotSchemaVersion         = uint16(1)
	snapshotValidationBufferBytes = 64 * 1024
)

const snapshotPathName = "projection.snapshot"

type snapshotHeader struct {
	kind         uint16
	storeID      StoreID
	topic        api.TopicID
	partition    uint32
	nextOffset   uint64
	payloadBytes uint64
	prefixDigest [32]byte
	entryCount   uint64
}

func encodeSnapshotHeader(header snapshotHeader) []byte {
	data := make([]byte, snapshotHeaderBytes)
	copy(data[:8], snapshotMagic)
	binary.LittleEndian.PutUint16(data[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(data[10:12], snapshotHeaderBytes)
	binary.LittleEndian.PutUint16(data[12:14], header.kind)
	binary.LittleEndian.PutUint16(data[14:16], snapshotSchemaVersion)
	copy(data[24:40], header.storeID[:])
	copy(data[40:56], header.topic[:])
	binary.LittleEndian.PutUint32(data[56:60], header.partition)
	binary.LittleEndian.PutUint64(data[64:72], header.nextOffset)
	binary.LittleEndian.PutUint64(data[72:80], header.payloadBytes)
	copy(data[80:112], header.prefixDigest[:])
	binary.LittleEndian.PutUint64(data[112:120], header.entryCount)
	binary.LittleEndian.PutUint32(data[124:128], CRC32C(data[:124]))
	return data
}

func decodeSnapshotHeader(data []byte) (snapshotHeader, error) {
	if len(data) < snapshotHeaderBytes || string(data[:8]) != snapshotMagic || binary.LittleEndian.Uint16(data[10:12]) != snapshotHeaderBytes {
		return snapshotHeader{}, errors.New("snapshot header is invalid")
	}
	if binary.LittleEndian.Uint32(data[124:128]) != CRC32C(data[:124]) {
		return snapshotHeader{}, errors.New("snapshot header checksum mismatch")
	}
	if binary.LittleEndian.Uint16(data[8:10]) != FormatVersion || binary.LittleEndian.Uint16(data[14:16]) != snapshotSchemaVersion {
		return snapshotHeader{}, errors.New("unsupported snapshot version")
	}
	if binary.LittleEndian.Uint32(data[16:20]) != 0 || binary.LittleEndian.Uint32(data[20:24]) != 0 || binary.LittleEndian.Uint32(data[60:64]) != 0 || binary.LittleEndian.Uint32(data[120:124]) != 0 {
		return snapshotHeader{}, errors.New("snapshot reserved fields are nonzero")
	}
	header := snapshotHeader{kind: binary.LittleEndian.Uint16(data[12:14]), partition: binary.LittleEndian.Uint32(data[56:60]), nextOffset: binary.LittleEndian.Uint64(data[64:72]), payloadBytes: binary.LittleEndian.Uint64(data[72:80]), entryCount: binary.LittleEndian.Uint64(data[112:120])}
	copy(header.storeID[:], data[24:40])
	copy(header.topic[:], data[40:56])
	copy(header.prefixDigest[:], data[80:112])
	if (header.kind != snapshotKindCatalog && header.kind != snapshotKindOffsets) || header.storeID == (StoreID{}) || header.topic.IsZero() || header.payloadBytes > maxSnapshotPayload || header.entryCount > maxSnapshotEntries {
		return snapshotHeader{}, errors.New("snapshot identity or bounds are invalid")
	}
	return header, nil
}

// prefixDigestState caches the running projection-prefix hash of one reserved
// system log. The hash covers the digest prelude followed by every batch byte
// below covered, so a durable append extends it in place instead of forcing
// the next snapshot to re-read the whole log. Every writer and reader of this
// cache runs under the store mutex: system-log appends are only issued while
// it is held, and so are snapshot builds.
type prefixDigestState struct {
	hash    hash.Cloner
	storeID StoreID
	covered uint64
}

// newPrefixDigest starts a projection-prefix hash over the log identity. The
// prelude binds the digest to one store, topic, and partition so a snapshot
// cannot be replayed against a different log.
func newPrefixDigest(storeID StoreID, topic api.TopicID, partition uint32) hash.Cloner {
	running := sha256.New().(hash.Cloner)
	running.Write([]byte("IEL-PROJECTION-PREFIX-V1\x00"))
	running.Write(storeID[:])
	running.Write(topic[:])
	var scalar [4]byte
	binary.LittleEndian.PutUint32(scalar[:], partition)
	running.Write(scalar[:])
	return running
}

// finishPrefixDigest seals a copy of the running hash at nextOffset. The
// running state is left untouched so later appends can keep extending it.
func finishPrefixDigest(running hash.Cloner, nextOffset uint64) ([32]byte, error) {
	var result [32]byte
	sealed, err := running.Clone()
	if err != nil {
		return result, err
	}
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], nextOffset)
	sealed.Write(scalar[:])
	copy(result[:], sealed.Sum(nil))
	return result, nil
}

// extendPrefixDigestLocked advances the cached prefix hash over one durable
// batch. The caller owns p.mu and has already advanced p.logEnd. A batch that
// does not continue the cached prefix drops the cache; the next digest request
// rebuilds it from the authoritative segments.
func (p *Partition) extendPrefixDigestLocked(base uint64, encoded []byte) {
	cached := p.prefixDigest
	if cached == nil {
		return
	}
	if cached.covered != base {
		p.prefixDigest = nil
		return
	}
	cached.hash.Write(encoded)
	cached.covered = p.logEnd
}

// projectionPrefixDigest returns the digest of the durable log prefix that
// ends at nextOffset. Reserved system logs keep the running hash for their
// current durable end, so the common snapshot path costs one hash copy rather
// than a re-read of every batch; any other request reads the prefix.
func projectionPrefixDigest(partition *Partition, storeID StoreID, nextOffset uint64) ([32]byte, error) {
	var result [32]byte
	if nextOffset > partition.logEnd {
		return result, errors.New("snapshot offset is beyond log end")
	}
	if cached := partition.prefixDigest; cached != nil && cached.storeID == storeID && cached.covered == nextOffset {
		return finishPrefixDigest(cached.hash, nextOffset)
	}
	running := newPrefixDigest(storeID, partition.topic, partition.partition)
	buffer := make([]byte, snapshotValidationBufferBytes)
	var covered uint64
	for _, segment := range partition.segments {
		for _, batch := range segment.batches {
			end := batch.base + uint64(batch.records)
			if end > nextOffset {
				if batch.base < nextOffset {
					return result, errors.New("snapshot offset is not a batch boundary")
				}
				continue
			}
			if end <= covered {
				continue
			}
			if batch.base != covered {
				return result, errors.New("snapshot prefix has a gap")
			}
			file, release, err := partition.acquireSegmentFile(segment)
			if err != nil {
				return result, err
			}
			reader := io.NewSectionReader(file, batch.position, int64(batch.bytes))
			count, err := io.CopyBuffer(running, reader, buffer)
			release()
			if err != nil {
				return result, err
			}
			if count != int64(batch.bytes) {
				return result, io.ErrUnexpectedEOF
			}
			covered = end
		}
	}
	if covered != nextOffset {
		return result, errors.New("snapshot prefix is incomplete")
	}
	if partition.isSystemPartition() && covered == partition.logEnd {
		partition.prefixDigest = &prefixDigestState{hash: running, storeID: storeID, covered: covered}
	}
	return finishPrefixDigest(running, nextOffset)
}

func encodeCatalogSnapshotPayload(projection *catalogProjection) ([]byte, uint64, error) {
	initialized, err := encodeStoreInitializedPayload(projection.initialized)
	if err != nil {
		return nil, 0, err
	}
	topics := make([]*catalogTopic, 0, len(projection.topicsByID))
	for _, topic := range projection.topicsByID {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool { return string(topics[i].descriptor.ID[:]) < string(topics[j].descriptor.ID[:]) })
	payload := append([]byte(nil), initialized...)
	payload = appendU32(payload, uint32(len(topics)))
	entries := uint64(1 + len(topics))
	for _, topic := range topics {
		payload = appendU64(payload, topic.creationOffset)
		payload = appendID(payload, [16]byte(topic.descriptor.ID))
		payload, err = appendString(payload, topic.descriptor.Name, maxTopicNameLen)
		if err != nil {
			return nil, 0, err
		}
		payload = appendU32(payload, uint32(len(topic.descriptor.Partitions)))
		entries += uint64(len(topic.descriptor.Partitions))
		for _, partition := range topic.descriptor.Partitions {
			payload = appendU32(payload, partition.Partition)
			config, err := encodePartitionConfig(partition.Config)
			if err != nil {
				return nil, 0, err
			}
			payload = append(payload, config...)
			payload, err = encodeAnchor(payload, eventAnchor{ID: partition.InitialSegment, HeaderHash: partition.InitialHeaderHash})
			if err != nil {
				return nil, 0, err
			}
			payload = appendU64(payload, partition.RetainedL)
			payload, err = encodeAnchor(payload, eventAnchor{ID: partition.CurrentRetainedSegment, HeaderHash: partition.CurrentRetainedHeaderHash})
			if err != nil {
				return nil, 0, err
			}
			payload = appendU64(payload, partition.BoundaryCatalogNext)
			payload = appendU64(payload, partition.MaximumRecordedRetirementH)
		}
	}
	if uint64(len(payload)) > maxSnapshotPayload || entries > maxSnapshotEntries {
		return nil, 0, errors.Join(api.ErrResourceLimit, errors.New("catalog snapshot exceeds bounds"))
	}
	return payload, entries, nil
}

func buildSnapshot(partition *Partition, kind uint16, storeID StoreID, payload []byte, entries uint64) ([]byte, error) {
	if uint64(len(payload)) > maxSnapshotPayload || entries > maxSnapshotEntries {
		return nil, errors.Join(api.ErrResourceLimit, errors.New("snapshot exceeds bounds"))
	}
	prefix, err := projectionPrefixDigest(partition, storeID, partition.logEnd)
	if err != nil {
		return nil, err
	}
	header := encodeSnapshotHeader(snapshotHeader{kind: kind, storeID: storeID, topic: partition.topic, partition: partition.partition, nextOffset: partition.logEnd, payloadBytes: uint64(len(payload)), prefixDigest: prefix, entryCount: entries})
	data := append(header, payload...)
	digest := sha256.Sum256(data)
	return append(data, digest[:]...), nil
}

func publishSnapshot(path string, data []byte) error {
	if uint64(len(data)) > maxSnapshotFileBytes {
		return errors.New("snapshot size is outside bounds")
	}
	temporary, err := fsCreateTemp(filepath.Dir(path), ".projection-snapshot-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	keep := false
	defer func() {
		if !keep {
			_ = fsRemove(temporaryPath)
		}
	}()
	if count, err := fileWrite(temporary, data); err != nil || count != len(data) {
		return errors.Join(err, io.ErrShortWrite)
	}
	if err := fileSync(temporary); err != nil {
		return err
	}
	if err := fileClose(temporary); err != nil {
		return err
	}
	if err := fsRename(temporaryPath, path); err != nil {
		return err
	}
	keep = true
	return syncDir(filepath.Dir(path))
}

func validateSnapshotFile(path string, partition *Partition, kind uint16, storeID StoreID, expectedPayload []byte, expectedEntries uint64) error {
	file, err := fsOpen(path)
	if err != nil {
		return err
	}
	defer fileClose(file)
	stat, err := fileStat(file)
	if err != nil {
		return err
	}
	if stat.Size() < snapshotHeaderBytes+snapshotTrailerBytes || uint64(stat.Size()) > maxSnapshotFileBytes {
		return errors.New("snapshot size is invalid")
	}
	headerBytes := make([]byte, snapshotHeaderBytes)
	if _, err := io.ReadFull(file, headerBytes); err != nil {
		return err
	}
	header, err := decodeSnapshotHeader(headerBytes)
	if err != nil {
		return err
	}
	fileBytes := uint64(snapshotHeaderBytes) + header.payloadBytes + snapshotTrailerBytes
	if fileBytes != uint64(stat.Size()) {
		return errors.New("snapshot length mismatch")
	}
	if header.kind != kind || header.storeID != storeID || header.topic != partition.topic || header.partition != partition.partition || header.nextOffset != partition.logEnd || header.entryCount != expectedEntries || header.payloadBytes != uint64(len(expectedPayload)) {
		return errors.New("snapshot identity or coverage mismatch")
	}
	prefix, err := projectionPrefixDigest(partition, storeID, header.nextOffset)
	if err != nil {
		return err
	}
	if prefix != header.prefixDigest {
		return errors.New("snapshot source mismatch")
	}
	hash := sha256.New()
	hash.Write(headerBytes)
	buffer := make([]byte, snapshotValidationBufferBytes)
	remaining := header.payloadBytes
	payloadOffset := 0
	payloadMatches := true
	for remaining != 0 {
		count := len(buffer)
		if uint64(count) > remaining {
			count = int(remaining)
		}
		if _, err := io.ReadFull(file, buffer[:count]); err != nil {
			return err
		}
		hash.Write(buffer[:count])
		if !bytes.Equal(buffer[:count], expectedPayload[payloadOffset:payloadOffset+count]) {
			payloadMatches = false
		}
		payloadOffset += count
		remaining -= uint64(count)
	}
	var trailer [snapshotTrailerBytes]byte
	if _, err := io.ReadFull(file, trailer[:]); err != nil {
		return err
	}
	if !bytes.Equal(hash.Sum(nil), trailer[:]) {
		return errors.New("snapshot digest mismatch")
	}
	if !payloadMatches {
		return errors.New("snapshot state mismatch")
	}
	return nil
}

// pendingSnapshot is one built, not yet published, projection cache.
type pendingSnapshot struct {
	dir  string
	kind uint16
	data []byte
}

// SaveSnapshots publishes replaceable projection caches for both reserved logs.
// The authoritative logs remain the only recovery source of truth.
//
// The store mutex is held only while the snapshot bytes are built. Writing and
// syncing them runs outside it, so a snapshot never delays a consumer poll,
// commit, or lease renewal. snapshotMu keeps concurrent callers from
// publishing an older snapshot over a newer one.
func (store *Store) SaveSnapshots() error {
	store.snapshotMu.Lock()
	defer store.snapshotMu.Unlock()
	pending, err := store.buildSnapshots()
	if err != nil {
		return err
	}
	for _, snapshot := range pending {
		publishErr := publishSnapshot(filepath.Join(snapshot.dir, snapshotPathName), snapshot.data)
		store.recordSnapshotDiagnostic(snapshot.kind, publishErr)
		if publishErr != nil {
			return publishErr
		}
	}
	return nil
}

// buildSnapshots encodes both projection caches under the store mutex. The
// returned bytes describe the durable prefix observed at this instant; a later
// append simply leaves the published snapshot behind its log, which reopen
// treats as a miss and answers by replaying the authoritative records.
func (store *Store) buildSnapshots() ([]pendingSnapshot, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, api.ErrClosed
	}
	if store.closing.Load() {
		return nil, api.ErrClosing
	}
	if store.metadataUnavailable {
		return nil, api.ErrMetadataUnavailable
	}
	catalogPayload, catalogEntries, err := encodeCatalogSnapshotPayload(store.catalogState)
	if err != nil {
		return nil, err
	}
	catalogData, err := buildSnapshot(store.catalog, snapshotKindCatalog, store.storeID, catalogPayload, catalogEntries)
	if err != nil {
		return nil, err
	}
	offsetsPayload, offsetEntries, err := encodeOffsetsSnapshotPayload(store.offsetsState)
	if err != nil {
		return nil, err
	}
	offsetsData, err := buildSnapshot(store.offsets, snapshotKindOffsets, store.storeID, offsetsPayload, offsetEntries)
	if err != nil {
		return nil, err
	}
	return []pendingSnapshot{
		{dir: store.catalog.dir, kind: snapshotKindCatalog, data: catalogData},
		{dir: store.offsets.dir, kind: snapshotKindOffsets, data: offsetsData},
	}, nil
}

// recordSnapshotDiagnostic publishes the latest outcome for one reserved log.
func (store *Store) recordSnapshotDiagnostic(kind uint16, cause error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if kind == snapshotKindCatalog {
		store.snapshotDiagnostics.Catalog = cause
		return
	}
	store.snapshotDiagnostics.Offsets = cause
}

func validateOptionalSnapshots(storeID StoreID, catalog, offsets *Partition, projection *catalogProjection, offsetsState *offsetsProjection) SnapshotDiagnostics {
	var diagnostics SnapshotDiagnostics
	catalogPayload, catalogEntries, err := encodeCatalogSnapshotPayload(projection)
	if err != nil {
		diagnostics.Catalog = err
	} else {
		diagnostics.Catalog = optionalSnapshotDiagnostic(validateSnapshotFile(filepath.Join(catalog.dir, snapshotPathName), catalog, snapshotKindCatalog, storeID, catalogPayload, catalogEntries))
	}
	offsetsPayload, offsetEntries, err := encodeOffsetsSnapshotPayload(offsetsState)
	if err != nil {
		diagnostics.Offsets = err
	} else {
		diagnostics.Offsets = optionalSnapshotDiagnostic(validateSnapshotFile(filepath.Join(offsets.dir, snapshotPathName), offsets, snapshotKindOffsets, storeID, offsetsPayload, offsetEntries))
	}
	return diagnostics
}

func optionalSnapshotDiagnostic(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
