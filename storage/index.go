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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
)

const (
	IndexMagic            = "IELIDX00"
	IndexHeaderBytes      = uint16(128)
	OffsetIndexEntryBytes = uint16(32)
	TimeIndexEntryBytes   = uint16(40)
	MaxIndexFileBytes     = uint32(64 * 1024 * 1024)
	DefaultIndexStride    = uint64(4096)
)

// StoreID identifies the durable store lineage recorded by StoreInitialized and
// used to bind rebuildable caches. Fresh bootstrap generates it randomly;
// reopen validates and reuses the persisted value.
type StoreID [16]byte

type batchInfo struct {
	base     uint64
	position int64
	bytes    uint32
	records  uint32
	maxTime  int64
}

type offsetIndexEntry struct {
	base     uint64
	position uint64
	bytes    uint32
	records  uint32
}

type timeIndexEntry struct {
	prefixMax int64
	base      uint64
	position  uint64
	bytes     uint32
	records   uint32
}

type indexHeader struct {
	kind       uint16
	entryBytes uint16
	storeID    StoreID
	topic      [16]byte
	partition  uint32
	segmentID  [16]byte
	base       uint64
	headerHash [32]byte
	stride     uint32
}

func storeIDForIdentity(identity dirIdentity) StoreID {
	var encoded [16]byte
	binary.LittleEndian.PutUint64(encoded[0:8], identity.device)
	binary.LittleEndian.PutUint64(encoded[8:16], identity.inode)
	digest := sha256.Sum256(append([]byte("immulog-store-id-v1\x00"), encoded[:]...))
	var id StoreID
	copy(id[:], digest[:16])
	if id == (StoreID{}) {
		id[15] = 1
	}
	return id
}

func segmentHeaderHash(encoded []byte) [32]byte {
	return sha256.Sum256(encoded[:SegmentHeaderBytes])
}

func newIndexHeader(kind, entryBytes uint16, storeID StoreID, segment *segment, stride uint64) indexHeader {
	if stride == 0 {
		stride = DefaultIndexStride
	}
	if stride > math.MaxUint32 {
		stride = math.MaxUint32
	}
	var topic [16]byte
	copy(topic[:], segment.header.Topic[:])
	var segmentID [16]byte
	copy(segmentID[:], segment.header.ID[:])
	return indexHeader{
		kind:       kind,
		entryBytes: entryBytes,
		storeID:    storeID,
		topic:      topic,
		partition:  segment.header.Partition,
		segmentID:  segmentID,
		base:       segment.header.BaseOffset,
		headerHash: segment.headerHash,
		stride:     uint32(stride),
	}
}

func encodeIndexHeader(header indexHeader) []byte {
	data := make([]byte, IndexHeaderBytes)
	copy(data[0:8], IndexMagic)
	binary.LittleEndian.PutUint16(data[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(data[10:12], IndexHeaderBytes)
	binary.LittleEndian.PutUint16(data[12:14], header.kind)
	binary.LittleEndian.PutUint16(data[14:16], header.entryBytes)
	copy(data[24:40], header.storeID[:])
	copy(data[40:56], header.topic[:])
	binary.LittleEndian.PutUint32(data[56:60], header.partition)
	copy(data[64:80], header.segmentID[:])
	binary.LittleEndian.PutUint64(data[80:88], header.base)
	copy(data[88:120], header.headerHash[:])
	binary.LittleEndian.PutUint32(data[120:124], header.stride)
	binary.LittleEndian.PutUint32(data[124:128], CRC32C(data[:124]))
	return data
}

func decodeIndexHeader(data []byte) (indexHeader, error) {
	if len(data) < int(IndexHeaderBytes) {
		return indexHeader{}, errors.New("index header is incomplete")
	}
	if string(data[0:8]) != IndexMagic {
		return indexHeader{}, errors.New("index magic mismatch")
	}
	if binary.LittleEndian.Uint16(data[10:12]) != IndexHeaderBytes {
		return indexHeader{}, errors.New("index header size mismatch")
	}
	if binary.LittleEndian.Uint32(data[124:128]) != CRC32C(data[:124]) {
		return indexHeader{}, errors.New("index header checksum mismatch")
	}
	if binary.LittleEndian.Uint16(data[8:10]) != FormatVersion {
		return indexHeader{}, errors.New("unsupported index version")
	}
	if binary.LittleEndian.Uint32(data[16:20]) != 0 || binary.LittleEndian.Uint32(data[20:24]) != 0 || binary.LittleEndian.Uint32(data[60:64]) != 0 {
		return indexHeader{}, errors.New("nonzero index reserved fields")
	}
	header := indexHeader{
		kind:       binary.LittleEndian.Uint16(data[12:14]),
		entryBytes: binary.LittleEndian.Uint16(data[14:16]),
		partition:  binary.LittleEndian.Uint32(data[56:60]),
		base:       binary.LittleEndian.Uint64(data[80:88]),
		stride:     binary.LittleEndian.Uint32(data[120:124]),
	}
	copy(header.storeID[:], data[24:40])
	copy(header.topic[:], data[40:56])
	copy(header.segmentID[:], data[64:80])
	copy(header.headerHash[:], data[88:120])
	if header.kind != 1 && header.kind != 2 {
		return indexHeader{}, errors.New("unsupported index kind")
	}
	wantEntryBytes := OffsetIndexEntryBytes
	if header.kind == 2 {
		wantEntryBytes = TimeIndexEntryBytes
	}
	if header.entryBytes != wantEntryBytes || header.stride == 0 {
		return indexHeader{}, errors.New("invalid index entry size or stride")
	}
	return header, nil
}

func encodeOffsetIndexEntry(entry offsetIndexEntry) []byte {
	data := make([]byte, OffsetIndexEntryBytes)
	binary.LittleEndian.PutUint64(data[0:8], entry.base)
	binary.LittleEndian.PutUint64(data[8:16], entry.position)
	binary.LittleEndian.PutUint32(data[16:20], entry.bytes)
	binary.LittleEndian.PutUint32(data[20:24], entry.records)
	binary.LittleEndian.PutUint32(data[28:32], CRC32C(data[:28]))
	return data
}

func decodeOffsetIndexEntry(data []byte) (offsetIndexEntry, error) {
	if len(data) != int(OffsetIndexEntryBytes) {
		return offsetIndexEntry{}, errors.New("invalid offset index entry size")
	}
	if binary.LittleEndian.Uint32(data[24:28]) != 0 || binary.LittleEndian.Uint32(data[28:32]) != CRC32C(data[:28]) {
		return offsetIndexEntry{}, errors.New("invalid offset index entry")
	}
	return offsetIndexEntry{
		base:     binary.LittleEndian.Uint64(data[0:8]),
		position: binary.LittleEndian.Uint64(data[8:16]),
		bytes:    binary.LittleEndian.Uint32(data[16:20]),
		records:  binary.LittleEndian.Uint32(data[20:24]),
	}, nil
}

func encodeTimeIndexEntry(entry timeIndexEntry) []byte {
	data := make([]byte, TimeIndexEntryBytes)
	binary.LittleEndian.PutUint64(data[0:8], uint64(entry.prefixMax))
	binary.LittleEndian.PutUint64(data[8:16], entry.base)
	binary.LittleEndian.PutUint64(data[16:24], entry.position)
	binary.LittleEndian.PutUint32(data[24:28], entry.bytes)
	binary.LittleEndian.PutUint32(data[28:32], entry.records)
	binary.LittleEndian.PutUint32(data[36:40], CRC32C(data[:36]))
	return data
}

func decodeTimeIndexEntry(data []byte) (timeIndexEntry, error) {
	if len(data) != int(TimeIndexEntryBytes) {
		return timeIndexEntry{}, errors.New("invalid time index entry size")
	}
	if binary.LittleEndian.Uint32(data[32:36]) != 0 || binary.LittleEndian.Uint32(data[36:40]) != CRC32C(data[:36]) {
		return timeIndexEntry{}, errors.New("invalid time index entry")
	}
	return timeIndexEntry{
		prefixMax: int64(binary.LittleEndian.Uint64(data[0:8])),
		base:      binary.LittleEndian.Uint64(data[8:16]),
		position:  binary.LittleEndian.Uint64(data[16:24]),
		bytes:     binary.LittleEndian.Uint32(data[24:28]),
		records:   binary.LittleEndian.Uint32(data[28:32]),
	}, nil
}

func indexPath(segmentPath string, time bool) string {
	extension := ".index"
	if time {
		extension = ".timeindex"
	}
	return segmentPath[:len(segmentPath)-len(filepath.Ext(segmentPath))] + extension
}

func sampleBatchIndexes(batches []batchInfo, stride uint64) ([]offsetIndexEntry, []timeIndexEntry) {
	if stride == 0 {
		stride = DefaultIndexStride
	}
	maxOffsetEntries := (uint64(MaxIndexFileBytes) - uint64(IndexHeaderBytes)) / uint64(OffsetIndexEntryBytes)
	maxTimeEntries := (uint64(MaxIndexFileBytes) - uint64(IndexHeaderBytes)) / uint64(TimeIndexEntryBytes)
	offsetEntries := make([]offsetIndexEntry, 0, len(batches))
	timeEntries := make([]timeIndexEntry, 0, len(batches))
	var lastPosition int64
	prefixMax := int64(math.MinInt64)
	for index, batch := range batches {
		if batch.maxTime > prefixMax {
			prefixMax = batch.maxTime
		}
		sample := index == 0 || uint64(batch.position-lastPosition) >= stride
		if !sample {
			continue
		}
		if uint64(len(offsetEntries)) < maxOffsetEntries {
			offsetEntries = append(offsetEntries, offsetIndexEntry{base: batch.base, position: uint64(batch.position), bytes: batch.bytes, records: batch.records})
		}
		if uint64(len(timeEntries)) < maxTimeEntries {
			timeEntries = append(timeEntries, timeIndexEntry{prefixMax: prefixMax, base: batch.base, position: uint64(batch.position), bytes: batch.bytes, records: batch.records})
		}
		lastPosition = batch.position
	}
	return offsetEntries, timeEntries
}

func readIndexFile(path string) ([]byte, error) {
	file, err := fsOpen(path)
	if err != nil {
		return nil, err
	}
	defer fileClose(file)
	stat, err := fileStat(file)
	if err != nil {
		return nil, err
	}
	if stat.Size() < int64(IndexHeaderBytes) || stat.Size() > int64(MaxIndexFileBytes) {
		return nil, errors.New("index file size outside v1 limits")
	}
	return readFile(file, stat.Size())
}

func batchMap(batches []batchInfo) map[uint64]batchInfo {
	result := make(map[uint64]batchInfo, len(batches))
	for _, batch := range batches {
		result[batch.base] = batch
	}
	return result
}

func validateIndexHeader(header indexHeader, segment *segment, storeID StoreID, time bool) error {
	wantKind := uint16(1)
	wantEntryBytes := OffsetIndexEntryBytes
	if time {
		wantKind = 2
		wantEntryBytes = TimeIndexEntryBytes
	}
	if header.kind != wantKind || header.entryBytes != wantEntryBytes || header.storeID != storeID || header.partition != segment.header.Partition || header.base != segment.header.BaseOffset || header.headerHash != segment.headerHash {
		return errors.New("index source identity mismatch")
	}
	if string(header.topic[:]) != string(segment.header.Topic[:]) || string(header.segmentID[:]) != string(segment.header.ID[:]) {
		return errors.New("index topic or segment identity mismatch")
	}
	return nil
}

func validateOffsetIndex(data []byte, segment *segment, storeID StoreID) ([]offsetIndexEntry, error) {
	header, err := decodeIndexHeader(data)
	if err != nil {
		return nil, err
	}
	if err := validateIndexHeader(header, segment, storeID, false); err != nil {
		return nil, err
	}
	if (uint64(len(data))-uint64(IndexHeaderBytes))%uint64(OffsetIndexEntryBytes) != 0 {
		return nil, errors.New("offset index has trailing bytes")
	}
	entries := make([]offsetIndexEntry, 0, (len(data)-int(IndexHeaderBytes))/int(OffsetIndexEntryBytes))
	batches := batchMap(segment.batches)
	var previousBase, previousPosition uint64
	for position := int(IndexHeaderBytes); position < len(data); position += int(OffsetIndexEntryBytes) {
		entry, err := decodeOffsetIndexEntry(data[position : position+int(OffsetIndexEntryBytes)])
		if err != nil {
			return nil, err
		}
		batch, ok := batches[entry.base]
		if !ok || entry.position != uint64(batch.position) || entry.bytes != batch.bytes || entry.records != batch.records || !validOffsetRange(entry.base, entry.records) {
			return nil, errors.New("offset index entry does not match source batch")
		}
		if len(entries) > 0 && (entry.base <= previousBase || entry.position <= previousPosition) {
			return nil, errors.New("offset index entries are not ordered")
		}
		entries = append(entries, entry)
		previousBase, previousPosition = entry.base, entry.position
	}
	return entries, nil
}

func validateTimeIndex(data []byte, segment *segment, storeID StoreID) ([]timeIndexEntry, error) {
	header, err := decodeIndexHeader(data)
	if err != nil {
		return nil, err
	}
	if err := validateIndexHeader(header, segment, storeID, true); err != nil {
		return nil, err
	}
	if (uint64(len(data))-uint64(IndexHeaderBytes))%uint64(TimeIndexEntryBytes) != 0 {
		return nil, errors.New("time index has trailing bytes")
	}
	entries := make([]timeIndexEntry, 0, (len(data)-int(IndexHeaderBytes))/int(TimeIndexEntryBytes))
	batches := batchMap(segment.batches)
	var previousBase, previousPosition uint64
	prefixMax := int64(math.MinInt64)
	batchCursor := 0
	for position := int(IndexHeaderBytes); position < len(data); position += int(TimeIndexEntryBytes) {
		entry, err := decodeTimeIndexEntry(data[position : position+int(TimeIndexEntryBytes)])
		if err != nil {
			return nil, err
		}
		batch, ok := batches[entry.base]
		if !ok || entry.position != uint64(batch.position) || entry.bytes != batch.bytes || entry.records != batch.records || !validOffsetRange(entry.base, entry.records) {
			return nil, errors.New("time index entry does not match source batch")
		}
		for batchCursor < len(segment.batches) && segment.batches[batchCursor].base <= entry.base {
			if segment.batches[batchCursor].maxTime > prefixMax {
				prefixMax = segment.batches[batchCursor].maxTime
			}
			batchCursor++
		}
		if entry.prefixMax != prefixMax || (len(entries) > 0 && (entry.prefixMax < entries[len(entries)-1].prefixMax || entry.base <= previousBase || entry.position <= previousPosition)) {
			return nil, errors.New("time index prefix does not match source batches")
		}
		entries = append(entries, entry)
		previousBase, previousPosition = entry.base, entry.position
	}
	return entries, nil
}

func writeIndexFile(path string, header indexHeader, entries [][]byte) error {
	temporary, err := fsCreateTemp(filepath.Dir(path), ".index-*.tmp")
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

	write := func(data []byte) error {
		count, err := fileWrite(temporary, data)
		if err != nil {
			return err
		}
		if count != len(data) {
			return io.ErrShortWrite
		}
		return nil
	}
	if err := write(encodeIndexHeader(header)); err != nil {
		_ = fileClose(temporary)
		return err
	}
	for _, entry := range entries {
		if err := write(entry); err != nil {
			_ = fileClose(temporary)
			return err
		}
	}
	if err := fileSync(temporary); err != nil {
		_ = fileClose(temporary)
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

func installSegmentIndexes(segment *segment, storeID StoreID, stride uint64) error {
	offsetEntries, timeEntries := sampleBatchIndexes(segment.batches, stride)
	offsetBytes := make([][]byte, 0, len(offsetEntries))
	for _, entry := range offsetEntries {
		offsetBytes = append(offsetBytes, encodeOffsetIndexEntry(entry))
	}
	timeBytes := make([][]byte, 0, len(timeEntries))
	for _, entry := range timeEntries {
		timeBytes = append(timeBytes, encodeTimeIndexEntry(entry))
	}
	offsetHeader := newIndexHeader(1, OffsetIndexEntryBytes, storeID, segment, stride)
	timeHeader := newIndexHeader(2, TimeIndexEntryBytes, storeID, segment, stride)
	if err := writeIndexFile(indexPath(segment.path, false), offsetHeader, offsetBytes); err != nil {
		return fmt.Errorf("publish offset index: %w", err)
	}
	if err := writeIndexFile(indexPath(segment.path, true), timeHeader, timeBytes); err != nil {
		return fmt.Errorf("publish time index: %w", err)
	}
	segment.offsetIndex = offsetEntries
	segment.timeIndex = timeEntries
	segment.indexDirty = false
	return nil
}

func loadOrBuildIndexes(segment *segment, storeID StoreID, stride uint64) {
	if data, err := readIndexFile(indexPath(segment.path, false)); err == nil {
		if entries, validateErr := validateOffsetIndex(data, segment, storeID); validateErr == nil {
			segment.offsetIndex = entries
		}
	}
	if data, err := readIndexFile(indexPath(segment.path, true)); err == nil {
		if entries, validateErr := validateTimeIndex(data, segment, storeID); validateErr == nil {
			segment.timeIndex = entries
		}
	}
	if segment.offsetIndex == nil || segment.timeIndex == nil {
		offsetEntries, timeEntries := sampleBatchIndexes(segment.batches, stride)
		if segment.offsetIndex == nil {
			segment.offsetIndex = offsetEntries
		}
		if segment.timeIndex == nil {
			segment.timeIndex = timeEntries
		}
		// A sidecar that could not be read back has to be republished.
		segment.indexDirty = true
	}
}

func refreshSegmentIndexes(segment *segment, _ StoreID, stride uint64) {
	// Indexes are derived caches. Keep the in-memory view current while allowing
	// persisted sidecars to lag until a lifecycle checkpoint.
	offsetEntries, timeEntries := sampleBatchIndexes(segment.batches, stride)
	segment.offsetIndex = offsetEntries
	segment.timeIndex = timeEntries
	segment.indexDirty = true
}
