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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/ayeshLK/immulog/api"
)

// preflightTopicStorage checks the catalog-to-filesystem relationship without
// mutating storage or publishing a partially opened store.
func preflightTopicStorage(rootPath string, projection *catalogProjection) error {
	topicsDir := filepath.Join(rootPath, "topics")
	entries, err := fsReadDir(topicsDir)
	if os.IsNotExist(err) {
		if len(projection.topicsByID) == 0 {
			return nil
		}
		return corrupt(errInvalidSegment, "catalog topic storage directory is missing")
	}
	if err != nil {
		return fmt.Errorf("inspect topic storage: %w", err)
	}
	seen := make(map[api.TopicID]struct{}, len(entries))
	catalogOwned := len(projection.topicsByID) != 0
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			return corrupt(errInvalidSegment, "topic storage contains an unexpected entry "+entry.Name())
		}
		id, err := parseTopicDirectoryName(entry.Name())
		if err != nil {
			if catalogOwned {
				return err
			}
			// Direct low-level partitions may predate catalog ownership; do not
			// mutate or adopt them during metadata recovery.
			continue
		}
		topic := projection.topicsByID[id]
		if topic == nil {
			prepared, err := isRecognizedTopicPreparation(filepath.Join(topicsDir, entry.Name()), id)
			if err != nil {
				return err
			}
			if prepared {
				continue
			}
			if catalogOwned {
				return corrupt(errInvalidSegment, "topic storage has an unreferenced TopicID "+entry.Name())
			}
			// Direct low-level partitions may predate catalog ownership; do not
			// mutate or adopt them during metadata recovery.
			continue
		}
		seen[id] = struct{}{}
		if err := preflightTopicDirectory(filepath.Join(topicsDir, entry.Name()), topic); err != nil {
			return err
		}
	}
	for id := range projection.topicsByID {
		if _, ok := seen[id]; !ok {
			return corrupt(errInvalidSegment, "catalog topic storage is missing TopicID "+id.String())
		}
	}
	return nil
}

func parseTopicDirectoryName(name string) (api.TopicID, error) {
	var id api.TopicID
	decoded, err := hex.DecodeString(name)
	if err != nil || len(decoded) != len(id) || fmt.Sprintf("%x", decoded) != name {
		return id, corrupt(errInvalidSegment, "topic storage directory name is not canonical")
	}
	copy(id[:], decoded)
	if id.IsZero() || id == api.ClusterMetadataTopicID || id == api.ConsumerOffsetsTopicID {
		return id, corrupt(errInvalidSegment, "topic storage directory uses a reserved TopicID")
	}
	return id, nil
}

func isRecognizedTopicPreparation(topicDir string, topic api.TopicID) (bool, error) {
	markerPath := filepath.Join(topicDir, topicPreparationMarker)
	if _, err := fsStat(markerPath); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("stat topic preparation marker: %w", err)
	}
	if err := validateTopicPreparationMarker(markerPath, topic); err != nil {
		return false, err
	}
	return true, nil
}

func preflightTopicDirectory(path string, topic *catalogTopic) error {
	entries, err := fsReadDir(path)
	if err != nil {
		return fmt.Errorf("inspect topic directory %q: %w", path, err)
	}
	seen := make(map[uint32]struct{}, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			if entry.Name() == topicPreparationMarker {
				if err := validateTopicPreparationMarker(filepath.Join(path, entry.Name()), topic.descriptor.ID); err != nil {
					return err
				}
				continue
			}
			return corrupt(errInvalidSegment, "topic directory contains an unexpected entry "+entry.Name())
		}
		partition, err := parsePartitionDirectoryName(entry.Name())
		if err != nil {
			return err
		}
		if partition >= uint32(len(topic.descriptor.Partitions)) {
			return corrupt(errInvalidSegment, "topic storage contains an unreferenced partition directory")
		}
		seen[partition] = struct{}{}
		descriptor := topic.descriptor.Partitions[partition]
		if err := preflightPartitionDirectory(filepath.Join(path, entry.Name()), topic.descriptor.ID, partition, descriptor, topic.retired[partition]); err != nil {
			return err
		}
	}
	for index := range topic.descriptor.Partitions {
		partition := uint32(index)
		if _, ok := seen[partition]; !ok {
			return corrupt(errInvalidSegment, "topic storage is missing a catalog partition")
		}
	}
	return nil
}

func parsePartitionDirectoryName(name string) (uint32, error) {
	value, err := strconv.ParseUint(name, 10, 32)
	if err != nil || strconv.FormatUint(value, 10) != name || value > uint64(int32Max) {
		return 0, corrupt(errInvalidSegment, "partition directory name is not canonical")
	}

	return uint32(value), nil
}
func preflightPartitionDirectory(path string, topic api.TopicID, partition uint32, descriptor TopicPartition, retired map[uint64]retiredSegmentEvent) error {
	first, headerBytes, err := preflightSegmentChain(path, topic, partition, descriptor.Config.SegmentMaxBytes, descriptor.RetainedL, false, retired)
	if err != nil {
		return err
	}
	if first.BaseOffset != descriptor.RetainedL || first.ID != descriptor.CurrentRetainedSegment {
		return corrupt(errInvalidSegment, "catalog partition retained segment identity mismatch")
	}
	if segmentHeaderHash(headerBytes) != descriptor.CurrentRetainedHeaderHash {
		return corrupt(errInvalidSegment, "catalog partition retained header hash mismatch")
	}
	return nil
}

// preflightSystemLogDirectory validates all authoritative system-log bytes
// before mutable recovery is allowed to repair a verified final tail.
func preflightSystemLogDirectory(path string, topic api.TopicID, partition uint32, config PartitionConfigV1) error {
	_, _, err := preflightSegmentChain(path, topic, partition, config.SegmentMaxBytes, 0, true, nil)
	return err
}
func preflightSegmentChain(path string, topic api.TopicID, partition uint32, maxBytes, initialOffset uint64, allowSnapshot bool, retired map[uint64]retiredSegmentEvent) (SegmentHeader, []byte, error) {
	files, err := discoverSegments(path)
	if err != nil {
		return SegmentHeader{}, nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].base < files[j].base })
	retained := make([]discoveredSegment, 0, len(files))
	allowed := make(map[string]struct{}, len(files)*3+len(retired)*3)
	for base, event := range retired {
		if base >= initialOffset || event.Base != base {
			return SegmentHeader{}, nil, corrupt(errInvalidSegment, "retired inventory is outside the retained boundary")
		}
		name := fmt.Sprintf("%020d", base)
		allowed[name+".log"] = struct{}{}
		allowed[name+".index"] = struct{}{}
		allowed[name+".timeindex"] = struct{}{}
	}
	for _, file := range files {
		if file.base < initialOffset {
			event, known := retired[file.base]
			if !known {
				return SegmentHeader{}, nil, corrupt(errInvalidSegment, "partition contains an unrecognized retired segment")
			}
			if err := validateRetiredSegmentReadOnly(file.path, topic, partition, event, maxBytes); err != nil {
				return SegmentHeader{}, nil, err
			}
			continue
		}
		retained = append(retained, file)
		base := strings.TrimSuffix(filepath.Base(file.path), ".log")
		allowed[base+".log"] = struct{}{}
		allowed[base+".index"] = struct{}{}
		allowed[base+".timeindex"] = struct{}{}
	}
	if len(retained) == 0 || retained[0].base != initialOffset {
		return SegmentHeader{}, nil, corrupt(errInvalidSegment, "partition has no retained-base segment")
	}
	entries, err := fsReadDir(path)
	if err != nil {
		return SegmentHeader{}, nil, fmt.Errorf("inspect partition directory %q: %w", path, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			return SegmentHeader{}, nil, corrupt(errInvalidSegment, "partition directory contains an unexpected subdirectory")
		}
		if _, ok := allowed[name]; ok || isRecognizedSegmentTemporary(name) || allowSnapshot && isRecognizedSnapshotArtifact(name) {
			continue
		}
		return SegmentHeader{}, nil, corrupt(errInvalidSegment, "partition directory contains an unexpected file "+name)
	}
	expected := initialOffset
	var first SegmentHeader
	var firstHeaderBytes []byte
	for index, file := range retained {
		next, headerBytes, err := validateSegmentReadOnly(file.path, topic, partition, index == len(retained)-1, expected, maxBytes)
		if err != nil {
			return SegmentHeader{}, nil, err
		}
		if index == 0 {
			first, err = DecodeSegmentHeader(headerBytes, topic, partition)
			if err != nil {
				return SegmentHeader{}, nil, err
			}
			firstHeaderBytes = append([]byte(nil), headerBytes...)
		}
		expected = next
	}
	return first, firstHeaderBytes, nil
}

func validateRetiredSegmentReadOnly(path string, topic api.TopicID, partition uint32, event retiredSegmentEvent, maxBytes uint64) error {
	file, err := fsOpen(path)
	if err != nil {
		return fmt.Errorf("open retired segment %q: %w", path, err)
	}
	defer fileClose(file)
	info, err := fileStat(file)
	if err != nil {
		return fmt.Errorf("stat retired segment %q: %w", path, err)
	}
	if info.Size() < int64(SegmentHeaderBytes) || uint64(info.Size()) != event.Bytes {
		return corrupt(errInvalidSegment, "retired segment length does not match catalog inventory")
	}
	data := make([]byte, SegmentHeaderBytes)
	if err := readAtFull(file, data, 0); err != nil {
		return fmt.Errorf("read retired segment header %q: %w", path, err)
	}
	header, err := DecodeSegmentHeader(data, topic, partition)
	if err != nil {
		return err
	}
	if header.BaseOffset != event.Base || header.ID != event.Anchor.ID || segmentHeaderHash(data) != event.Anchor.HeaderHash {
		return corrupt(errInvalidSegment, "retired segment identity does not match catalog inventory")
	}
	next, _, err := validateSegmentReadOnly(path, topic, partition, false, event.Base, maxBytes)
	if err != nil {
		return err
	}
	if next != event.End {
		return corrupt(errInvalidSegment, "retired segment end does not match catalog inventory")
	}
	return nil
}
func isRecognizedSegmentTemporary(name string) bool {
	return strings.HasPrefix(name, ".index-") && strings.HasSuffix(name, ".tmp") ||
		strings.HasPrefix(name, ".segment-") && strings.HasSuffix(name, ".tmp")
}

func isRecognizedSnapshotArtifact(name string) bool {
	return name == snapshotPathName ||
		strings.HasPrefix(name, ".projection-snapshot-") && strings.HasSuffix(name, ".tmp")
}

// validateSegmentReadOnly validates a segment's header and all complete
// batches without truncating an incomplete final tail. The normal partition
// opener performs that permitted repair after this preflight succeeds.
func validateSegmentReadOnly(path string, topic api.TopicID, partition uint32, final bool, expected uint64, maxBytes uint64) (uint64, []byte, error) {
	file, err := fsOpen(path)
	if err != nil {
		return 0, nil, fmt.Errorf("open segment %q: %w", path, err)
	}
	defer fileClose(file)
	info, err := fileStat(file)
	if err != nil {
		return 0, nil, fmt.Errorf("stat segment %q: %w", path, err)
	}
	if info.Size() < int64(SegmentHeaderBytes) || uint64(info.Size()) > maxBytes {
		return 0, nil, corrupt(errInvalidSegment, "segment size is outside configured limits")
	}
	data, err := readFile(file, info.Size())
	if err != nil {
		return 0, nil, corrupt(errInvalidSegment, fmt.Sprintf("read segment %q: %v", path, err))
	}
	headerBytes := data[:SegmentHeaderBytes]
	header, err := DecodeSegmentHeader(headerBytes, topic, partition)
	if err != nil {
		return 0, nil, err
	}
	wantName := fmt.Sprintf("%020d.log", header.BaseOffset)
	if filepath.Base(path) != wantName || header.BaseOffset != expected {
		return 0, nil, corrupt(errInvalidSegment, "segment header/path/order mismatch")
	}
	current := header.BaseOffset
	position := int(SegmentHeaderBytes)
	var records uint64
	for position < len(data) {
		remaining := len(data) - position
		if remaining < int(BatchHeaderBytes) {
			return 0, nil, corrupt(errInvalidBatch, "partial batch header")
		}
		batchLength := binary.LittleEndian.Uint32(data[position+8 : position+12])
		if uint64(batchLength) > uint64(remaining) {
			if final && canTruncateTail(data[position:], current) {
				return current, headerBytes, nil
			}
			return 0, nil, corrupt(errInvalidBatch, "incomplete batch body")
		}
		if batchLength < uint32(BatchHeaderBytes)+BatchTrailerBytes || position+int(batchLength) > len(data) {
			return 0, nil, corrupt(errInvalidBatch, "invalid batch length")
		}
		batch, err := DecodeBatch(data[position:position+int(batchLength)], topic, partition)
		if err != nil {
			return 0, nil, fmt.Errorf("validate batch at %q:%d: %w", path, position, err)
		}
		if batch.BaseOffset != current {
			return 0, nil, corrupt(errInvalidBatch, "batch base offset is not the expected next offset")
		}
		current += uint64(len(batch.Records))
		records += uint64(len(batch.Records))
		position += int(batchLength)
	}
	if !final && records == 0 {
		return 0, nil, corrupt(errInvalidSegment, "non-final segment is empty")
	}
	return current, headerBytes, nil
}
