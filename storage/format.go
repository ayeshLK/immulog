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

// Package storage provides a durable, append-only, partitioned event log and
// the immutable on-disk format that backs it.
package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"unicode/utf8"

	"github.com/ayeshLK/immulog/api"
)

const (
	SegmentMagic              = "IELSEG00"
	BatchMagic                = "IELB"
	FormatVersion      uint16 = 1
	SegmentHeaderBytes uint16 = 72
	BatchHeaderBytes   uint16 = 48
	BatchTrailerBytes         = 4
	RecordPrefixBytes         = 28

	MaxBatchBytes      uint32 = 64 * 1024 * 1024
	MaxRecordBytes     uint32 = 16 * 1024 * 1024
	MaxBatchRecords    uint32 = 65_536
	MaxRecordHeaders   uint32 = 1_024
	MaxHeaderNameBytes uint32 = 65_535
	NullBytesLength    uint32 = math.MaxUint32
)

const maxOffset = uint64(math.MaxInt64)

// EventType identifies the canonical event values stored in system logs.
type EventType uint16

const (
	EventTopicCreated              EventType = 1
	EventPartitionLogStartAdvanced EventType = 2
	EventGroupCreated              EventType = 3
	EventLocalAssignmentChanged    EventType = 4
	EventOffsetCommitted           EventType = 5
	EventStoreInitialized          EventType = 6
)

// SegmentHeader is the mutable-free identity header for a segment. Record
// count, end offset, and active state are intentionally not stored here.
type SegmentHeader struct {
	Topic      api.TopicID
	Partition  uint32
	BaseOffset uint64
	ID         api.SegmentID
}

var (
	errInvalidBatch   = errors.New("storage: invalid batch")
	errInvalidRecord  = errors.New("storage: invalid record")
	errInvalidSegment = errors.New("storage: invalid segment")
)

// CRC32C returns the v1 checksum for data. The numeric checksum is serialized
// little-endian by the format writers.
func CRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}

func corrupt(kind error, detail string) error {
	return errors.Join(api.ErrCorruptLog, kind, errors.New(detail))
}

func invalidArgument(kind error, detail string) error {
	return errors.Join(api.ErrInvalidArgument, kind, errors.New(detail))
}

func unsupported(detail string) error {
	return errors.Join(api.ErrUnsupportedFormat, errors.New(detail))
}

func validHeaderName(name string) bool {
	return utf8.ValidString(name) && uint64(len(name)) <= uint64(MaxHeaderNameBytes)
}

func validOffsetRange(base uint64, count uint32) bool {
	return base <= maxOffset && uint64(count) > 0 && base+uint64(count) <= maxOffset
}

// EncodeSegmentHeader serializes the 72-byte v1 segment header.
func EncodeSegmentHeader(header SegmentHeader) ([]byte, error) {
	if header.Topic.IsZero() || header.ID.IsZero() {
		return nil, invalidArgument(errInvalidSegment, "segment and topic IDs must be nonzero")
	}
	if header.Partition > math.MaxInt32 || header.BaseOffset > maxOffset {
		return nil, invalidArgument(errInvalidSegment, "partition or base offset is outside v1 range")
	}

	data := make([]byte, SegmentHeaderBytes)
	copy(data[0:8], SegmentMagic)
	binary.LittleEndian.PutUint16(data[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(data[10:12], SegmentHeaderBytes)
	copy(data[16:32], header.Topic[:])
	binary.LittleEndian.PutUint32(data[32:36], header.Partition)
	binary.LittleEndian.PutUint16(data[36:38], FormatVersion)
	binary.LittleEndian.PutUint64(data[40:48], header.BaseOffset)
	copy(data[48:64], header.ID[:])
	binary.LittleEndian.PutUint32(data[68:72], CRC32C(data[:68]))
	return data, nil
}

// DecodeSegmentHeader validates and decodes a complete v1 segment header.
func DecodeSegmentHeader(data []byte, expectedTopic api.TopicID, expectedPartition uint32) (SegmentHeader, error) {
	if len(data) < int(SegmentHeaderBytes) {
		return SegmentHeader{}, corrupt(errInvalidSegment, "incomplete segment header")
	}
	if string(data[0:8]) != SegmentMagic {
		return SegmentHeader{}, corrupt(errInvalidSegment, "segment magic mismatch")
	}
	if binary.LittleEndian.Uint16(data[10:12]) != SegmentHeaderBytes {
		return SegmentHeader{}, corrupt(errInvalidSegment, "segment header size mismatch")
	}
	if binary.LittleEndian.Uint32(data[68:72]) != CRC32C(data[:68]) {
		return SegmentHeader{}, corrupt(errInvalidSegment, "segment header checksum mismatch")
	}
	if binary.LittleEndian.Uint16(data[8:10]) != FormatVersion || binary.LittleEndian.Uint16(data[36:38]) != FormatVersion {
		return SegmentHeader{}, unsupported("unsupported segment or batch version")
	}
	if binary.LittleEndian.Uint32(data[12:16]) != 0 || binary.LittleEndian.Uint16(data[38:40]) != 0 || binary.LittleEndian.Uint32(data[64:68]) != 0 {
		return SegmentHeader{}, corrupt(errInvalidSegment, "nonzero reserved segment fields")
	}

	var header SegmentHeader
	copy(header.Topic[:], data[16:32])
	header.Partition = binary.LittleEndian.Uint32(data[32:36])
	header.BaseOffset = binary.LittleEndian.Uint64(data[40:48])
	copy(header.ID[:], data[48:64])
	if header.Topic.IsZero() || header.ID.IsZero() || header.Partition > math.MaxInt32 || header.BaseOffset > maxOffset {
		return SegmentHeader{}, corrupt(errInvalidSegment, "invalid segment identity or range")
	}
	if !expectedTopic.IsZero() && header.Topic != expectedTopic {
		return SegmentHeader{}, corrupt(errInvalidSegment, "segment topic identity mismatch")
	}
	if header.Partition != expectedPartition {
		return SegmentHeader{}, corrupt(errInvalidSegment, "segment partition identity mismatch")
	}
	return header, nil
}
