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
	"errors"
	"math"
	"time"

	"github.com/ayeshLK/immulog/api"
)

// PartitionConfigV1 is the fully resolved, immutable configuration recorded
// in a catalog event. Operating limits such as in-flight admission are not
// part of this on-disk policy.
type PartitionConfigV1 struct {
	MaxRecordBytes       uint32
	MaxBatchBytes        uint32
	MaxBatchRecords      uint32
	SegmentMaxBytes      uint64
	BatchLinger          time.Duration
	RetentionMask        uint8
	RetentionMillis      uint64
	RetentionBytes       uint64
	MaxSegmentAgeMillis  uint64
	RetentionCheckMillis uint64
}

const partitionConfigBytes = 64

func (config PartitionConfigV1) validate(system bool) error {
	minimumBatch := uint32(BatchHeaderBytes) + BatchTrailerBytes + RecordPrefixBytes
	if config.MaxRecordBytes < RecordPrefixBytes || config.MaxRecordBytes > MaxRecordBytes {
		return errors.Join(api.ErrInvalidArgument, errors.New("configured maximum record bytes are outside v1 limits"))
	}
	if config.MaxBatchBytes < minimumBatch || config.MaxBatchBytes > MaxBatchBytes || config.MaxRecordBytes+uint32(BatchHeaderBytes)+BatchTrailerBytes > config.MaxBatchBytes {
		return errors.Join(api.ErrInvalidArgument, errors.New("configured maximum batch bytes are outside v1 limits"))
	}
	if config.MaxBatchRecords == 0 || config.MaxBatchRecords > MaxBatchRecords {
		return errors.Join(api.ErrInvalidArgument, errors.New("configured maximum batch records are outside v1 limits"))
	}
	if config.SegmentMaxBytes < uint64(SegmentHeaderBytes)+uint64(config.MaxBatchBytes) || config.SegmentMaxBytes > math.MaxInt64 {
		return errors.Join(api.ErrInvalidArgument, errors.New("configured segment bytes are outside v1 limits"))
	}
	if config.RetentionMask&^uint8(3) != 0 {
		return errors.Join(api.ErrUnsupportedFormat, errors.New("unsupported retention policy bit"))
	}
	if system && config.RetentionMask != 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("system-log retention is disabled"))
	}
	if config.RetentionMask != 0 && config.RetentionCheckMillis == 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("retention requires a positive check interval"))
	}
	if config.RetentionMask&1 != 0 && config.MaxSegmentAgeMillis == 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("time retention requires a segment age"))
	}
	for _, millis := range []uint64{config.RetentionMillis, config.MaxSegmentAgeMillis, config.RetentionCheckMillis} {
		if millis > uint64(math.MaxInt64)/uint64(time.Millisecond) {
			return errors.Join(api.ErrInvalidArgument, errors.New("configured duration is outside Go duration range"))
		}
	}
	if config.RetentionBytes > math.MaxInt64 {
		return errors.Join(api.ErrInvalidArgument, errors.New("configured retention bytes are outside v1 limits"))
	}
	if config.RetentionMask&1 == 0 && config.RetentionMillis != 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("disabled time retention has a nonzero duration"))
	}
	if config.RetentionMask&2 == 0 && config.RetentionBytes != 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("disabled size retention has a nonzero byte limit"))
	}
	if config.RetentionMask == 0 && config.RetentionCheckMillis != 0 {
		return errors.Join(api.ErrInvalidArgument, errors.New("disabled retention has a nonzero check interval"))
	}
	return nil
}

func encodePartitionConfig(config PartitionConfigV1) ([]byte, error) {
	if err := config.validate(false); err != nil {
		return nil, err
	}
	data := make([]byte, partitionConfigBytes)
	binary.LittleEndian.PutUint16(data[0:2], 1)
	binary.LittleEndian.PutUint16(data[2:4], partitionConfigBytes)
	binary.LittleEndian.PutUint32(data[4:8], config.MaxRecordBytes)
	binary.LittleEndian.PutUint32(data[8:12], config.MaxBatchBytes)
	binary.LittleEndian.PutUint32(data[12:16], config.MaxBatchRecords)
	binary.LittleEndian.PutUint64(data[16:24], config.SegmentMaxBytes)
	linger, err := durationMillis(config.BatchLinger)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(data[24:28], linger)
	data[28] = config.RetentionMask
	binary.LittleEndian.PutUint64(data[32:40], config.RetentionMillis)
	binary.LittleEndian.PutUint64(data[40:48], config.RetentionBytes)
	binary.LittleEndian.PutUint64(data[48:56], config.MaxSegmentAgeMillis)
	binary.LittleEndian.PutUint64(data[56:64], config.RetentionCheckMillis)
	return data, nil
}

func decodePartitionConfig(cursor *eventCursor, system bool) (PartitionConfigV1, error) {
	data, err := cursor.take(partitionConfigBytes)
	if err != nil {
		return PartitionConfigV1{}, err
	}
	if binary.LittleEndian.Uint16(data[0:2]) != 1 {
		return PartitionConfigV1{}, unsupported("unsupported partition configuration version")
	}
	if binary.LittleEndian.Uint16(data[2:4]) != partitionConfigBytes || data[29] != 0 || binary.LittleEndian.Uint16(data[30:32]) != 0 {
		return PartitionConfigV1{}, corrupt(errInvalidRecord, "invalid partition configuration encoding")
	}
	config := PartitionConfigV1{
		MaxRecordBytes:       binary.LittleEndian.Uint32(data[4:8]),
		MaxBatchBytes:        binary.LittleEndian.Uint32(data[8:12]),
		MaxBatchRecords:      binary.LittleEndian.Uint32(data[12:16]),
		SegmentMaxBytes:      binary.LittleEndian.Uint64(data[16:24]),
		BatchLinger:          time.Duration(binary.LittleEndian.Uint32(data[24:28])) * time.Millisecond,
		RetentionMask:        data[28],
		RetentionMillis:      binary.LittleEndian.Uint64(data[32:40]),
		RetentionBytes:       binary.LittleEndian.Uint64(data[40:48]),
		MaxSegmentAgeMillis:  binary.LittleEndian.Uint64(data[48:56]),
		RetentionCheckMillis: binary.LittleEndian.Uint64(data[56:64]),
	}
	if err := config.validate(system); err != nil {
		return PartitionConfigV1{}, corrupt(errInvalidRecord, err.Error())
	}
	return config, nil
}

func durationMillis(value time.Duration) (uint32, error) {
	if value < 0 || value%time.Millisecond != 0 {
		return 0, errors.Join(api.ErrInvalidArgument, errors.New("duration must be a nonnegative whole millisecond"))
	}
	millis := value / time.Millisecond
	if millis > math.MaxUint32 {
		return 0, errors.Join(api.ErrInvalidArgument, errors.New("batch linger exceeds v1 range"))
	}
	return uint32(millis), nil
}

func durationMillis64(value time.Duration, name string) (uint64, error) {
	if value < 0 || value%time.Millisecond != 0 {
		return 0, errors.Join(api.ErrInvalidArgument, errors.New(name+" must be a nonnegative whole millisecond"))
	}
	return uint64(value / time.Millisecond), nil
}

func configFromOptions(options PartitionOptions) (PartitionConfigV1, error) {
	options.normalize()
	linger, err := durationMillis(options.BatchLinger)
	if err != nil {
		return PartitionConfigV1{}, err
	}
	retentionMillis, err := durationMillis64(options.RetentionDuration, "retention duration")
	if err != nil {
		return PartitionConfigV1{}, err
	}
	maxSegmentAgeMillis, err := durationMillis64(options.MaxSegmentAge, "maximum segment age")
	if err != nil {
		return PartitionConfigV1{}, err
	}
	retentionCheckMillis, err := durationMillis64(options.RetentionCheck, "retention check interval")
	if err != nil {
		return PartitionConfigV1{}, err
	}
	var retentionMask uint8
	if options.RetentionTimeEnabled {
		retentionMask |= 1
	}
	if options.RetentionSizeEnabled {
		retentionMask |= 2
	}
	if retentionMask == 0 {
		retentionMillis = 0
		retentionCheckMillis = 0
	} else if retentionCheckMillis == 0 {
		return PartitionConfigV1{}, errors.Join(api.ErrInvalidArgument, errors.New("retention requires a positive check interval"))
	}
	if retentionMask&1 == 0 {
		retentionMillis = 0
	}
	if retentionMask&2 == 0 {
		options.RetentionBytes = 0
	}
	config := PartitionConfigV1{
		MaxRecordBytes: options.RecordBytes, MaxBatchBytes: options.BatchBytes,
		MaxBatchRecords: options.BatchRecords, SegmentMaxBytes: options.SegmentBytes,
		BatchLinger: time.Duration(linger) * time.Millisecond, RetentionMask: retentionMask,
		RetentionMillis: retentionMillis, RetentionBytes: options.RetentionBytes,
		MaxSegmentAgeMillis: maxSegmentAgeMillis, RetentionCheckMillis: retentionCheckMillis,
	}
	if err := config.validate(false); err != nil {
		return PartitionConfigV1{}, err
	}
	return config, nil
}

func (config PartitionConfigV1) options() PartitionOptions {
	options := PartitionOptions{
		SegmentBytes: config.SegmentMaxBytes, BatchBytes: config.MaxBatchBytes,
		BatchRecords: config.MaxBatchRecords, RecordBytes: config.MaxRecordBytes,
		BatchLinger: config.BatchLinger, RetentionTimeEnabled: config.RetentionMask&1 != 0,
		RetentionSizeEnabled: config.RetentionMask&2 != 0,
		RetentionDuration:    time.Duration(config.RetentionMillis) * time.Millisecond,
		RetentionBytes:       config.RetentionBytes,
		MaxSegmentAge:        time.Duration(config.MaxSegmentAgeMillis) * time.Millisecond,
		RetentionCheck:       time.Duration(config.RetentionCheckMillis) * time.Millisecond,
	}
	options.normalize()
	return options
}

func systemPartitionConfig() PartitionConfigV1 {
	return PartitionConfigV1{
		MaxRecordBytes:  MaxRecordBytes,
		MaxBatchBytes:   MaxBatchBytes,
		MaxBatchRecords: MaxBatchRecords,
		SegmentMaxBytes: defaultSegmentBytes,
	}
}
