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

	"github.com/ayeshLK/immulog/api"
)

// EncodeRecord encodes a record body. Topic and partition are supplied by the
// enclosing segment and are therefore not present in the record bytes.
func EncodeRecord(record api.Record, baseOffset uint64) ([]byte, error) {
	if record.Offset < baseOffset || record.Offset-baseOffset > math.MaxUint32 {
		return nil, invalidArgument(errInvalidRecord, "offset delta is outside uint32 range")
	}
	if baseOffset > maxOffset || record.Offset > maxOffset {
		return nil, invalidArgument(errInvalidRecord, "offset is outside v1 range")
	}
	if len(record.Headers) > int(MaxRecordHeaders) {
		return nil, invalidArgument(errInvalidRecord, "header count exceeds v1 limit")
	}

	body := make([]byte, RecordPrefixBytes, RecordPrefixBytes+len(record.Key)+len(record.Value))
	binary.LittleEndian.PutUint32(body[4:8], uint32(record.Offset-baseOffset))
	binary.LittleEndian.PutUint64(body[8:16], uint64(record.Timestamp))
	binary.LittleEndian.PutUint32(body[24:28], uint32(len(record.Headers)))

	var err error
	body, err = appendNullable(body, record.Key)
	if err != nil {
		return nil, err
	}
	body, err = appendNullable(body, record.Value)
	if err != nil {
		return nil, err
	}
	for _, header := range record.Headers {
		if !validHeaderName(header.Name) {
			return nil, invalidArgument(errInvalidRecord, "header name is not valid UTF-8 or is too large")
		}
		body = appendU32(body, uint32(len(header.Name)))
		if header.Value == nil {
			body = appendU32(body, NullBytesLength)
		} else {
			if uint64(len(header.Value)) > uint64(math.MaxUint32) {
				return nil, errors.Join(api.ErrRecordTooLarge, errInvalidRecord)
			}
			body = appendU32(body, uint32(len(header.Value)))
		}
		body = append(body, header.Name...)
		if header.Value != nil {
			body = append(body, header.Value...)
		}
	}
	if len(body) > int(MaxRecordBytes) {
		return nil, errors.Join(api.ErrRecordTooLarge, errInvalidRecord)
	}
	binary.LittleEndian.PutUint32(body[0:4], uint32(len(body)))
	return body, nil
}

func appendU32(dst []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendNullable(dst []byte, value []byte) ([]byte, error) {
	if value == nil {
		return appendU32(dst, NullBytesLength), nil
	}
	if uint64(len(value)) > uint64(math.MaxUint32) {
		return nil, errors.Join(api.ErrRecordTooLarge, errInvalidRecord)
	}
	dst = appendU32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

type cursor struct {
	data []byte
	pos  int
}

func (c *cursor) remaining() int { return len(c.data) - c.pos }

func (c *cursor) u32() (uint32, error) {
	if c.remaining() < 4 {
		return 0, corrupt(errInvalidRecord, "truncated uint32")
	}
	value := binary.LittleEndian.Uint32(c.data[c.pos : c.pos+4])
	c.pos += 4
	return value, nil
}

func (c *cursor) bytes(length uint32) ([]byte, error) {
	if uint64(length) > uint64(c.remaining()) {
		return nil, corrupt(errInvalidRecord, "declared field exceeds record")
	}
	end := c.pos + int(length)
	value := c.data[c.pos:end]
	c.pos = end
	return value, nil
}

func (c *cursor) nullableBytes() ([]byte, error) {
	length, err := c.u32()
	if err != nil {
		return nil, err
	}
	if length == NullBytesLength {
		return nil, nil
	}
	return c.bytes(length)
}

// DecodeRecord decodes exactly one complete record body and returns owned
// payloads. The input may not contain trailing bytes.
func DecodeRecord(data []byte, baseOffset uint64, topic api.TopicID, partition uint32) (api.Record, error) {
	record, consumed, err := decodeRecord(data, baseOffset, topic, partition, nil)
	if err != nil {
		return api.Record{}, err
	}
	if consumed != len(data) {
		return api.Record{}, corrupt(errInvalidRecord, "trailing bytes after record")
	}
	return record, nil
}

func decodeRecord(data []byte, baseOffset uint64, topic api.TopicID, partition uint32, expectedDelta *uint32) (api.Record, int, error) {
	if len(data) < RecordPrefixBytes {
		return api.Record{}, 0, corrupt(errInvalidRecord, "record is shorter than fixed prefix")
	}
	recordBytes := binary.LittleEndian.Uint32(data[0:4])
	if recordBytes < RecordPrefixBytes || recordBytes > MaxRecordBytes {
		return api.Record{}, 0, corrupt(errInvalidRecord, "record length outside v1 limits")
	}
	if uint64(recordBytes) > uint64(len(data)) {
		return api.Record{}, 0, corrupt(errInvalidRecord, "record body is incomplete")
	}
	body := data[:recordBytes]
	delta := binary.LittleEndian.Uint32(body[4:8])
	if expectedDelta != nil && delta != *expectedDelta {
		return api.Record{}, 0, corrupt(errInvalidRecord, "record offset delta is not contiguous")
	}
	if baseOffset > maxOffset || uint64(delta) > maxOffset-baseOffset {
		return api.Record{}, 0, corrupt(errInvalidRecord, "record offset is outside v1 range")
	}
	headerCount := binary.LittleEndian.Uint32(body[24:28])
	if headerCount > MaxRecordHeaders {
		return api.Record{}, 0, corrupt(errInvalidRecord, "record header count outside v1 limits")
	}

	c := cursor{data: body[RecordPrefixBytes:]}
	key, err := c.nullableBytes()
	if err != nil {
		return api.Record{}, 0, err
	}
	value, err := c.nullableBytes()
	if err != nil {
		return api.Record{}, 0, err
	}
	record := api.Record{
		Topic:     topic,
		Partition: partition,
		Offset:    baseOffset + uint64(delta),
		Timestamp: int64(binary.LittleEndian.Uint64(body[8:16])),
		Key:       cloneBytes(key),
		Value:     cloneBytes(value),
		Headers:   make([]api.Header, 0, headerCount),
	}
	for index := uint32(0); index < headerCount; index++ {
		nameLength, err := c.u32()
		if err != nil {
			return api.Record{}, 0, err
		}
		if nameLength > MaxHeaderNameBytes {
			return api.Record{}, 0, corrupt(errInvalidRecord, "header name exceeds v1 limit")
		}
		valueLength, err := c.u32()
		if err != nil {
			return api.Record{}, 0, err
		}
		nameBytes, err := c.bytes(nameLength)
		if err != nil {
			return api.Record{}, 0, err
		}
		if !validHeaderName(string(nameBytes)) {
			return api.Record{}, 0, corrupt(errInvalidRecord, "header name is not valid UTF-8")
		}
		var headerValue []byte
		if valueLength != NullBytesLength {
			headerValue, err = c.bytes(valueLength)
			if err != nil {
				return api.Record{}, 0, err
			}
		}
		record.Headers = append(record.Headers, api.Header{Name: string(nameBytes), Value: cloneBytes(headerValue)})
	}
	if c.pos != len(c.data) {
		return api.Record{}, 0, corrupt(errInvalidRecord, "trailing bytes in record")
	}
	return record, int(recordBytes), nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	clone := make([]byte, len(value))
	copy(clone, value)
	return clone
}

// EncodeBatch serializes one v1 batch from exactly one partition. Record
// offsets must be contiguous starting at BaseOffset.
func EncodeBatch(batch api.RecordBatch) ([]byte, error) {
	if batch.Topic.IsZero() || len(batch.Records) == 0 || len(batch.Records) > int(MaxBatchRecords) {
		return nil, invalidArgument(errInvalidBatch, "invalid topic or record count")
	}
	if !validOffsetRange(batch.BaseOffset, uint32(len(batch.Records))) {
		return nil, invalidArgument(errInvalidBatch, "batch offset range is outside v1 limits")
	}

	recordBytes := make([]byte, 0)
	maxTimestamp := int64(math.MinInt64)
	for index, record := range batch.Records {
		expectedOffset := batch.BaseOffset + uint64(index)
		if record.Topic != batch.Topic || record.Partition != batch.Partition || record.Offset != expectedOffset {
			return nil, invalidArgument(errInvalidBatch, "records do not match batch identity or order")
		}
		if record.Timestamp > maxTimestamp {
			maxTimestamp = record.Timestamp
		}
		encoded, err := EncodeRecord(record, batch.BaseOffset)
		if err != nil {
			return nil, err
		}
		recordBytes = append(recordBytes, encoded...)
	}
	total := uint64(BatchHeaderBytes) + uint64(len(recordBytes)) + BatchTrailerBytes
	if total > uint64(MaxBatchBytes) || total > uint64(math.MaxUint32) {
		return nil, errors.Join(api.ErrRecordTooLarge, errInvalidBatch)
	}

	data := make([]byte, total)
	copy(data[0:4], BatchMagic)
	binary.LittleEndian.PutUint16(data[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(data[6:8], BatchHeaderBytes)
	binary.LittleEndian.PutUint32(data[8:12], uint32(total))
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(batch.Records)))
	binary.LittleEndian.PutUint64(data[16:24], batch.BaseOffset)
	binary.LittleEndian.PutUint64(data[24:32], uint64(maxTimestamp))
	binary.LittleEndian.PutUint16(data[36:38], FormatVersion)
	binary.LittleEndian.PutUint32(data[44:48], CRC32C(data[:44]))
	copy(data[BatchHeaderBytes:total-BatchTrailerBytes], recordBytes)
	binary.LittleEndian.PutUint32(data[total-BatchTrailerBytes:], CRC32C(data[:total-BatchTrailerBytes]))
	return data, nil
}

// DecodeBatch validates and decodes one complete v1 batch. Returned payloads
// are owned by the caller and do not alias data.
func DecodeBatch(data []byte, topic api.TopicID, partition uint32) (api.RecordBatch, error) {
	if len(data) < int(BatchHeaderBytes) {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "incomplete batch header")
	}
	if string(data[0:4]) != BatchMagic {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch magic mismatch")
	}
	if binary.LittleEndian.Uint16(data[6:8]) != BatchHeaderBytes {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch header size mismatch")
	}
	if binary.LittleEndian.Uint32(data[44:48]) != CRC32C(data[:44]) {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch header checksum mismatch")
	}
	if binary.LittleEndian.Uint16(data[4:6]) != FormatVersion || binary.LittleEndian.Uint16(data[36:38]) != FormatVersion {
		return api.RecordBatch{}, unsupported("unsupported batch or record encoding version")
	}
	if binary.LittleEndian.Uint32(data[32:36]) != 0 || binary.LittleEndian.Uint16(data[38:40]) != 0 || binary.LittleEndian.Uint32(data[40:44]) != 0 {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "nonzero reserved or required batch attributes")
	}
	if topic.IsZero() {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch topic identity is zero")
	}

	total := binary.LittleEndian.Uint32(data[8:12])
	count := binary.LittleEndian.Uint32(data[12:16])
	baseOffset := binary.LittleEndian.Uint64(data[16:24])
	if count == 0 || count > MaxBatchRecords || total < uint32(BatchHeaderBytes)+BatchTrailerBytes+RecordPrefixBytes || total > MaxBatchBytes {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch length or record count outside v1 limits")
	}
	if !validOffsetRange(baseOffset, count) {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch offset range is outside v1 limits")
	}
	if uint64(count)*uint64(RecordPrefixBytes) > uint64(total)-uint64(BatchHeaderBytes)-BatchTrailerBytes {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch cannot contain its declared records")
	}
	if uint64(total) != uint64(len(data)) {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch has incomplete or trailing bytes")
	}
	trailerPosition := len(data) - BatchTrailerBytes
	if binary.LittleEndian.Uint32(data[trailerPosition:]) != CRC32C(data[:trailerPosition]) {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch checksum mismatch")
	}

	batch := api.RecordBatch{Topic: topic, Partition: partition, BaseOffset: baseOffset, Records: make([]api.Record, 0, count)}
	position := int(BatchHeaderBytes)
	var maxTimestamp int64 = math.MinInt64
	for index := uint32(0); index < count; index++ {
		if len(data)-BatchTrailerBytes-position < RecordPrefixBytes {
			return api.RecordBatch{}, corrupt(errInvalidBatch, "record prefix exceeds batch body")
		}
		recordLength := binary.LittleEndian.Uint32(data[position : position+4])
		if recordLength < RecordPrefixBytes || uint64(recordLength) > uint64(len(data)-BatchTrailerBytes-position) {
			return api.RecordBatch{}, corrupt(errInvalidBatch, "record length exceeds batch body")
		}
		expectedDelta := index
		record, consumed, err := decodeRecord(data[position:position+int(recordLength)], baseOffset, topic, partition, &expectedDelta)
		if err != nil {
			return api.RecordBatch{}, err
		}
		if consumed != int(recordLength) {
			return api.RecordBatch{}, corrupt(errInvalidBatch, "record decoder consumed unexpected bytes")
		}
		if record.Timestamp > maxTimestamp {
			maxTimestamp = record.Timestamp
		}
		batch.Records = append(batch.Records, record)
		position += int(recordLength)
	}
	if position != trailerPosition {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "trailing bytes after declared records")
	}
	if int64(binary.LittleEndian.Uint64(data[24:32])) != maxTimestamp {
		return api.RecordBatch{}, corrupt(errInvalidBatch, "batch maximum timestamp mismatch")
	}
	return batch, nil
}
