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
	"reflect"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func testTopic() api.TopicID {
	return api.TopicID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
}

func TestCRC32CVector(t *testing.T) {
	if got, want := CRC32C([]byte("123456789")), uint32(0xe3069283); got != want {
		t.Fatalf("CRC32C = %#x, want %#x", got, want)
	}
	encoded := make([]byte, 4)
	binary.LittleEndian.PutUint32(encoded, CRC32C([]byte("123456789")))
	if got, want := encoded, []byte{0x83, 0x92, 0x06, 0xe3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("encoded CRC32C = %x, want %x", got, want)
	}
}

func TestSegmentHeaderRoundTrip(t *testing.T) {
	want := SegmentHeader{Topic: testTopic(), Partition: 7, BaseOffset: 42, ID: api.SegmentID{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}}
	encoded, err := EncodeSegmentHeader(want)
	if err != nil {
		t.Fatal(err)
	}
	if got, wantBytes := len(encoded), int(SegmentHeaderBytes); got != wantBytes {
		t.Fatalf("segment header length = %d, want %d", got, wantBytes)
	}
	got, err := DecodeSegmentHeader(encoded, want.Topic, want.Partition)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded header = %#v, want %#v", got, want)
	}
}

func TestBatchRoundTripPreservesNilEmptyAndOrderedHeaders(t *testing.T) {
	topic := testTopic()
	want := api.RecordBatch{
		Topic: topic, Partition: 3, BaseOffset: 100,
		Records: []api.Record{
			{Topic: topic, Partition: 3, Offset: 100, Timestamp: -7, Key: nil, Value: []byte{}, Headers: []api.Header{}},
			{Topic: topic, Partition: 3, Offset: 101, Timestamp: 9, Key: []byte("k"), Value: []byte("value"), Headers: []api.Header{
				{Name: "x", Value: nil}, {Name: "x", Value: []byte{}}, {Name: "y", Value: []byte{0, 1, 2}},
			}},
		},
	}

	encoded, err := EncodeBatch(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBatch(encoded, topic, want.Partition)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded batch = %#v, want %#v", got, want)
	}
	if got.Records[0].Value == nil || got.Records[1].Headers[1].Value == nil {
		t.Fatal("empty present byte fields lost their distinction from nil")
	}
}

func TestDecodeBatchRejectsCorruptionAndUnsupportedVersions(t *testing.T) {
	topic := testTopic()
	encoded, err := EncodeBatch(api.RecordBatch{Topic: topic, Partition: 0, BaseOffset: 0, Records: []api.Record{{Topic: topic, Offset: 0, Timestamp: 1, Value: []byte("ok")}}})
	if err != nil {
		t.Fatal(err)
	}

	corruptBody := append([]byte(nil), encoded...)
	corruptBody[len(corruptBody)-5] ^= 0x80
	if _, err := DecodeBatch(corruptBody, topic, 0); !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("body corruption error = %v, want ErrCorruptLog", err)
	}

	unsupportedVersion := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint16(unsupportedVersion[4:6], 2)
	binary.LittleEndian.PutUint32(unsupportedVersion[44:48], CRC32C(unsupportedVersion[:44]))
	if _, err := DecodeBatch(unsupportedVersion, topic, 0); !errors.Is(err, api.ErrUnsupportedFormat) {
		t.Fatalf("unsupported version error = %v, want ErrUnsupportedFormat", err)
	}

	badHeader := append([]byte(nil), encoded...)
	badHeader[44] ^= 0x01
	if _, err := DecodeBatch(badHeader, topic, 0); !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("header corruption error = %v, want ErrCorruptLog", err)
	}
}

func TestDecodeBatchRejectsNoncontiguousRecords(t *testing.T) {
	topic := testTopic()
	encoded, err := EncodeBatch(api.RecordBatch{Topic: topic, Partition: 0, BaseOffset: 4, Records: []api.Record{
		{Topic: topic, Offset: 4, Timestamp: 1, Value: []byte("a")},
		{Topic: topic, Offset: 5, Timestamp: 2, Value: []byte("b")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// The second record's delta is at the beginning of the second record body.
	firstLength := int(binary.LittleEndian.Uint32(encoded[BatchHeaderBytes : BatchHeaderBytes+4]))
	secondDelta := int(BatchHeaderBytes) + firstLength + 4
	binary.LittleEndian.PutUint32(encoded[secondDelta:secondDelta+4], 7)
	// Recompute the enclosing checksum so the semantic error is reached.
	binary.LittleEndian.PutUint32(encoded[len(encoded)-4:], CRC32C(encoded[:len(encoded)-4]))
	if _, err := DecodeBatch(encoded, topic, 0); !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("noncontiguous delta error = %v, want ErrCorruptLog", err)
	}
}

func TestEncodeBatchRejectsInvalidOrderAndHeaderNames(t *testing.T) {
	topic := testTopic()
	_, err := EncodeBatch(api.RecordBatch{Topic: topic, Records: []api.Record{{Topic: topic, Offset: 2}}})
	if !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("invalid order error = %v, want ErrInvalidArgument", err)
	}

	_, err = EncodeBatch(api.RecordBatch{Topic: topic, Records: []api.Record{{Topic: topic, Offset: 0, Headers: []api.Header{{Name: string([]byte{0xff})}}}}})
	if !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("invalid header name error = %v, want ErrInvalidArgument", err)
	}
}
