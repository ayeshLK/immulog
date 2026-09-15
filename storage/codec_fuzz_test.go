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
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func FuzzDecodeBatch(f *testing.F) {
	topic := testTopic()
	encoded, err := EncodeBatch(api.RecordBatch{
		Topic: topic, Partition: 3, BaseOffset: 7,
		Records: []api.Record{{Topic: topic, Partition: 3, Offset: 7, Timestamp: 11, Value: []byte("seed")}},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte{})
	f.Add(make([]byte, BatchHeaderBytes-1))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeBatch(data, topic, 3)
	})
}

func FuzzDecodeSegmentHeader(f *testing.F) {
	topic := testTopic()
	encoded, err := EncodeSegmentHeader(SegmentHeader{
		Topic: topic, Partition: 3, BaseOffset: 7, ID: api.SegmentID{1},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte{})
	f.Add(make([]byte, SegmentHeaderBytes-1))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeSegmentHeader(data, topic, 3)
	})
}
