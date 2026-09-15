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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestCorruptSegmentMatrixPreservesRefusedRecoveryBytes(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func([]byte) []byte
		wantError error
	}{
		{
			name:      "segment-magic",
			mutate:    func(data []byte) []byte { data[0] ^= 1; return data },
			wantError: api.ErrCorruptLog,
		},
		{
			name:      "segment-header-crc",
			mutate:    func(data []byte) []byte { data[SegmentHeaderBytes-1] ^= 1; return data },
			wantError: api.ErrCorruptLog,
		},
		{
			name:      "partial-segment-header",
			mutate:    func(data []byte) []byte { return data[:SegmentHeaderBytes-1] },
			wantError: api.ErrCorruptLog,
		},
		{
			name:      "batch-crc",
			mutate:    func(data []byte) []byte { data[len(data)-1] ^= 1; return data },
			wantError: api.ErrCorruptLog,
		},
		{
			name: "batch-version",
			mutate: func(data []byte) []byte {
				batchStart := int(SegmentHeaderBytes)
				binary.LittleEndian.PutUint16(data[batchStart+4:batchStart+6], FormatVersion+1)
				binary.LittleEndian.PutUint32(data[batchStart+44:batchStart+48], CRC32C(data[batchStart:batchStart+44]))
				binary.LittleEndian.PutUint32(data[len(data)-4:], CRC32C(data[:len(data)-4]))
				return data
			},
			wantError: api.ErrUnsupportedFormat,
		},
		{
			name: "batch-count-zero",
			mutate: func(data []byte) []byte {
				binary.LittleEndian.PutUint32(data[SegmentHeaderBytes+12:SegmentHeaderBytes+16], 0)
				return data
			},
			wantError: api.ErrCorruptLog,
		},
		{
			name:      "unrecognized-tail",
			mutate:    func(data []byte) []byte { return append(data, 0xa5) },
			wantError: api.ErrCorruptLog,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, logPath, original := createCorruptionFixture(t)
			mutated := test.mutate(append([]byte(nil), original...))
			if err := os.WriteFile(logPath, mutated, 0o644); err != nil {
				t.Fatal(err)
			}
			store, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.OpenPartition(testTopic(), 0, PartitionOptions{})
			if err == nil || !errors.Is(err, test.wantError) {
				_ = store.Close()
				t.Fatalf("corrupt reopen error = %v, want %v", err, test.wantError)
			}
			if closeErr := store.Close(); closeErr != nil {
				t.Fatalf("close after refused recovery = %v", closeErr)
			}
			after, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, mutated) {
				t.Fatalf("refused recovery changed log bytes: before=%x after=%x", mutated, after)
			}
		})
	}
}

func createCorruptionFixture(t *testing.T) (string, string, []byte) {
	t.Helper()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	partition, err := store.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(testBatch(testTopic(), 0, 0, "corruption-fixture")); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "topics", testTopic().String(), "0", "00000000000000000000.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	return dir, logPath, data
}
