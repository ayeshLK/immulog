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
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func FuzzPreflightSystemLogSegment(f *testing.F) {
	header, err := EncodeSegmentHeader(SegmentHeader{
		Topic: api.ClusterMetadataTopicID, Partition: 0, ID: api.SegmentID{1},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(header)
	f.Add([]byte{})
	f.Add(append(append([]byte(nil), header...), 1))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "00000000000000000000.log")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		before := append([]byte(nil), data...)
		_, _, _ = preflightSegmentChain(dir, api.ClusterMetadataTopicID, 0, systemPartitionConfig().SegmentMaxBytes, 0, true, nil)
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(after, before) {
			t.Fatal("recovery preflight modified the authoritative segment")
		}
	})
}
