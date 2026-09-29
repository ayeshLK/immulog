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

package api_test

import (
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestReservedTopicIDsReturnCanonicalCopies(t *testing.T) {
	tests := []struct {
		name string
		get  func() api.TopicID
		last byte
	}{
		{name: "cluster metadata", get: api.ClusterMetadataTopicID, last: 1},
		{name: "consumer offsets", get: api.ConsumerOffsetsTopicID, last: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var want api.TopicID
			want[len(want)-1] = test.last
			got := test.get()
			if got != want {
				t.Fatalf("reserved topic ID = %v, want %v", got, want)
			}
			got[0] = 0xff
			got[len(got)-1] = 0xff
			if next := test.get(); next != want {
				t.Fatalf("reserved topic ID after caller mutation = %v, want %v", next, want)
			}
		})
	}
}
