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
	"errors"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestRetentionEventReplayValidatesCoverageAndAnchor(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("retained", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	topic := store.catalogState.topicsByID[descriptor.ID]
	topic.descriptor.Partitions[0].Config.RetentionMask = 1
	partition := topic.descriptor.Partitions[0]
	var successor api.SegmentID
	successor[0] = 9
	var successorHash [32]byte
	successorHash[0] = 8
	event := partitionLogStartAdvancedEvent{
		StoreID: store.storeID, ExpectedCatalogNext: store.catalog.logEnd,
		Key: topicKey{topic: descriptor.ID, partition: 0}, ExpectedOldL: 0,
		NewL: 1, EvaluatedDurableEnd: 1, ReasonMask: 1,
		RetainedAnchor: eventAnchor{ID: successor, HeaderHash: successorHash},
		Retired:        []retiredSegmentEvent{{Base: 0, End: 1, Bytes: uint64(SegmentHeaderBytes), Anchor: eventAnchor{ID: partition.InitialSegment, HeaderHash: partition.InitialHeaderHash}}},
	}
	body, err := encodePartitionLogStartAdvancedPayload(event)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeSystemEvent(EventPartitionLogStartAdvanced, body)
	if err != nil {
		t.Fatal(err)
	}
	record := api.Record{Topic: api.ClusterMetadataTopicID, Partition: 0, Offset: store.catalog.logEnd, Value: encoded}
	if err := store.catalogState.apply(record); err != nil {
		t.Fatal(err)
	}
	updated := store.catalogState.topicsByID[descriptor.ID].descriptor.Partitions[0]
	if updated.RetainedL != 1 || updated.CurrentRetainedSegment != successor || updated.BoundaryCatalogNext != record.Offset+1 {
		t.Fatalf("retention state = %#v", updated)
	}

	bad := event
	bad.Retired = []retiredSegmentEvent{{Base: 1, End: 2, Bytes: 1, Anchor: event.RetainedAnchor}}
	badBody, err := encodePartitionLogStartAdvancedPayload(bad)
	if err != nil {
		t.Fatal(err)
	}
	badRecord := record
	badRecord.Value, err = encodeSystemEvent(EventPartitionLogStartAdvanced, badBody)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.catalogState.apply(badRecord); err == nil || !errors.Is(err, api.ErrCorruptLog) {
		t.Fatalf("invalid retention replay error = %v", err)
	}
}
