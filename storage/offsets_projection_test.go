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
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestOffsetsProjectionReplayAndSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := store.CreateTopic("orders", 1, PartitionOptions{BatchBytes: 4096, SegmentBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	key := topicKey{topic: descriptor.ID, partition: 0}
	var instance, session [16]byte
	instance[0], session[0] = 1, 2
	groupID := "orders-group"
	catalogNext := store.catalog.logEnd

	groupBody := append([]byte(nil), store.storeID[:]...)
	groupBody = appendU64(groupBody, catalogNext)
	groupBody, err = appendString(groupBody, groupID, maxGroupIDBytes)
	if err != nil {
		t.Fatal(err)
	}
	groupBody = append(groupBody, 1, 0, 0, 0)
	groupBody = appendU32(groupBody, 0)
	appendOffsetEvent(t, store.offsets, EventGroupCreated, groupBody)

	assignmentBody := append([]byte(nil), store.storeID[:]...)
	assignmentBody = appendU64(assignmentBody, catalogNext)
	assignmentBody, err = appendString(assignmentBody, groupID, maxGroupIDBytes)
	if err != nil {
		t.Fatal(err)
	}
	assignmentBody = appendID(assignmentBody, instance)
	assignmentBody = appendU64(assignmentBody, 0)
	assignmentBody = appendU64(assignmentBody, 1)
	assignmentBody = append(assignmentBody, 1, 0, 0, 0)
	assignmentBody = appendU32(assignmentBody, 1)
	assignmentBody = appendID(assignmentBody, session)
	assignmentBody = appendU32(assignmentBody, 1)
	assignmentBody = appendID(assignmentBody, [16]byte(key.topic))
	assignmentBody = appendU32(assignmentBody, key.partition)
	assignmentBody = appendU32(assignmentBody, 1)
	assignmentBody = appendID(assignmentBody, [16]byte(key.topic))
	assignmentBody = appendU32(assignmentBody, key.partition)
	assignmentBody = appendID(assignmentBody, session)
	assignmentBody = appendU64(assignmentBody, 0)
	assignmentBody = append(assignmentBody, 1, 0, 0, 0)
	assignmentBody = appendU64(assignmentBody, 0)
	appendOffsetEvent(t, store.offsets, EventLocalAssignmentChanged, assignmentBody)

	commitBody := append([]byte(nil), store.storeID[:]...)
	commitBody = appendU64(commitBody, catalogNext)
	commitBody, err = appendString(commitBody, groupID, maxGroupIDBytes)
	if err != nil {
		t.Fatal(err)
	}
	commitBody = appendID(commitBody, instance)
	commitBody = appendID(commitBody, session)
	commitBody = appendU64(commitBody, 1)
	commitBody = appendID(commitBody, [16]byte(key.topic))
	commitBody = appendU32(commitBody, key.partition)
	commitBody = append(commitBody, 0, 0, 0, 0)
	commitBody = appendU64(commitBody, 0)
	commitBody = appendU64(commitBody, 3)
	appendOffsetEvent(t, store.offsets, EventOffsetCommitted, commitBody)

	state, err := replayOffsetsHistory(store.offsets, store.storeID, store.catalogState)
	if err != nil {
		t.Fatal(err)
	}
	store.offsetsState = state
	if err := store.SaveSnapshots(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	group := store.offsetsState.groups[groupID]
	if group == nil || group.Generation != 1 || group.Progress[key].CommittedNext != 3 {
		t.Fatalf("recovered offsets projection = %#v", store.offsetsState)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, consumerOffsetsDir, snapshotPathName))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= snapshotHeaderBytes+snapshotTrailerBytes {
		t.Fatal("offset snapshot unexpectedly empty")
	}
}

func appendOffsetEvent(t *testing.T, partition *Partition, eventType EventType, body []byte) {
	t.Helper()
	value, err := encodeSystemEvent(eventType, body)
	if err != nil {
		t.Fatal(err)
	}
	base, err := partition.EndOffset()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendBatch(api.RecordBatch{
		Topic: api.ConsumerOffsetsTopicID, Partition: 0, BaseOffset: base,
		Records: []api.Record{{Topic: api.ConsumerOffsetsTopicID, Partition: 0, Offset: base, Value: value}},
	}); err != nil {
		t.Fatal(err)
	}
}
