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

package api

import (
	"encoding/hex"
	"fmt"
	"time"
)

// TopicID is the opaque on-disk identity of a topic.
type TopicID [16]byte

// SegmentID is the opaque identity of one segment incarnation.
type SegmentID [16]byte

var (
	// ClusterMetadataTopicID is reserved for the cluster metadata system log.
	ClusterMetadataTopicID = TopicID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	// ConsumerOffsetsTopicID is reserved for the consumer-offsets system log.
	ConsumerOffsetsTopicID = TopicID{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
)

// IsZero reports whether the identifier is all zero bytes.
func (id TopicID) IsZero() bool { return id == TopicID{} }

// IsZero reports whether the identifier is all zero bytes.
func (id SegmentID) IsZero() bool { return id == SegmentID{} }

// String returns the canonical lower-case hexadecimal representation.
func (id TopicID) String() string { return hex.EncodeToString(id[:]) }

// String returns the canonical lower-case hexadecimal representation.
func (id SegmentID) String() string { return hex.EncodeToString(id[:]) }

// Header is an ordered key/value pair. Duplicate names and their order are
// meaningful; a map is deliberately not used.
type Header struct {
	Name  string
	Value []byte
}

// AppendRequest is the caller-owned input to a partition writer. Offset is
// intentionally absent: only the partition writer assigns offsets.
type AppendRequest struct {
	Topic     TopicID
	Partition uint32
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Record is the public, caller-owned representation of a delivered record.
type Record struct {
	Topic     TopicID
	Partition uint32
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// RecordBatch is one ordered batch from exactly one partition.
type RecordBatch struct {
	Topic      TopicID
	Partition  uint32
	BaseOffset uint64
	Records    []Record
}

// FetchOptions bounds one durable fetch result. A zero record or byte field
// selects the library's finite default. MaxWait is honored by managed Poll;
// raw Fetch remains non-waiting.
type FetchOptions struct {
	MaxRecords uint32
	MaxBytes   uint64
	MaxWait    time.Duration
}

// FetchResult is a caller-owned durable-record prefix. NextOffset is the next
// offset to fetch after Records; it equals the requested offset for an empty
// result or a limit error.
type FetchResult struct {
	Records    []Record
	NextOffset uint64
}

// GroupStart chooses the durable baseline for a partition's first assignment.
type GroupStart uint8

const (
	GroupStartEarliest GroupStart = 1
	GroupStartLatest   GroupStart = 2
	GroupStartExplicit GroupStart = 3
)

// ConsumerOptions configures a same-process durable consumer. Start is recorded
// when the group is created; later joins must use the same policy. ExplicitStart
// is used only with GroupStartExplicit.
type ConsumerOptions struct {
	Start         GroupStart
	ExplicitStart uint64
	// ProgressTimeout fences a local assignment unless an admitted Poll renews it.
	// Zero selects the library's 30-second default.
	ProgressTimeout time.Duration
	Fetch           FetchOptions
}

// TopicPartition identifies one explicitly subscribed consumer partition.
type TopicPartition struct {
	Topic     TopicID
	Partition uint32
}

// ExplicitStart supplies one immutable initial next offset for an explicitly
// started consumer group key.
type ExplicitStart struct {
	Topic     TopicID
	Partition uint32
	Next      uint64
}

// ConsumerGroupMember declares one local member's unique subscriptions. An
// OpenConsumerGroup call supplies the complete local membership snapshot;
// equivalent live requests are coalesced, while changed requests replace it.
type ConsumerGroupMember struct {
	Subscriptions []TopicPartition
}

// ConsumerGroupOptions configures a complete same-process membership snapshot.
// Equivalent options and membership reuse the current live snapshot. For
// GroupStartExplicit, ExplicitStarts must cover every subscribed key.
type ConsumerGroupOptions struct {
	Start           GroupStart
	ExplicitStarts  []ExplicitStart
	Fetch           FetchOptions
	ProgressTimeout time.Duration
}

func (id TopicID) GoString() string   { return fmt.Sprintf("TopicID(%q)", id.String()) }
func (id SegmentID) GoString() string { return fmt.Sprintf("SegmentID(%q)", id.String()) }
