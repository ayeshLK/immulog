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

// Package api defines the public immulog contracts.
package api

import "errors"

// Domain errors are stable values callers can test with errors.Is.
var (
	ErrUnknownTopic           = errors.New("immulog: unknown topic")
	ErrOffsetOutOfRange       = errors.New("immulog: offset out of range")
	ErrPartitionUnavailable   = errors.New("immulog: partition unavailable")
	ErrAppendOutcomeUnknown   = errors.New("immulog: append outcome unknown")
	ErrDataDirLocked          = errors.New("immulog: data directory locked")
	ErrLockUnsupported        = errors.New("immulog: data-directory locking unsupported")
	ErrTopicExists            = errors.New("immulog: topic already exists")
	ErrMetadataUnavailable    = errors.New("immulog: metadata unavailable")
	ErrMetadataOutcomeUnknown = errors.New("immulog: metadata outcome unknown")
	ErrAssignmentLost         = errors.New("immulog: assignment lost")
	ErrGroupUnavailable       = errors.New("immulog: consumer group unavailable")
	ErrCommitOutcomeUnknown   = errors.New("immulog: commit outcome unknown")
	ErrCommitRegression       = errors.New("immulog: commit regression")
	ErrInvalidCommit          = errors.New("immulog: invalid commit")
	ErrBackpressure           = errors.New("immulog: backpressure")
	ErrRecordTooLarge         = errors.New("immulog: record too large")
	ErrFetchLimitTooSmall     = errors.New("immulog: fetch limit too small")
	ErrResourceLimit          = errors.New("immulog: resource limit")
	ErrSystemLogCapacity      = errors.New("immulog: system log capacity")
	ErrDiskPressure           = errors.New("immulog: disk pressure")
	ErrClosing                = errors.New("immulog: closing")
	ErrClosed                 = errors.New("immulog: closed")
	ErrCloseIncomplete        = errors.New("immulog: close incomplete")
	ErrConcurrentOperation    = errors.New("immulog: concurrent operation")
	ErrCorruptLog             = errors.New("immulog: corrupt log")
	ErrUnsupportedFormat      = errors.New("immulog: unsupported format")
	ErrInvalidArgument        = errors.New("immulog: invalid argument")
)
