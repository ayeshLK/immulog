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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayeshLK/immulog/api"
)

const defaultSegmentBytes = uint64(SegmentHeaderBytes) + uint64(MaxBatchBytes)

// Writer limits are finite operating settings; zero selects the documented
// baseline profile. BatchLinger bounds how long ingress waits to collect
// newly published requests after the first request is available.
// PartitionOptions controls the bounded segment size and initial retained
// offset for a newly created partition. Existing files determine recovery
// state; options do not rewrite them.
type PartitionOptions struct {
	SegmentBytes     uint64
	InitialOffset    uint64
	IndexStride      uint64
	BatchBytes       uint32
	BatchRecords     uint32
	RecordBytes      uint32
	InFlightBytes    uint64
	InFlightRecords  uint32
	AdmissionWaiters uint32
	BatchLinger      time.Duration
	// Retention is persisted for catalog-owned user topics. Enabled zero limits
	// expire eligible closed segments on the next maintenance pass.
	RetentionTimeEnabled bool
	RetentionSizeEnabled bool
	RetentionDuration    time.Duration
	RetentionBytes       uint64
	MaxSegmentAge        time.Duration
	RetentionCheck       time.Duration
	// TailSlots and TailBytes bound the optional in-memory durable tail cache.
	// They are runtime-only and are not recorded in PartitionConfigV1.
	TailSlots uint32
	TailBytes uint64
}

func (options *PartitionOptions) normalize() {
	if options.SegmentBytes == 0 {
		options.SegmentBytes = defaultSegmentBytes
	}
	if options.IndexStride == 0 {
		options.IndexStride = DefaultIndexStride
	}
	options.normalizeWriter()
}

// Partition is a local append-only partition engine. Concurrent producers enter
// its bounded ingress ring; exactly one terminal handler performs durable writes.
type Partition struct {
	mu        sync.RWMutex
	store     *Store
	dir       string
	topic     api.TopicID
	partition uint32
	options   PartitionOptions
	storeID   StoreID
	tail      *partitionTail
	segments  []*segment
	logEnd    uint64
	// prefixDigest caches the running projection-prefix hash of a reserved
	// system log. It stays nil for user partitions, which are never snapshotted.
	prefixDigest *prefixDigestState
	closed       bool
	closing      bool
	fetchWake    chan struct{}
	fetchWaiters uint32
	// ingress is a bounded, multi-producer ring with one terminal writer.
	ingress              *partitionIngress
	publicationCredits   uint32
	publishing           uint32
	publisherWake        chan struct{}
	queueMu              sync.Mutex
	queue                []*appendRequest
	admittedBytes        uint64
	admittedRecords      uint32
	waiting              uint32
	queueWake            chan struct{}
	spaceWake            chan struct{}
	closingSignal        chan struct{}
	writerDone           chan struct{}
	closeDone            chan struct{}
	queueClosing         bool
	queueClosed          bool
	writerUnavailable    bool
	closeErr             error
	unavailable          bool
	appendAcked          atomic.Uint64
	appendKnownUnwritten atomic.Uint64
	appendUnknown        atomic.Uint64
	admissionWaits       atomic.Uint64
	admissionWaitNanos   atomic.Uint64
	rejectedBackpressure atomic.Uint64
	rejectedClosing      atomic.Uint64
	rejectedClosed       atomic.Uint64
	rejectedUnavailable  atomic.Uint64
	rejectedDiskPressure atomic.Uint64
	rejectedCanceled     atomic.Uint64
	rejectedResource     atomic.Uint64
	rejectedOther        atomic.Uint64
	writeNanos           atomic.Uint64
	syncNanos            atomic.Uint64
	namespaceNanos       atomic.Uint64
	writeOps             atomic.Uint64
	syncOps              atomic.Uint64
	namespaceOps         atomic.Uint64
	writeBuckets         [latencyBucketCount]atomic.Uint64
	syncBuckets          [latencyBucketCount]atomic.Uint64
	namespaceBuckets     [latencyBucketCount]atomic.Uint64
}

type segment struct {
	path        string
	file        *os.File
	header      SegmentHeader
	end         uint64
	size        int64
	records     uint64
	maxTime     int64
	headerHash  [32]byte
	batches     []batchInfo
	offsetIndex []offsetIndexEntry
	timeIndex   []timeIndexEntry
	// indexDirty marks an in-memory index that the persisted sidecars do not
	// reflect yet. A sealed segment is checkpointed once when it rolls, so only
	// the active segment and unreadable sidecars need work at close.
	indexDirty bool
}

func openPartition(dir string, topic api.TopicID, partition uint32, options PartitionOptions, storeID StoreID) (*Partition, error) {
	tail, err := newPartitionTail(options)
	if err != nil {
		return nil, err
	}
	files, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}
	p := &Partition{dir: dir, topic: topic, partition: partition, options: options, storeID: storeID, tail: tail}
	if len(files) == 0 {
		created, err := createSegment(dir, topic, partition, options.InitialOffset)
		if err != nil {
			return nil, err
		}
		p.segments = []*segment{created}
		loadOrBuildIndexes(created, storeID, options.IndexStride)
		p.logEnd = options.InitialOffset
		if err := p.startWriter(); err != nil {
			_ = fileClose(created.file)
			return nil, err
		}
		return p, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].base < files[j].base })
	var expected uint64
	for index, file := range files {
		if index == 0 {
			expected = file.base
			if expected != options.InitialOffset {
				return nil, corrupt(errInvalidSegment, fmt.Sprintf("first segment base %d does not match initial offset %d", expected, options.InitialOffset))
			}
		} else if file.base != expected {
			return nil, corrupt(errInvalidSegment, fmt.Sprintf("segment base %d does not follow offset %d", file.base, expected))
		}
		opened, next, err := recoverSegment(file.path, topic, partition, index == len(files)-1, expected)
		if err != nil {
			for _, prior := range p.segments {
				if prior.file != nil {
					_ = fileClose(prior.file)
				}
			}
			return nil, err
		}
		p.segments = append(p.segments, opened)
		loadOrBuildIndexes(opened, storeID, options.IndexStride)
		expected = next
	}
	p.logEnd = expected
	// Recovery opens every segment to rebuild its batch map. Only the active
	// segment needs a lasting handle; sealed segments are reopened on demand
	// through the store's bounded descriptor cache.
	if err := p.releaseSealedHandles(); err != nil {
		for _, segment := range p.segments {
			if segment.file != nil {
				_ = fileClose(segment.file)
			}
		}
		return nil, err
	}
	if err := p.startWriter(); err != nil {
		for _, segment := range p.segments {
			if segment.file != nil {
				_ = fileClose(segment.file)
			}
		}
		return nil, err
	}
	return p, nil
}

type discoveredSegment struct {
	path string
	base uint64
}

func discoverSegments(dir string) ([]discoveredSegment, error) {
	entries, err := fsReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read partition directory: %w", err)
	}
	seen := make(map[uint64]struct{})
	files := make([]discoveredSegment, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".log")
		if len(name) != 20 {
			return nil, corrupt(errInvalidSegment, "noncanonical segment filename "+entry.Name())
		}
		for _, character := range name {
			if character < '0' || character > '9' {
				return nil, corrupt(errInvalidSegment, "noncanonical segment filename "+entry.Name())
			}
		}
		base, err := strconv.ParseUint(name, 10, 64)
		if err != nil || base > maxOffset {
			return nil, corrupt(errInvalidSegment, "segment filename offset outside v1 range")
		}
		if _, exists := seen[base]; exists {
			return nil, corrupt(errInvalidSegment, "duplicate segment base")
		}
		seen[base] = struct{}{}
		files = append(files, discoveredSegment{path: filepath.Join(dir, entry.Name()), base: base})
	}
	return files, nil
}

func createSegment(dir string, topic api.TopicID, partition uint32, base uint64) (*segment, error) {
	var id api.SegmentID
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("generate segment ID: %w", err)
	}
	if id.IsZero() {
		id[15] = 1
	}
	header := SegmentHeader{Topic: topic, Partition: partition, BaseOffset: base, ID: id}
	encoded, err := EncodeSegmentHeader(header)
	if err != nil {
		return nil, err
	}
	finalPath := filepath.Join(dir, fmt.Sprintf("%020d.log", base))
	temporaryPath := filepath.Join(dir, ".segment-"+id.String()+".tmp")
	file, err := fsOpenFile(temporaryPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create temporary segment: %w", err)
	}
	if count, writeErr := fileWrite(file, encoded); writeErr != nil || count != len(encoded) {
		_ = fileClose(file)
		return nil, errors.Join(writeErr, io.ErrShortWrite)
	}
	if err := fileSync(file); err != nil {
		_ = fileClose(file)
		return nil, fmt.Errorf("sync segment header: %w", err)
	}
	if err := fileClose(file); err != nil {
		return nil, fmt.Errorf("close temporary segment: %w", err)
	}
	if _, err := fsStat(finalPath); err == nil {
		return nil, corrupt(errInvalidSegment, "segment path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("check segment path: %w", err)
	}
	if err := fsRename(temporaryPath, finalPath); err != nil {
		return nil, fmt.Errorf("publish segment header: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return nil, fmt.Errorf("sync segment directory: %w", err)
	}
	file, err = fsOpenFile(finalPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open published segment: %w", err)
	}
	// A fresh segment has no sidecars on disk yet.
	return &segment{path: finalPath, file: file, header: header, end: base, size: int64(SegmentHeaderBytes), maxTime: math.MinInt64, headerHash: segmentHeaderHash(encoded), indexDirty: true}, nil
}

func recoverSegment(path string, topic api.TopicID, partition uint32, final bool, expected uint64) (*segment, uint64, error) {
	file, err := fsOpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("open segment %q: %w", path, err)
	}
	stat, err := fileStat(file)
	if err != nil {
		_ = fileClose(file)
		return nil, 0, fmt.Errorf("stat segment %q: %w", path, err)
	}
	if stat.Size() < int64(SegmentHeaderBytes) {
		_ = fileClose(file)
		return nil, 0, corrupt(errInvalidSegment, "segment is shorter than its header")
	}
	data, err := readFile(file, stat.Size())
	if err != nil {
		_ = fileClose(file)
		return nil, 0, fmt.Errorf("read segment %q: %w", path, err)
	}
	header, err := DecodeSegmentHeader(data[:SegmentHeaderBytes], topic, partition)
	if err != nil {
		_ = fileClose(file)
		return nil, 0, fmt.Errorf("validate segment %q: %w", path, err)
	}
	wantName := fmt.Sprintf("%020d.log", header.BaseOffset)
	if filepath.Base(path) != wantName || header.BaseOffset != expected {
		_ = fileClose(file)
		return nil, 0, corrupt(errInvalidSegment, "segment header/path/order mismatch")
	}
	current := header.BaseOffset
	position := int(SegmentHeaderBytes)
	var records uint64
	maxTime := int64(math.MinInt64)
	batches := make([]batchInfo, 0)
	for position < len(data) {
		remaining := len(data) - position
		if remaining < int(BatchHeaderBytes) {
			_ = fileClose(file)
			return nil, 0, corrupt(errInvalidBatch, "partial batch header")
		}
		batchLength := binary.LittleEndian.Uint32(data[position+8 : position+12])
		if uint64(batchLength) > uint64(remaining) {
			if final && canTruncateTail(data[position:], current) {
				if err := fileTruncate(file, int64(position)); err != nil {
					_ = fileClose(file)
					return nil, 0, fmt.Errorf("truncate incomplete final batch: %w", err)
				}
				if err := fileSync(file); err != nil {
					_ = fileClose(file)
					return nil, 0, fmt.Errorf("sync recovered segment: %w", err)
				}
				data = data[:position]
				break
			}
			_ = fileClose(file)
			return nil, 0, corrupt(errInvalidBatch, "incomplete batch body")
		}
		if batchLength < uint32(BatchHeaderBytes)+BatchTrailerBytes || position+int(batchLength) > len(data) {
			_ = fileClose(file)
			return nil, 0, corrupt(errInvalidBatch, "invalid batch length")
		}
		batch, err := DecodeBatch(data[position:position+int(batchLength)], topic, partition)
		if err != nil {
			_ = fileClose(file)
			return nil, 0, fmt.Errorf("validate batch at %q:%d: %w", path, position, err)
		}
		if batch.BaseOffset != current {
			_ = fileClose(file)
			return nil, 0, corrupt(errInvalidBatch, "batch base offset is not the expected next offset")
		}
		current += uint64(len(batch.Records))
		records += uint64(len(batch.Records))
		for _, record := range batch.Records {
			if record.Timestamp > maxTime {
				maxTime = record.Timestamp
			}
		}
		batches = append(batches, batchInfo{base: batch.BaseOffset, position: int64(position), bytes: batchLength, records: uint32(len(batch.Records)), maxTime: int64(binary.LittleEndian.Uint64(data[position+24 : position+32]))})
		position += int(batchLength)
	}
	if !final && records == 0 {
		_ = fileClose(file)
		return nil, 0, corrupt(errInvalidSegment, "non-final segment is empty")
	}
	return &segment{path: path, file: file, header: header, end: current, size: int64(len(data)), records: records, maxTime: maxTime, headerHash: segmentHeaderHash(data[:SegmentHeaderBytes]), batches: batches}, current, nil
}

func canTruncateTail(data []byte, expectedOffset uint64) bool {
	if len(data) < int(BatchHeaderBytes) || string(data[:4]) != BatchMagic || binary.LittleEndian.Uint16(data[6:8]) != BatchHeaderBytes {
		return false
	}
	if binary.LittleEndian.Uint32(data[44:48]) != CRC32C(data[:44]) {
		return false
	}
	if binary.LittleEndian.Uint16(data[4:6]) != FormatVersion || binary.LittleEndian.Uint16(data[36:38]) != FormatVersion || binary.LittleEndian.Uint32(data[32:36]) != 0 || binary.LittleEndian.Uint16(data[38:40]) != 0 || binary.LittleEndian.Uint32(data[40:44]) != 0 {
		return false
	}
	total := binary.LittleEndian.Uint32(data[8:12])
	count := binary.LittleEndian.Uint32(data[12:16])
	base := binary.LittleEndian.Uint64(data[16:24])
	return total >= uint32(BatchHeaderBytes)+BatchTrailerBytes+RecordPrefixBytes && total <= MaxBatchBytes && count > 0 && count <= MaxBatchRecords && validOffsetRange(base, count) && base == expectedOffset && uint64(total) > uint64(len(data))
}

func readFile(file *os.File, size int64) ([]byte, error) {
	if size < 0 || uint64(size) > uint64(int(^uint(0)>>1)) {
		return nil, errors.New("segment size cannot be represented in memory")
	}
	data := make([]byte, int(size))
	count, err := fileReadAt(file, data, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if count != len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

// readSegmentFile reads a whole segment through the bounded descriptor cache
// rather than assuming segment.file is a live handle, which only the active
// segment keeps.
func readSegmentFile(p *Partition, target *segment) ([]byte, error) {
	file, release, err := p.acquireSegmentFile(target)
	if err != nil {
		return nil, err
	}
	defer release()
	return readFile(file, target.size)
}

// AppendBatch durably appends a complete, contiguous batch and returns its
// first offset. It is the direct reference writer used before ingress rings.
//
// The store-wide closing flag fences new user work, but system partitions
// (__cluster_metadata and __consumer_offsets) must settle already-reserved
// control commands after Close begins. Their own queueClosing gate stops new
// system work once Close reaches their teardown (§7.2, §8.3).
func (p *Partition) AppendBatch(batch api.RecordBatch) (uint64, error) {
	if p.store != nil && p.store.closing.Load() && !p.isSystemPartition() {
		err := api.ErrClosing
		p.recordAdmissionOutcome(err)
		return 0, err
	}
	p.queueMu.Lock()
	queueClosing, queueClosed := p.queueClosing, p.queueClosed
	p.queueMu.Unlock()
	if queueClosed {
		err := api.ErrClosed
		p.recordAdmissionOutcome(err)
		return 0, err
	}
	if queueClosing {
		err := api.ErrClosing
		p.recordAdmissionOutcome(err)
		return 0, err
	}
	estimated, err := batchEncodedBytes(batch.Records)
	if err != nil {
		return 0, err
	}
	if estimated > uint64(p.options.BatchBytes) {
		return 0, errors.Join(api.ErrRecordTooLarge, errors.New("batch exceeds configured writer limit"))
	}
	encoded, err := EncodeBatch(batch)
	if err != nil {
		return 0, err
	}
	diskBytes, diskInodes, err := estimateDiskBatchGrowth(uint64(len(encoded)))
	if err != nil {
		return 0, err
	}
	disk, err := p.reserveDisk(diskBytes, diskInodes)
	if err != nil {
		p.recordAdmissionOutcome(err)
		return 0, err
	}
	defer disk.release()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		err := api.ErrClosed
		p.recordAdmissionOutcome(err)
		return 0, err
	}
	if p.closing {
		p.mu.Unlock()
		err := api.ErrClosing
		p.recordAdmissionOutcome(err)
		return 0, err
	}
	offset, publication, err := p.appendEncodedLocked(batch, encoded)
	p.mu.Unlock()
	p.publishTail(publication)
	p.recordAppendOutcome(err)
	return offset, err
}

// EndOffset returns H, the next offset after the durable records.
func (p *Partition) EndOffset() (uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0, api.ErrClosed
	}
	if p.closing {
		return 0, api.ErrClosing
	}
	if p.unavailable {
		return 0, api.ErrPartitionUnavailable
	}
	return p.logEnd, nil
}

// Read uses a validated sparse hint, then scans and validates authoritative
// batches from that hint through the requested result.
func (p *Partition) Read(offset uint64, maxRecords uint32) ([]api.Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, api.ErrClosed
	}
	if p.closing {
		return nil, api.ErrClosing
	}
	if p.unavailable {
		return nil, api.ErrPartitionUnavailable
	}
	if len(p.segments) == 0 || offset < p.segments[0].header.BaseOffset {
		return nil, api.ErrOffsetOutOfRange
	}
	if offset > p.logEnd {
		return nil, api.ErrOffsetOutOfRange
	}
	if offset == p.logEnd {
		return nil, nil
	}
	if maxRecords == 0 {
		maxRecords = MaxBatchRecords
	}
	result := make([]api.Record, 0)
	for _, segment := range p.segments {
		if offset >= segment.end {
			continue
		}
		data, err := readSegmentFile(p, segment)
		if err != nil {
			return nil, fmt.Errorf("read segment %q: %w", segment.path, err)
		}
		position := int(SegmentHeaderBytes)
		for _, hint := range segment.offsetIndex {
			if hint.base > offset {
				break
			}
			position = int(hint.position)
		}
		for position < len(data) {
			if len(data)-position < int(BatchHeaderBytes) {
				return nil, corrupt(errInvalidBatch, "partial batch header during read")
			}
			length := binary.LittleEndian.Uint32(data[position+8 : position+12])
			if length < uint32(BatchHeaderBytes)+BatchTrailerBytes || uint64(length) > uint64(len(data)-position) {
				return nil, corrupt(errInvalidBatch, "invalid batch length during read")
			}
			batch, err := DecodeBatch(data[position:position+int(length)], p.topic, p.partition)
			if err != nil {
				return nil, err
			}
			position += int(length)
			for _, record := range batch.Records {
				if record.Offset < offset {
					continue
				}
				if uint32(len(result)) >= maxRecords {
					return result, nil
				}
				result = append(result, record)
			}
		}
	}
	return result, nil
}

// Close drains admitted appends, then closes segment handles and publishes the
// closed state. It is idempotent and safe for concurrent callers.
func (p *Partition) Close() error {
	return p.closeWriter()
}
