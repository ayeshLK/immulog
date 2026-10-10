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

// Command history creates one controlled durable history and measures a cold
// Store.Open. It is intentionally an opt-in evidence tool, not a test: use a
// fresh data directory for every invocation and preserve the JSON output.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

type report struct {
	Version             int    `json:"version"`
	DataDir             string `json:"data_dir"`
	RecordsRequested    uint64 `json:"records_requested"`
	RecordsWritten      uint64 `json:"records_written"`
	Partitions          uint32 `json:"partitions"`
	SegmentBytes        uint64 `json:"segment_bytes"`
	BatchRecords        uint32 `json:"batch_records"`
	CatalogTopics       uint32 `json:"catalog_topics"`
	ConsumerCommits     uint64 `json:"consumer_commits"`
	Snapshots           bool   `json:"snapshots"`
	SnapshotInvalidated bool   `json:"snapshot_invalidated"`
	Segments            uint64 `json:"segments"`
	LogBytes            uint64 `json:"log_bytes"`
	OpenNanos           uint64 `json:"open_nanos"`
	HeapAllocBefore     uint64 `json:"heap_alloc_before"`
	HeapAllocAfter      uint64 `json:"heap_alloc_after"`
	HeapAllocDelta      int64  `json:"heap_alloc_delta"`
	HeapInuseAfter      uint64 `json:"heap_inuse_after"`
	RSSBefore           uint64 `json:"rss_before"`
	RSSAfter            uint64 `json:"rss_after"`
	SnapshotDiagnostics string `json:"snapshot_diagnostics"`
}

func main() {
	var (
		dir                string
		records            uint64
		partitions         uint
		segmentBytes       uint64
		batchRecords       uint
		catalogTopics      uint
		consumerCommits    uint64
		withSnapshots      bool
		invalidateSnapshot bool
		output             string
	)
	flag.StringVar(&dir, "data-dir", "", "fresh directory for the generated history (required)")
	flag.Uint64Var(&records, "records", 10000, "records per partition")
	flag.UintVar(&partitions, "partitions", 1, "number of partitions")
	flag.Uint64Var(&segmentBytes, "segment-bytes", 64<<10, "maximum segment size")
	flag.UintVar(&batchRecords, "batch-records", 32, "records per append batch")
	flag.UintVar(&catalogTopics, "catalog-topics", 1, "total catalog topics, including the data-bearing topic")
	flag.Uint64Var(&consumerCommits, "consumer-commits", 0, "durable consumer commits to generate on partition zero")
	flag.BoolVar(&withSnapshots, "snapshots", false, "publish system-log projection snapshots before reopen")
	flag.BoolVar(&invalidateSnapshot, "invalidate-snapshot", false, "invalidate both system-log snapshots before reopen")
	flag.StringVar(&output, "output", "", "write JSON report to this path (stdout by default)")
	flag.Parse()
	if dir == "" || records == 0 || partitions == 0 || batchRecords == 0 || segmentBytes == 0 || catalogTopics == 0 {
		fatal("data-dir, records, partitions, batch-records, segment-bytes, and catalog-topics must be positive")
	}
	if uint64(partitions) > uint64(^uint32(0)) || uint64(batchRecords) > uint64(^uint32(0)) || uint64(catalogTopics) > uint64(^uint32(0)) {
		fatal("partitions, batch-records, and catalog-topics must fit in uint32")
	}
	if consumerCommits > records {
		fatal("consumer-commits (%d) cannot exceed records per partition (%d)", consumerCommits, records)
	}
	if invalidateSnapshot && !withSnapshots {
		fatal("invalidate-snapshot requires snapshots")
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) != 0 {
		fatal("data directory must be fresh: %s", dir)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		fatal("inspect data directory: %v", err)
	}

	options := storage.PartitionOptions{SegmentBytes: segmentBytes, BatchRecords: uint32(batchRecords), BatchBytes: 16 << 10, RecordBytes: 4096}
	store, err := storage.Open(dir)
	if err != nil {
		fatal("open new store: %v", err)
	}
	descriptor, err := store.CreateTopic("history-scale", uint32(partitions), options)
	if err != nil {
		_ = store.Close()
		fatal("create topic: %v", err)
	}
	for topic := uint(1); topic < catalogTopics; topic++ {
		name := fmt.Sprintf("history-catalog-%06d", topic)
		if _, err := store.CreateTopic(name, 1, options); err != nil {
			_ = store.Close()
			fatal("create catalog topic %q: %v", name, err)
		}
	}
	payload := []byte(strings.Repeat("x", 256))
	var written uint64
	for partition := uint32(0); partition < uint32(partitions); partition++ {
		part, err := store.OpenPartition(descriptor.ID, partition, options)
		if err != nil {
			_ = store.Close()
			fatal("open partition %d: %v", partition, err)
		}
		for base := uint64(0); base < records; base += uint64(batchRecords) {
			count := uint64(batchRecords)
			if remaining := records - base; remaining < count {
				count = remaining
			}
			batch := api.RecordBatch{Topic: descriptor.ID, Partition: partition, BaseOffset: base, Records: make([]api.Record, count)}
			for index := range batch.Records {
				batch.Records[index] = api.Record{Topic: descriptor.ID, Partition: partition, Offset: base + uint64(index), Value: payload}
			}
			if _, err := part.AppendBatch(batch); err != nil {
				_ = store.Close()
				fatal("append partition %d at %d: %v", partition, base, err)
			}
			written += count
		}
	}
	if consumerCommits > 0 {
		consumer, err := store.OpenConsumer(context.Background(), "history-scale", descriptor.ID, 0, api.ConsumerOptions{Start: api.GroupStartEarliest})
		if err != nil {
			_ = store.Close()
			fatal("open history consumer: %v", err)
		}
		var delivered uint64
		for delivered < records {
			result, err := consumer.Poll(context.Background(), api.FetchOptions{MaxRecords: 4096, MaxBytes: 4 << 20})
			if err != nil {
				_ = consumer.Close()
				_ = store.Close()
				fatal("deliver history records at %d: %v", delivered, err)
			}
			if result.NextOffset <= delivered {
				_ = consumer.Close()
				_ = store.Close()
				fatal("history consumer made no progress at %d", delivered)
			}
			delivered = result.NextOffset
		}
		quotient, remainder := records/consumerCommits, records%consumerCommits
		for commit := uint64(1); commit <= consumerCommits; commit++ {
			next := commit * quotient
			if commit < remainder {
				next += commit
			} else {
				next += remainder
			}
			if err := consumer.Commit(context.Background(), next); err != nil {
				_ = consumer.Close()
				_ = store.Close()
				fatal("write consumer commit %d at offset %d: %v", commit, next, err)
			}
		}
		if err := consumer.Close(); err != nil {
			_ = store.Close()
			fatal("close history consumer: %v", err)
		}
	}
	if withSnapshots {
		if err := store.SaveSnapshots(); err != nil {
			_ = store.Close()
			fatal("save snapshots: %v", err)
		}
	}
	if err := store.Close(); err != nil {
		fatal("close generated store: %v", err)
	}
	if invalidateSnapshot {
		for _, systemLog := range []string{"cluster-metadata", "consumer-offsets"} {
			path := filepath.Join(dir, "system", systemLog, "0", "projection.snapshot")
			if err := invalidate(path); err != nil {
				fatal("invalidate %s snapshot: %v", systemLog, err)
			}
		}
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	rssBefore := readRSS()
	started := time.Now()
	reopened, err := storage.Open(dir)
	openNanos := uint64(time.Since(started))
	if err != nil {
		fatal("reopen generated store: %v", err)
	}
	runtime.ReadMemStats(&after)
	rssAfter := readRSS()
	diagnostics := reopened.SnapshotDiagnostics()
	if err := reopened.Close(); err != nil {
		fatal("close reopened store: %v", err)
	}
	segments, logBytes := inventory(dir)
	result := report{Version: 2, DataDir: dir, RecordsRequested: records, RecordsWritten: written, Partitions: uint32(partitions), SegmentBytes: segmentBytes, BatchRecords: uint32(batchRecords), CatalogTopics: uint32(catalogTopics), ConsumerCommits: consumerCommits, Snapshots: withSnapshots, SnapshotInvalidated: invalidateSnapshot, Segments: segments, LogBytes: logBytes, OpenNanos: openNanos, HeapAllocBefore: before.HeapAlloc, HeapAllocAfter: after.HeapAlloc, HeapAllocDelta: int64(after.HeapAlloc) - int64(before.HeapAlloc), HeapInuseAfter: after.HeapInuse, RSSBefore: rssBefore, RSSAfter: rssAfter, SnapshotDiagnostics: fmt.Sprintf("catalog=%v offsets=%v", diagnostics.Catalog, diagnostics.Offsets)}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatal("encode report: %v", err)
	}
	if output == "" {
		fmt.Println(string(data))
		return
	}
	if err := os.WriteFile(output, append(data, '\n'), 0o644); err != nil {
		fatal("write report: %v", err)
	}
}

func invalidate(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < 32 {
		return fmt.Errorf("snapshot is only %d bytes", len(data))
	}
	data[len(data)/2] ^= 0xff
	return os.WriteFile(path, data, 0o600)
}

func inventory(root string) (segments, bytes uint64) {
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".log") {
			segments++
			bytes += uint64(info.Size())
		}
		return nil
	})
	return segments, bytes
}

func readRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmRSS:" {
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			return value * 1024
		}
	}
	return 0
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "history: "+format+"\n", args...)
	os.Exit(1)
}
