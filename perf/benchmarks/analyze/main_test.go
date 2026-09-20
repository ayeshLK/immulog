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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseBenchmarkLine(t *testing.T) {
	sample, ok, err := parseBenchmarkLine("BenchmarkIngressAppendParallel/producers-8/payload-256 100 250000 ns/op 0.40 MB/s 400000 producer-bytes/s 1560 producer-records/s 39050 B/op 14 allocs/op 0.25 syncs/record 0.25 writes/record")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("benchmark line was not recognized")
	}
	if sample.Name != "BenchmarkIngressAppendParallel/producers-8/payload-256" || sample.NanosPerOp != 250000 {
		t.Fatalf("sample identity = %#v", sample)
	}
	if !sample.HasRecords || sample.RecordsPerS != 1560 || !sample.HasSyncsPerRecord || sample.SyncsPerRecord != 0.25 {
		t.Fatalf("sample rates = %#v", sample)
	}
}

func TestBuildReportAggregatesRunsAndMetadata(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "configuration.txt"), []byte("profile=qualification\ncount=10\nruns=3\ncommit=abc123\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "environment.txt"), []byte("date=2026-09-20T09:23:00+05:30\ngo version go1.26.2 linux/amd64\nTARGET SOURCE FSTYPE OPTIONS\n/ /dev/root ext4 rw\nmodel name : Test CPU\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	paths := []string{filepath.Join(dir, "run-1.txt"), filepath.Join(dir, "run-2.txt")}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("BenchmarkFetch 10 100 ns/op 10 MB/s 1000 consumer-records/s 20 B/op 2 allocs/op\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	samples, err := loadSamples(paths)
	if err != nil {
		t.Fatal(err)
	}
	result := buildReport(paths, samples)
	if len(result.Benchmarks) != 1 || result.Benchmarks[0].NanosPerOp.Samples != 2 {
		t.Fatalf("benchmarks = %#v", result.Benchmarks)
	}
	if result.Metadata["filesystem"] != "ext4" || result.Metadata["cpu_model"] != "Test CPU" {
		t.Fatalf("metadata = %#v", result.Metadata)
	}
}

func TestAppendMarkdownRejectsDuplicateEvidence(t *testing.T) {
	dir := t.TempDir()
	document := filepath.Join(dir, "BENCHMARKS.md")
	if err := os.WriteFile(document, []byte("## Recorded results\n\nexisting\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	result := report{Metadata: map[string]string{"commit": "abc123", "date": "2026-09-20T00:00:00Z", "profile": "qualification"}}
	content := renderMarkdown(result)
	if err := appendMarkdown(document, content, result); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(document)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "### Microbenchmark qualification — 2026-09-20") {
		t.Fatalf("appended document = %s", data)
	}
	if err := appendMarkdown(document, content, result); err == nil {
		t.Fatal("duplicate evidence was accepted")
	}
}
