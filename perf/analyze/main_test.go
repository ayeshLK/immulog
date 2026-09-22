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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeReportUsesMeasurementSamples(t *testing.T) {
	report := `{
		"version":2,
		"measurement_nanos":10000000000,
		"total_nanos":12000000000,
		"offered":30,
		"acknowledged":25,
		"acknowledged_bytes":2500,
		"delivered_payload_bytes":2000,
		"samples":[
			{"elapsed_nanos":1000000000,"acknowledged":10,"delivered":4},
			{"elapsed_nanos":9000000000,"acknowledged":20,"delivered":15}
		],
		"latency":{"commit":{"p50_bucket_nanos":10000000,"p95_bucket_nanos":0}},
		"oracles":{"soak-stable/0":{}}
	}`
	path := filepath.Join(t.TempDir(), "metrics.json")
	if err := os.WriteFile(path, []byte(report), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := loadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	result := analyze(path, raw)
	if result.MeasurementNanos != 10_000_000_000 || result.TotalNanos != 12_000_000_000 {
		t.Fatalf("durations = (%d, %d)", result.MeasurementNanos, result.TotalNanos)
	}
	if result.BacklogStart != 6 || result.BacklogEnd != 5 || result.BacklogMax != 6 {
		t.Fatalf("backlog = (%d, %d, %d)", result.BacklogStart, result.BacklogEnd, result.BacklogMax)
	}
	if result.Valid == false || result.Latency["commit"].P95Nanos != nil {
		t.Fatalf("analysis validity/quantile = (%t, %#v)", result.Valid, result.Latency["commit"])
	}
	if !strings.Contains(renderMarkdown([]analysis{result}), "10.0s") {
		t.Fatal("markdown omitted measurement duration")
	}
}

func TestLoadReportRejectsUnsupportedOrZeroDuration(t *testing.T) {
	for name, content := range map[string]string{
		"unsupported": `{"version":3,"measurement_nanos":1}`,
		"zero":        `{"version":1,"measurement_nanos":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metrics.json")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadReport(path); err == nil {
				t.Fatal("loadReport succeeded for invalid report")
			}
		})
	}
}

func TestAnalyzeFlagsIncompleteRun(t *testing.T) {
	completed := false
	result := analyze("fixture", rawReport{
		Version: 2, Completed: &completed, Failure: "assignment lost",
		MeasurementNanos: 1_000_000_000, Oracles: map[string]json.RawMessage{"stable": {}},
	})
	if result.Valid || !strings.Contains(result.Reasons[0], "assignment lost") {
		t.Fatalf("incomplete run = %#v", result)
	}
}

func TestAnalyzeFlagsIncreasingBacklog(t *testing.T) {
	result := analyze("fixture", rawReport{
		Version: 2, MeasurementNanos: 1_000_000_000,
		Samples: []rawSample{{ElapsedNanos: 0, Acknowledged: 1, Delivered: 0}, {ElapsedNanos: 1_000_000_000, Acknowledged: 3, Delivered: 0}},
		Oracles: map[string]json.RawMessage{"stable": {}},
	})
	if result.Valid || result.BacklogSlopePerS <= 0 {
		t.Fatalf("increasing backlog = %#v", result)
	}
}
