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
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

type inputPaths []string

func (paths *inputPaths) String() string { return strings.Join(*paths, ",") }

func (paths *inputPaths) Set(value string) error {
	if value == "" {
		return errors.New("input path cannot be empty")
	}
	*paths = append(*paths, value)
	return nil
}

type rawPhase struct {
	Name          string `json:"name"`
	DurationNanos uint64 `json:"duration_nanos"`
}

type rawSample struct {
	ElapsedNanos uint64 `json:"elapsed_nanos"`
	Offered      uint64 `json:"offered"`
	Acknowledged uint64 `json:"acknowledged"`
	Delivered    uint64 `json:"delivered"`
	Committed    uint64 `json:"committed"`
}

type rawLatency struct {
	P50  uint64 `json:"p50_bucket_nanos"`
	P90  uint64 `json:"p90_bucket_nanos"`
	P95  uint64 `json:"p95_bucket_nanos"`
	P99  uint64 `json:"p99_bucket_nanos"`
	P999 uint64 `json:"p999_bucket_nanos"`
}

type rawReport struct {
	Version          uint32                     `json:"version"`
	WarmupNanos      uint64                     `json:"warmup_nanos"`
	MeasurementNanos uint64                     `json:"measurement_nanos"`
	TotalNanos       uint64                     `json:"total_nanos"`
	Phases           []rawPhase                 `json:"phases"`
	SamplesDropped   uint64                     `json:"samples_dropped"`
	Offered          uint64                     `json:"offered"`
	Acknowledged     uint64                     `json:"acknowledged"`
	Unknown          uint64                     `json:"unknown"`
	KnownRejected    uint64                     `json:"known_rejected"`
	DeliveredBytes   uint64                     `json:"delivered_payload_bytes"`
	AckBytes         uint64                     `json:"acknowledged_bytes"`
	Samples          []rawSample                `json:"samples"`
	Latency          map[string]rawLatency      `json:"latency"`
	Oracles          map[string]json.RawMessage `json:"oracles"`
}

type latencySummary struct {
	P50Nanos  *uint64 `json:"p50_nanos"`
	P90Nanos  *uint64 `json:"p90_nanos"`
	P95Nanos  *uint64 `json:"p95_nanos"`
	P99Nanos  *uint64 `json:"p99_nanos"`
	P999Nanos *uint64 `json:"p999_nanos"`
}

type analysis struct {
	Input              string                    `json:"input"`
	Version            uint32                    `json:"version"`
	WarmupNanos        uint64                    `json:"warmup_nanos"`
	MeasurementNanos   uint64                    `json:"measurement_nanos"`
	TotalNanos         uint64                    `json:"total_nanos"`
	DrainNanos         uint64                    `json:"drain_nanos"`
	VerifyNanos        uint64                    `json:"verify_nanos"`
	OfferedRecordsPerS float64                   `json:"offered_records_per_second"`
	AcknowledgedPerS   float64                   `json:"acknowledged_records_per_second"`
	AcknowledgedBytesS float64                   `json:"acknowledged_bytes_per_second"`
	DeliveredBytesS    float64                   `json:"delivered_payload_bytes_per_second"`
	Offered            uint64                    `json:"offered"`
	Acknowledged       uint64                    `json:"acknowledged"`
	Unknown            uint64                    `json:"unknown"`
	KnownRejected      uint64                    `json:"known_rejected"`
	BacklogStart       uint64                    `json:"backlog_start"`
	BacklogEnd         uint64                    `json:"backlog_end"`
	BacklogMax         uint64                    `json:"backlog_max"`
	BacklogSlopePerS   float64                   `json:"backlog_slope_per_second"`
	Samples            uint64                    `json:"samples"`
	SamplesDropped     uint64                    `json:"samples_dropped"`
	Latency            map[string]latencySummary `json:"latency"`
	Valid              bool                      `json:"valid"`
	Reasons            []string                  `json:"reasons,omitempty"`
}

func loadReport(path string) (rawReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return rawReport{}, fmt.Errorf("read %s: %w", path, err)
	}
	var report rawReport
	if err := json.Unmarshal(data, &report); err != nil {
		return rawReport{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if report.Version == 0 || report.Version > 2 {
		return rawReport{}, fmt.Errorf("%s has unsupported metrics version %d", path, report.Version)
	}
	if report.MeasurementNanos == 0 {
		return rawReport{}, fmt.Errorf("%s has zero measurement duration", path)
	}
	if report.TotalNanos == 0 {
		report.TotalNanos = report.MeasurementNanos
	}
	return report, nil
}

func analyze(path string, report rawReport) analysis {
	seconds := float64(report.MeasurementNanos) / 1e9
	result := analysis{
		Input: path, Version: report.Version, WarmupNanos: report.WarmupNanos, MeasurementNanos: report.MeasurementNanos,
		TotalNanos: report.TotalNanos, OfferedRecordsPerS: float64(report.Offered) / seconds,
		AcknowledgedPerS:   float64(report.Acknowledged) / seconds,
		AcknowledgedBytesS: float64(report.AckBytes) / seconds,
		DeliveredBytesS:    float64(report.DeliveredBytes) / seconds,
		Offered:            report.Offered, Acknowledged: report.Acknowledged, Unknown: report.Unknown,
		KnownRejected: report.KnownRejected, Samples: uint64(len(report.Samples)),
		SamplesDropped: report.SamplesDropped, Latency: make(map[string]latencySummary), Valid: true,
	}
	for _, phase := range report.Phases {
		switch phase.Name {
		case "drain":
			result.DrainNanos = phase.DurationNanos
		case "verify":
			result.VerifyNanos = phase.DurationNanos
		}
	}
	if len(report.Samples) > 0 {
		first := report.Samples[0]
		last := report.Samples[len(report.Samples)-1]
		result.BacklogStart = backlog(first)
		result.BacklogEnd = backlog(last)
		for _, sample := range report.Samples {
			if current := backlog(sample); current > result.BacklogMax {
				result.BacklogMax = current
			}
		}
		if last.ElapsedNanos > first.ElapsedNanos {
			result.BacklogSlopePerS = float64(int64(result.BacklogEnd)-int64(result.BacklogStart)) * 1e9 / float64(last.ElapsedNanos-first.ElapsedNanos)
		}
	}
	for name, latency := range report.Latency {
		result.Latency[name] = latencySummary{P50Nanos: bucket(latency.P50), P90Nanos: bucket(latency.P90), P95Nanos: bucket(latency.P95), P99Nanos: bucket(latency.P99), P999Nanos: bucket(latency.P999)}
	}
	if report.SamplesDropped != 0 {
		result.Valid = false
		result.Reasons = append(result.Reasons, "samples were dropped")
	}
	if result.BacklogSlopePerS > 0 {
		result.Valid = false
		result.Reasons = append(result.Reasons, "backlog increased during the sampled interval")
	}
	if len(report.Oracles) == 0 {
		result.Valid = false
		result.Reasons = append(result.Reasons, "oracle results are missing")
	}
	return result
}

func backlog(sample rawSample) uint64 {
	if sample.Acknowledged <= sample.Delivered {
		return 0
	}
	return sample.Acknowledged - sample.Delivered
}

func bucket(value uint64) *uint64 {
	if value == 0 {
		return nil
	}
	return &value
}

func renderMarkdown(results []analysis) string {
	var builder strings.Builder
	builder.WriteString("| Run | Valid | Measurement | Offered records/s | Acknowledged records/s | Backlog start | Backlog end | Backlog slope/s |\n")
	builder.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, result := range results {
		builder.WriteString(fmt.Sprintf("| `%s` | %t | %.1fs | %.2f | %.2f | %d | %d | %.2f |\n", result.Input, result.Valid, float64(result.MeasurementNanos)/1e9, result.OfferedRecordsPerS, result.AcknowledgedPerS, result.BacklogStart, result.BacklogEnd, result.BacklogSlopePerS))
		for _, reason := range result.Reasons {
			builder.WriteString(fmt.Sprintf("\n- `%s`: %s\n", result.Input, reason))
		}
	}
	return builder.String()
}

func main() {
	var inputs inputPaths
	format := flag.String("format", "json", "output format: json or markdown")
	flag.Var(&inputs, "input", "metrics.json input; may be repeated")
	flag.Parse()
	if len(inputs) == 0 {
		fmt.Fprintln(os.Stderr, "at least one --input is required")
		os.Exit(2)
	}
	if *format != "json" && *format != "markdown" {
		fmt.Fprintf(os.Stderr, "unsupported format %q\n", *format)
		os.Exit(2)
	}
	results := make([]analysis, 0, len(inputs))
	for _, path := range inputs {
		raw, err := loadReport(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		results = append(results, analyze(path, raw))
	}
	if *format == "markdown" {
		fmt.Print(renderMarkdown(results))
		return
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if len(results) == 1 {
		_ = encoder.Encode(results[0])
	} else {
		_ = encoder.Encode(results)
	}
}
