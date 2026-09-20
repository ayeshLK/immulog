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
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

type benchmarkSample struct {
	Name               string  `json:"name"`
	NanosPerOp         float64 `json:"nanos_per_op"`
	MegabytesPerS      float64 `json:"megabytes_per_second,omitempty"`
	BytesPerS          float64 `json:"bytes_per_second,omitempty"`
	RecordsPerS        float64 `json:"records_per_second,omitempty"`
	BytesPerOp         float64 `json:"bytes_per_op,omitempty"`
	AllocsPerOp        float64 `json:"allocs_per_op,omitempty"`
	SyncsPerRecord     float64 `json:"syncs_per_record,omitempty"`
	WritesPerRecord    float64 `json:"writes_per_record,omitempty"`
	HasMegabytes       bool    `json:"-"`
	HasBytes           bool    `json:"-"`
	HasRecords         bool    `json:"-"`
	HasBytesPerOp      bool    `json:"-"`
	HasAllocsPerOp     bool    `json:"-"`
	HasSyncsPerRecord  bool    `json:"-"`
	HasWritesPerRecord bool    `json:"-"`
}

type metricStats struct {
	Samples int     `json:"samples"`
	Median  float64 `json:"median"`
	P10     float64 `json:"p10"`
	P90     float64 `json:"p90"`
	Minimum float64 `json:"minimum"`
	Maximum float64 `json:"maximum"`
	CV      float64 `json:"coefficient_of_variation"`
}

type benchmarkSummary struct {
	Name            string      `json:"name"`
	NanosPerOp      metricStats `json:"nanos_per_op"`
	MegabytesPerS   metricStats `json:"megabytes_per_second,omitempty"`
	BytesPerS       metricStats `json:"bytes_per_second,omitempty"`
	RecordsPerS     metricStats `json:"records_per_second,omitempty"`
	BytesPerOp      metricStats `json:"bytes_per_op,omitempty"`
	AllocsPerOp     metricStats `json:"allocs_per_op,omitempty"`
	SyncsPerRecord  metricStats `json:"syncs_per_record,omitempty"`
	WritesPerRecord metricStats `json:"writes_per_record,omitempty"`
}

type report struct {
	Metadata   map[string]string  `json:"metadata"`
	Files      []string           `json:"files"`
	Benchmarks []benchmarkSummary `json:"benchmarks"`
	Warnings   []string           `json:"warnings,omitempty"`
}

func parseBenchmarkLine(line string) (benchmarkSample, bool, error) {
	fields := strings.Fields(line)
	if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") {
		return benchmarkSample{}, false, nil
	}
	name := fields[0]
	nsIndex := -1
	for index, field := range fields[1:] {
		if field == "ns/op" {
			nsIndex = index + 1
			break
		}
	}
	if nsIndex < 1 {
		return benchmarkSample{}, false, nil
	}
	nanos, err := strconv.ParseFloat(fields[nsIndex-1], 64)
	if err != nil {
		return benchmarkSample{}, false, fmt.Errorf("%s: invalid ns/op value %q: %w", name, fields[nsIndex-1], err)
	}
	sample := benchmarkSample{Name: name, NanosPerOp: nanos}
	for index := nsIndex + 1; index+1 < len(fields); index++ {
		value, err := strconv.ParseFloat(fields[index], 64)
		if err != nil {
			continue
		}
		switch fields[index+1] {
		case "MB/s":
			sample.MegabytesPerS, sample.HasMegabytes = value, true
		case "producer-bytes/s", "consumer-bytes/s":
			sample.BytesPerS, sample.HasBytes = value, true
		case "producer-records/s", "consumer-records/s":
			sample.RecordsPerS, sample.HasRecords = value, true
		case "B/op":
			sample.BytesPerOp, sample.HasBytesPerOp = value, true
		case "allocs/op":
			sample.AllocsPerOp, sample.HasAllocsPerOp = value, true
		case "syncs/record":
			sample.SyncsPerRecord, sample.HasSyncsPerRecord = value, true
		case "writes/record":
			sample.WritesPerRecord, sample.HasWritesPerRecord = value, true
		}
	}
	return sample, true, nil
}

func loadSamples(paths []string) ([]benchmarkSample, error) {
	var samples []benchmarkSample
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			sample, ok, err := parseBenchmarkLine(scanner.Text())
			if err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("parse %s: %w", path, err)
			}
			if ok {
				samples = append(samples, sample)
			}
		}
		if err := scanner.Err(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close %s: %w", path, err)
		}
	}
	if len(samples) == 0 {
		return nil, errors.New("no benchmark results found")
	}
	return samples, nil
}

func readKeyValues(path string) map[string]string {
	values := make(map[string]string)
	data, err := os.ReadFile(path)
	if err != nil {
		return values
	}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, found := strings.Cut(line, "=")
		if found {
			values[key] = value
		}
	}
	return values
}

func readEnvironment(path string) map[string]string {
	values := readKeyValues(path)
	data, err := os.ReadFile(path)
	if err != nil {
		return values
	}
	lines := strings.Split(string(data), "\n")
	for index, line := range lines {
		switch {
		case strings.HasPrefix(line, "go version "):
			values["go_version"] = strings.TrimPrefix(line, "go version ")
		case strings.HasPrefix(line, "model name"):
			if _, value, found := strings.Cut(line, ":"); found {
				values["cpu_model"] = strings.TrimSpace(value)
			}
		case strings.HasPrefix(line, "Linux "):
			values["host_kernel"] = line
		case index+1 < len(lines):
			fields := strings.Fields(line)
			if len(fields) >= 3 && fields[0] == "TARGET" && fields[1] == "SOURCE" && fields[2] == "FSTYPE" {
				mountFields := strings.Fields(lines[index+1])
				if len(mountFields) >= 3 {
					values["filesystem"] = mountFields[2]
				}
			}
		case line == "Filesystem" && index+1 < len(lines):
			fields := strings.Fields(lines[index+1])
			if len(fields) >= 3 && fields[2] != "1K-blocks" {
				values["filesystem"] = fields[2]
			}
		}
	}
	return values
}

func summarize(values []float64) metricStats {
	if len(values) == 0 {
		return metricStats{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mean := 0.0
	for _, value := range sorted {
		mean += value
	}
	mean /= float64(len(sorted))
	variance := 0.0
	for _, value := range sorted {
		variance += (value - mean) * (value - mean)
	}
	variance /= float64(len(sorted))
	return metricStats{
		Samples: len(sorted), Median: quantile(sorted, 0.5), P10: quantile(sorted, 0.1),
		P90: quantile(sorted, 0.9), Minimum: sorted[0], Maximum: sorted[len(sorted)-1],
		CV: math.Sqrt(variance) / mean * 100,
	}
}

func quantile(sorted []float64, fraction float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	position := fraction * float64(len(sorted)-1)
	lower := int(position)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[lower]
	}
	return sorted[lower] + (sorted[upper]-sorted[lower])*(position-float64(lower))
}

func buildReport(inputs []string, samples []benchmarkSample) report {
	groups := make(map[string][]benchmarkSample)
	for _, sample := range samples {
		groups[sample.Name] = append(groups[sample.Name], sample)
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	benchmarks := make([]benchmarkSummary, 0, len(names))
	warnings := make([]string, 0)
	for _, name := range names {
		group := groups[name]
		values := func(selectValue func(benchmarkSample) (float64, bool)) []float64 {
			result := make([]float64, 0, len(group))
			for _, sample := range group {
				if value, ok := selectValue(sample); ok {
					result = append(result, value)
				}
			}
			return result
		}
		summary := benchmarkSummary{
			Name:            name,
			NanosPerOp:      summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.NanosPerOp, true })),
			MegabytesPerS:   summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.MegabytesPerS, sample.HasMegabytes })),
			BytesPerS:       summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.BytesPerS, sample.HasBytes })),
			RecordsPerS:     summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.RecordsPerS, sample.HasRecords })),
			BytesPerOp:      summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.BytesPerOp, sample.HasBytesPerOp })),
			AllocsPerOp:     summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.AllocsPerOp, sample.HasAllocsPerOp })),
			SyncsPerRecord:  summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.SyncsPerRecord, sample.HasSyncsPerRecord })),
			WritesPerRecord: summarize(values(func(sample benchmarkSample) (float64, bool) { return sample.WritesPerRecord, sample.HasWritesPerRecord })),
		}
		benchmarks = append(benchmarks, summary)
		if summary.NanosPerOp.CV >= 10 {
			warnings = append(warnings, fmt.Sprintf("%s has %.1f%% coefficient of variation for ns/op", name, summary.NanosPerOp.CV))
		}
	}
	metadata := make(map[string]string)
	runDir := filepath.Dir(inputs[0])
	for key, value := range readKeyValues(filepath.Join(runDir, "configuration.txt")) {
		metadata[key] = value
	}
	for key, value := range readEnvironment(filepath.Join(runDir, "environment.txt")) {
		if _, exists := metadata[key]; !exists {
			metadata[key] = value
		}
	}
	return report{Metadata: metadata, Files: inputs, Benchmarks: benchmarks, Warnings: warnings}
}

func displayValue(stats metricStats) string {
	if stats.Samples == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f", stats.Median)
}

func displayDecimal(stats metricStats) string {
	if stats.Samples == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", stats.Median)
}

func displayRange(stats metricStats) string {
	if stats.Samples == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f–%.0f", stats.P10, stats.P90)
}

func renderMarkdown(result report) string {
	date := result.Metadata["date"]
	if len(date) >= 10 {
		date = date[:10]
	}
	if date == "" {
		date = "undated"
	}
	commit := result.Metadata["commit"]
	marker := fmt.Sprintf("<!-- microbenchmark-evidence:commit=%s,date=%s -->", commit, date)
	var builder strings.Builder
	builder.WriteString(marker + "\n\n")
	builder.WriteString(fmt.Sprintf("### Microbenchmark %s — %s\n\n", result.Metadata["profile"], date))
	builder.WriteString(fmt.Sprintf("Measured from commit `%s` with profile `%s`, suite `%s`, `%s` per sample, %s samples per process run, and %s independent process runs.\n\n", commit, result.Metadata["profile"], result.Metadata["suite"], result.Metadata["benchtime"], result.Metadata["count"], result.Metadata["runs"]))
	builder.WriteString(fmt.Sprintf("Host: %s; Go: %s; filesystem: %s.\n\n", result.Metadata["cpu_model"], result.Metadata["go_version"], result.Metadata["filesystem"]))
	builder.WriteString("Values are medians across all captured samples; P10–P90 shows ns/op variability.\n\n")
	builder.WriteString("| Benchmark | Samples | Median ns/op | P10–P90 ns/op | CV | Median records/s | Median MB/s | Median B/op | Median allocs/op |\n")
	builder.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, benchmark := range result.Benchmarks {
		builder.WriteString(fmt.Sprintf("| `%s` | %d | %s | %s | %.1f%% | %s | %s | %s | %s |\n", benchmark.Name, benchmark.NanosPerOp.Samples, displayValue(benchmark.NanosPerOp), displayRange(benchmark.NanosPerOp), benchmark.NanosPerOp.CV, displayValue(benchmark.RecordsPerS), displayDecimal(benchmark.MegabytesPerS), displayValue(benchmark.BytesPerOp), displayValue(benchmark.AllocsPerOp)))
	}
	if len(result.Warnings) > 0 {
		builder.WriteString("\nVariance notes:\n\n")
		for _, warning := range result.Warnings {
			builder.WriteString("- " + warning + ".\n")
		}
	}
	builder.WriteString("\nThe raw run artifacts and machine-readable analysis are preserved in the benchmark run directory. This is host-specific evidence, not a portable throughput guarantee.\n")
	return builder.String()
}

func writeOutput(path, content string) error {
	if path == "" {
		_, err := fmt.Print(content)
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

func appendMarkdown(path, content string, result report) error {
	document, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read append target %s: %w", path, err)
	}
	date := result.Metadata["date"]
	if len(date) >= 10 {
		date = date[:10]
	}
	marker := fmt.Sprintf("<!-- microbenchmark-evidence:commit=%s,date=%s -->", result.Metadata["commit"], date)
	if strings.Contains(string(document), marker) {
		return fmt.Errorf("benchmark evidence is already recorded in %s", path)
	}
	heading := "## Recorded results\n"
	index := strings.Index(string(document), heading)
	if index < 0 {
		return fmt.Errorf("append target %s has no %q heading", path, strings.TrimSpace(heading))
	}
	insertAt := index + len(heading)
	updated := string(document[:insertAt]) + "\n" + content + "\n" + string(document[insertAt:])
	return os.WriteFile(path, []byte(updated), 0o644)
}

func main() {
	var inputs inputPaths
	format := flag.String("format", "markdown", "output format: markdown or json")
	output := flag.String("output", "", "output path; stdout when omitted")
	appendTo := flag.String("append-to", "", "append Markdown output below the BENCHMARKS.md Recorded results heading")
	flag.Var(&inputs, "input", "benchmark output file; may be repeated")
	flag.Parse()
	if len(inputs) == 0 {
		fmt.Fprintln(os.Stderr, "at least one --input is required")
		os.Exit(2)
	}
	if *format != "markdown" && *format != "json" {
		fmt.Fprintf(os.Stderr, "unsupported format %q\n", *format)
		os.Exit(2)
	}
	if *appendTo != "" && *format != "markdown" {
		fmt.Fprintln(os.Stderr, "--append-to requires Markdown format")
		os.Exit(2)
	}
	samples, err := loadSamples(inputs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result := buildReport(inputs, samples)
	var content []byte
	if *format == "markdown" {
		content = []byte(renderMarkdown(result))
	} else {
		content, err = json.MarshalIndent(result, "", "  ")
		if err == nil {
			content = append(content, '\n')
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeOutput(*output, string(content)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *appendTo != "" {
		if err := appendMarkdown(*appendTo, string(content), result); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
