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

// Package metrics contains benchmark-only measurement helpers. It has no
// dependency on the storage engine and is not part of the public API.
package metrics

import (
	"math"
	"sort"
)

// Workload describes the dimensions needed to interpret one measurement.
type Workload struct {
	Profile       string `json:"profile"`
	PayloadBytes  int    `json:"payload_bytes,omitempty"`
	BatchRecords  int    `json:"batch_records,omitempty"`
	BatchBytes    int    `json:"batch_bytes,omitempty"`
	Producers     int    `json:"producers,omitempty"`
	Consumers     int    `json:"consumers,omitempty"`
	Partitions    int    `json:"partitions,omitempty"`
	LingerNanos   int64  `json:"linger_nanos,omitempty"`
	FetchRecords  int    `json:"fetch_records,omitempty"`
	FetchBytes    int    `json:"fetch_bytes,omitempty"`
	ReadMode      string `json:"read_mode,omitempty"`
	DurationNanos int64  `json:"duration_nanos,omitempty"`
}

// Rate returns units per second. It returns zero for a non-positive duration.
func Rate(units uint64, elapsedNanos uint64) float64 {
	if elapsedNanos == 0 {
		return 0
	}
	return float64(units) * 1e9 / float64(elapsedNanos)
}

// Samples stores operation durations for one bounded measurement interval.
// Samples is intentionally not safe for concurrent use; use one recorder per
// worker and merge them after the timed section.
type Samples struct {
	values []uint64
	total  uint64
}

// Observe records one duration in nanoseconds.
func (samples *Samples) Observe(nanos uint64) {
	samples.values = append(samples.values, nanos)
	samples.total += nanos
}

// Count returns the number of recorded durations.
func (samples *Samples) Count() uint64 {
	return uint64(len(samples.values))
}

// TotalNanos returns the sum of recorded durations.
func (samples *Samples) TotalNanos() uint64 {
	return samples.total
}

// Merge appends another recorder's samples to the receiver.
func (samples *Samples) Merge(other Samples) {
	samples.values = append(samples.values, other.values...)
	samples.total += other.total
}

// Values returns a copy of the recorded durations.
func (samples *Samples) Values() []uint64 {
	return append([]uint64(nil), samples.values...)
}

// Summary contains exact order statistics for one bounded sample set.
type Summary struct {
	Count        uint64  `json:"count"`
	TotalNanos   uint64  `json:"total_nanos"`
	AverageNanos float64 `json:"average_nanos"`
	MinNanos     uint64  `json:"min_nanos"`
	MaxNanos     uint64  `json:"max_nanos"`
	P50Nanos     uint64  `json:"p50_nanos"`
	P90Nanos     uint64  `json:"p90_nanos"`
	P95Nanos     uint64  `json:"p95_nanos"`
	P99Nanos     uint64  `json:"p99_nanos"`
	P999Nanos    uint64  `json:"p999_nanos"`
}

// Summarize returns exact percentiles for the supplied bounded samples.
func Summarize(samples *Samples) Summary {
	if samples == nil || len(samples.values) == 0 {
		return Summary{}
	}
	values := samples.Values()
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	result := Summary{
		Count:        uint64(len(values)),
		TotalNanos:   samples.total,
		AverageNanos: float64(samples.total) / float64(len(values)),
		MinNanos:     values[0],
		MaxNanos:     values[len(values)-1],
		P50Nanos:     percentile(values, 0.50),
		P90Nanos:     percentile(values, 0.90),
		P95Nanos:     percentile(values, 0.95),
		P99Nanos:     percentile(values, 0.99),
		P999Nanos:    percentile(values, 0.999),
	}
	return result
}

func percentile(sorted []uint64, fraction float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// Histogram is a bounded, mergeable duration histogram. Each boundary is an
// exclusive upper bound in nanoseconds; the final bucket contains all larger
// values. It is intended for long-running workloads where retaining every
// sample is undesirable.
type Histogram struct {
	boundaries []uint64
	counts     []uint64
	total      uint64
}

// NewHistogram creates a histogram with the supplied exclusive upper bounds.
func NewHistogram(boundaries []uint64) Histogram {
	copyBoundaries := append([]uint64(nil), boundaries...)
	return Histogram{boundaries: copyBoundaries, counts: make([]uint64, len(copyBoundaries)+1)}
}

// Observe adds one duration in nanoseconds to the appropriate bucket.
func (histogram *Histogram) Observe(nanos uint64) {
	index := sort.Search(len(histogram.boundaries), func(index int) bool {
		return nanos < histogram.boundaries[index]
	})
	histogram.counts[index]++
	histogram.total += nanos
}

// Merge adds another histogram with identical boundaries.
func (histogram *Histogram) Merge(other Histogram) {
	if len(histogram.boundaries) != len(other.boundaries) {
		return
	}
	for index := range histogram.boundaries {
		if histogram.boundaries[index] != other.boundaries[index] {
			return
		}
	}
	for index, count := range other.counts {
		histogram.counts[index] += count
	}
	histogram.total += other.total
}

// HistogramSnapshot is the serializable view of a histogram.
type HistogramSnapshot struct {
	Boundaries []uint64 `json:"boundaries_nanos"`
	Counts     []uint64 `json:"counts"`
	Total      uint64   `json:"total_nanos"`
	Count      uint64   `json:"count"`
}

// Snapshot returns a copy suitable for serialization or reporting.
func (histogram *Histogram) Snapshot() HistogramSnapshot {
	counts := append([]uint64(nil), histogram.counts...)
	var count uint64
	for _, value := range counts {
		count += value
	}
	return HistogramSnapshot{
		Boundaries: append([]uint64(nil), histogram.boundaries...),
		Counts:     counts,
		Total:      histogram.total,
		Count:      count,
	}
}

// Quantile returns the upper bound of the bucket containing the requested
// quantile. A zero upper bound means the quantile is in the final open bucket.
func (snapshot HistogramSnapshot) Quantile(fraction float64) uint64 {
	if snapshot.Count == 0 || len(snapshot.Counts) == 0 {
		return 0
	}
	target := uint64(math.Ceil(fraction * float64(snapshot.Count)))
	if target == 0 {
		target = 1
	}
	var cumulative uint64
	for index, count := range snapshot.Counts {
		cumulative += count
		if cumulative >= target {
			if index < len(snapshot.Boundaries) {
				return snapshot.Boundaries[index]
			}
			return 0
		}
	}
	return 0
}
