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

package metrics

import (
	"reflect"
	"testing"
)

func TestSummarizeEmpty(t *testing.T) {
	if got := Summarize(&Samples{}); got != (Summary{}) {
		t.Fatalf("empty summary = %#v, want zero summary", got)
	}
}

func TestSummarizePercentiles(t *testing.T) {
	var samples Samples
	for _, value := range []uint64{40, 10, 30, 20, 50} {
		samples.Observe(value)
	}

	got := Summarize(&samples)
	want := Summary{
		Count:        5,
		TotalNanos:   150,
		AverageNanos: 30,
		MinNanos:     10,
		MaxNanos:     50,
		P50Nanos:     30,
		P90Nanos:     50,
		P95Nanos:     50,
		P99Nanos:     50,
		P999Nanos:    50,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("summary = %#v, want %#v", got, want)
	}
}

func TestSamplesMergeAndValuesCopy(t *testing.T) {
	var left, right Samples
	left.Observe(10)
	right.Observe(20)
	left.Merge(right)
	values := left.Values()
	values[0] = 99

	if got, want := left.Count(), uint64(2); got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
	if got, want := left.TotalNanos(), uint64(30); got != want {
		t.Fatalf("total = %d, want %d", got, want)
	}
	if got, want := left.Values(), []uint64{10, 20}; !reflect.DeepEqual(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestRate(t *testing.T) {
	if got, want := Rate(250, 2_000_000_000), 125.0; got != want {
		t.Fatalf("rate = %f, want %f", got, want)
	}
	if got := Rate(1, 0); got != 0 {
		t.Fatalf("zero-duration rate = %f, want zero", got)
	}
}

func TestHistogram(t *testing.T) {
	histogram := NewHistogram([]uint64{10, 100})
	for _, value := range []uint64{1, 10, 99, 100, 1000} {
		histogram.Observe(value)
	}

	got := histogram.Snapshot()
	want := HistogramSnapshot{
		Boundaries: []uint64{10, 100},
		Counts:     []uint64{1, 2, 2},
		Total:      1210,
		Count:      5,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot = %#v, want %#v", got, want)
	}
	if got, want := got.Quantile(0.5), uint64(100); got != want {
		t.Fatalf("median bucket = %d, want %d", got, want)
	}
}

func TestHistogramMerge(t *testing.T) {
	left := NewHistogram([]uint64{10, 100})
	right := NewHistogram([]uint64{10, 100})
	left.Observe(1)
	right.Observe(1000)
	left.Merge(right)

	got := left.Snapshot()
	if got.Count != 2 || got.Total != 1001 {
		t.Fatalf("merged snapshot = %#v, want two samples totaling 1001", got)
	}
}
