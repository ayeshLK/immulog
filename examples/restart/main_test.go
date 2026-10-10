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
	"context"
	"errors"
	"testing"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

func TestRestartExample(t *testing.T) {
	result, err := run(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstDelivery != 1 || result.Redelivery != 1 || result.AppliedEffects != 1 || result.FinalDelivery != 0 {
		t.Fatalf("unexpected report: %#v", result)
	}
}

func TestReconcileAppendFindsDurableRecord(t *testing.T) {
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	descriptor, err := store.CreateTopic("orders", 1, storage.PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := store.OpenTopic(descriptor.Name)
	if err != nil {
		t.Fatal(err)
	}
	request := api.AppendRequest{Topic: descriptor.ID, Partition: 0, Key: []byte("request-1"), Value: []byte("value")}
	if _, err := partitions[0].Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, err := reconcileAppend(context.Background(), partitions[0], request, api.ErrAppendOutcomeUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if string(record.Key) != "request-1" {
		t.Fatalf("reconciled key = %q", record.Key)
	}
	_, err = reconcileAppend(context.Background(), partitions[0], api.AppendRequest{Topic: descriptor.ID, Partition: 0, Key: []byte("missing")}, api.ErrAppendOutcomeUnknown)
	if !errors.Is(err, errReconciliationRequired) {
		t.Fatalf("missing reconciliation error = %v", err)
	}
}
