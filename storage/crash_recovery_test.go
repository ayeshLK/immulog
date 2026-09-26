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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

const (
	crashHelperEnvironment    = "IMMULOG_CRASH_HELPER"
	crashDirectoryEnvironment = "IMMULOG_CRASH_DIR"
	crashValueEnvironment     = "IMMULOG_CRASH_VALUE"
)

func TestProcessCrashRecovery(t *testing.T) {
	if os.Getenv(crashHelperEnvironment) == "1" {
		runProcessCrashHelper()
		return
	}

	dir := t.TempDir()
	expected := make([]string, 0, 2)
	for _, value := range []string{"first-crash-record", "second-crash-record"} {
		expected = append(expected, value)
		command := exec.Command(os.Args[0], "-test.run", "^TestProcessCrashRecovery$")
		command.Env = append(os.Environ(),
			crashHelperEnvironment+"=1",
			crashDirectoryEnvironment+"="+dir,
			crashValueEnvironment+"="+value,
		)
		output, err := command.CombinedOutput()
		if err == nil {
			t.Fatalf("crash helper exited successfully, output=%s", output)
		}
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("crash helper error = %T %v, output=%s", err, err, output)
		}
		if !crashTerminationExpected(exitErr) {
			t.Fatalf("crash helper status = %#v, output=%s", exitErr.ProcessState, output)
		}

		verifyCrashedRecords(t, dir, expected)
	}
}

func runProcessCrashHelper() {
	dir := os.Getenv(crashDirectoryEnvironment)
	value := os.Getenv(crashValueEnvironment)
	if dir == "" || value == "" {
		fmt.Fprintln(os.Stderr, "crash helper environment is incomplete")
		os.Exit(2)
	}
	store, err := Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open crash directory: %v\n", err)
		os.Exit(2)
	}
	partition, err := store.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open crash partition: %v\n", err)
		os.Exit(2)
	}
	_, err = partition.Append(context.Background(), api.AppendRequest{
		Topic: testTopic(), Partition: 0, Value: []byte(value),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "append crash record: %v\n", err)
		os.Exit(2)
	}
	if err := terminateCrashHelper(); err != nil {
		fmt.Fprintf(os.Stderr, "kill crash helper: %v\n", err)
		os.Exit(2)
	}
}

func verifyCrashedRecords(t *testing.T, dir string, expected []string) {
	t.Helper()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	partition, err := store.OpenPartition(testTopic(), 0, PartitionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	records, err := partition.Read(0, uint32(len(expected)+1))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(expected) {
		t.Fatalf("recovered record count = %d, want %d", len(records), len(expected))
	}
	for index, value := range expected {
		if records[index].Offset != uint64(index) || !bytes.Equal(records[index].Value, []byte(value)) {
			t.Fatalf("recovered record %d = %#v, want offset %d value %q", index, records[index], index, value)
		}
	}
}
