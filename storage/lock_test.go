// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ayeshLK/immulog/api"
)

func TestStoreDirectoryLockIsExclusive(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := Open(dir); !errors.Is(err, api.ErrDataDirLocked) {
		t.Fatalf("second store open error = %v, want ErrDataDirLocked", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); err != nil {
		t.Fatalf("stable LOCK stat = %v", err)
	}
}
