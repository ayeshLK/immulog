//go:build !windows

// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package storage

import "errors"

func syncDirectory(path string) error {
	directory, err := fsOpen(path)
	if err != nil {
		return err
	}
	syncErr := fileSync(directory)
	closeErr := fileClose(directory)
	return errors.Join(syncErr, closeErr)
}
