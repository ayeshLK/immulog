//go:build windows

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
	"fmt"

	"golang.org/x/sys/windows"
)

func syncDirectory(path string) error {
	directory, err := fsOpen(path)
	if err != nil {
		return err
	}
	syncErr := fileSync(directory)
	closeErr := fileClose(directory)
	if syncErr == nil {
		return closeErr
	}
	if !errors.Is(syncErr, windows.ERROR_ACCESS_DENIED) {
		return errors.Join(syncErr, closeErr)
	}

	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return errors.Join(syncErr, closeErr, err)
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return errors.Join(syncErr, closeErr, fmt.Errorf("open directory for sync: %w", err))
	}
	flushErr := windows.FlushFileBuffers(handle)
	closeHandleErr := windows.CloseHandle(handle)
	return errors.Join(closeErr, flushErr, closeHandleErr)
}
