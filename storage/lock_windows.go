//go:build windows

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
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/ayeshLK/immulog/api"
	"golang.org/x/sys/windows"
)

type dirIdentity struct {
	device uint64
	inode  uint64
}

var directoryOwners struct {
	sync.Mutex
	items map[dirIdentity]struct{}
}

func init() { directoryOwners.items = make(map[dirIdentity]struct{}) }

func identityOf(file *os.File) (dirIdentity, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return dirIdentity{}, err
	}
	index := uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)
	return dirIdentity{device: uint64(info.VolumeSerialNumber), inode: index}, nil
}

func reserveDirectory(identity dirIdentity) bool {
	directoryOwners.Lock()
	defer directoryOwners.Unlock()
	if _, exists := directoryOwners.items[identity]; exists {
		return false
	}
	directoryOwners.items[identity] = struct{}{}
	return true
}

func releaseDirectory(identity dirIdentity) {
	directoryOwners.Lock()
	delete(directoryOwners.items, identity)
	directoryOwners.Unlock()
}

func acquireLock(path string) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("encode LOCK path: %w", err)
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE, 0, nil, windows.OPEN_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		if errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, api.ErrDataDirLocked
		}
		return nil, fmt.Errorf("open LOCK: %w", err)
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("stat LOCK: %w", err)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("%w: LOCK is a reparse point", api.ErrLockUnsupported)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("%w: LOCK is not a regular file", api.ErrLockUnsupported)
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, errors.New("create LOCK file handle")
	}
	return file, nil
}

func releaseLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Close()
}
