//go:build darwin

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
	"syscall"

	"github.com/ayeshLK/immulog/api"
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
	info, err := file.Stat()
	if err != nil {
		return dirIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return dirIdentity{}, errors.New("unsupported filesystem identity type")
	}
	return dirIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, nil
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
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("%w: LOCK is a symlink", api.ErrLockUnsupported)
		}
		return nil, fmt.Errorf("open LOCK: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, errors.New("create LOCK file handle")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat LOCK: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("%w: LOCK is not a regular file", api.ErrLockUnsupported)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, api.ErrDataDirLocked
		}
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOTSUP) {
			return nil, fmt.Errorf("%w: flock: %v", api.ErrLockUnsupported, err)
		}
		return nil, fmt.Errorf("acquire LOCK: %w", err)
	}
	return file, nil
}

func releaseLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
}
