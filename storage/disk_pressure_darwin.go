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
	"fmt"
	"math"
	"syscall"
	"time"
)

func filesystemCapacityProbe(path string) func() (diskObservation, error) {
	return func() (diskObservation, error) {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(path, &stat); err != nil {
			return diskObservation{}, err
		}
		blockSize := uint64(stat.Bsize)
		availableBlocks := uint64(stat.Bavail)
		if blockSize == 0 || availableBlocks > math.MaxUint64/blockSize {
			return diskObservation{}, fmt.Errorf("invalid filesystem block capacity")
		}
		return diskObservation{availableBytes: availableBlocks * blockSize, availableInodes: uint64(stat.Ffree), inodesKnown: true, capturedAt: time.Now()}, nil
	}
}
