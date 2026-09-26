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
	"time"

	"golang.org/x/sys/windows"
)

func filesystemCapacityProbe(path string) func() (diskObservation, error) {
	return func() (diskObservation, error) {
		name, err := windows.UTF16PtrFromString(path)
		if err != nil {
			return diskObservation{}, err
		}
		var available, total, free uint64
		if err := windows.GetDiskFreeSpaceEx(name, &available, &total, &free); err != nil {
			return diskObservation{}, err
		}
		return diskObservation{availableBytes: available, inodesKnown: false, capturedAt: time.Now()}, nil
	}
}
