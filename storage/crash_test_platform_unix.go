//go:build !windows

// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"os"
	"os/exec"
	"syscall"
)

func terminateCrashHelper() error {
	return syscall.Kill(os.Getpid(), syscall.SIGKILL)
}

func crashTerminationExpected(exitErr *exec.ExitError) bool {
	status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}
