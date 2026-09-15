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
	"io/fs"
	"os"
	"sync"
)

type fileSystemOps struct {
	mkdirAll  func(string, fs.FileMode) error
	open      func(string) (*os.File, error)
	openFile  func(string, int, fs.FileMode) (*os.File, error)
	createTmp func(string, string) (*os.File, error)
	readDir   func(string) ([]os.DirEntry, error)
	stat      func(string) (os.FileInfo, error)
	readFile  func(string) ([]byte, error)
	rename    func(string, string) error
	remove    func(string) error
	write     func(*os.File, []byte) (int, error)
	writeAt   func(*os.File, []byte, int64) (int, error)
	readAt    func(*os.File, []byte, int64) (int, error)
	statFile  func(*os.File) (os.FileInfo, error)
	sync      func(*os.File) error
	close     func(*os.File) error
	truncate  func(*os.File, int64) error
}

func operatingSystemFileSystem() fileSystemOps {
	return fileSystemOps{
		mkdirAll:  os.MkdirAll,
		open:      os.Open,
		openFile:  os.OpenFile,
		createTmp: os.CreateTemp,
		readDir:   os.ReadDir,
		stat:      os.Stat,
		readFile:  os.ReadFile,
		rename:    os.Rename,
		remove:    os.Remove,
		write:     func(file *os.File, data []byte) (int, error) { return file.Write(data) },
		writeAt:   func(file *os.File, data []byte, offset int64) (int, error) { return file.WriteAt(data, offset) },
		readAt:    func(file *os.File, data []byte, offset int64) (int, error) { return file.ReadAt(data, offset) },
		statFile:  func(file *os.File) (os.FileInfo, error) { return file.Stat() },
		sync:      func(file *os.File) error { return file.Sync() },
		close:     func(file *os.File) error { return file.Close() },
		truncate:  func(file *os.File, size int64) error { return file.Truncate(size) },
	}
}

var (
	fileSystemMu sync.RWMutex
	// fileSystem remains private so tests can model I/O failures without
	// expanding the storage package's public API.
	fileSystem = operatingSystemFileSystem()
)

func currentFileSystem() fileSystemOps {
	fileSystemMu.RLock()
	defer fileSystemMu.RUnlock()
	return fileSystem
}

func fsMkdirAll(path string, permission fs.FileMode) error {
	return currentFileSystem().mkdirAll(path, permission)
}

func fsOpen(path string) (*os.File, error) {
	return currentFileSystem().open(path)
}

func fsOpenFile(path string, flags int, permission fs.FileMode) (*os.File, error) {
	return currentFileSystem().openFile(path, flags, permission)
}

func fsCreateTemp(directory, pattern string) (*os.File, error) {
	return currentFileSystem().createTmp(directory, pattern)
}

func fsReadDir(path string) ([]os.DirEntry, error) {
	return currentFileSystem().readDir(path)
}

func fsStat(path string) (os.FileInfo, error) {
	return currentFileSystem().stat(path)
}

func fsReadFile(path string) ([]byte, error) {
	return currentFileSystem().readFile(path)
}

func fsRename(oldPath, newPath string) error {
	return currentFileSystem().rename(oldPath, newPath)
}

func fsRemove(path string) error {
	return currentFileSystem().remove(path)
}

func fileWrite(file *os.File, data []byte) (int, error) {
	return currentFileSystem().write(file, data)
}

func fileWriteAt(file *os.File, data []byte, offset int64) (int, error) {
	return currentFileSystem().writeAt(file, data, offset)
}

func fileReadAt(file *os.File, data []byte, offset int64) (int, error) {
	return currentFileSystem().readAt(file, data, offset)
}

func fileStat(file *os.File) (os.FileInfo, error) {
	return currentFileSystem().statFile(file)
}

func fileSync(file *os.File) error {
	return currentFileSystem().sync(file)
}

func fileClose(file *os.File) error {
	return currentFileSystem().close(file)
}

func fileTruncate(file *os.File, size int64) error {
	return currentFileSystem().truncate(file, size)
}

func syncDir(path string) error {
	directory, err := fsOpen(path)
	if err != nil {
		return err
	}
	syncErr := fileSync(directory)
	closeErr := fileClose(directory)
	return errors.Join(syncErr, closeErr)
}
