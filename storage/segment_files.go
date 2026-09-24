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
	"os"
	"sync"
)

// segmentFileCache bounds how many sealed segments hold an open descriptor.
// A partition keeps its own writer handle for the active segment; every older
// segment is immutable, so a reader opens it on demand and the cache closes
// the least recently used handle once the budget is exceeded. Without this,
// a store held one descriptor per segment for as long as it stayed open and a
// long-lived log eventually exhausted the process file limit.
type segmentFileCache struct {
	mu      sync.Mutex
	limit   int
	clock   uint64
	entries map[string]*segmentFileEntry
}

// segmentFileEntry is one cached descriptor. refs counts the readers currently
// using it, so eviction never closes a handle out from under a fetch.
type segmentFileEntry struct {
	file *os.File
	refs int
	used uint64
}

func newSegmentFileCache(limit uint32) *segmentFileCache {
	return &segmentFileCache{limit: int(limit), entries: make(map[string]*segmentFileEntry, limit)}
}

// acquire pins a read handle for path. Every successful call must be followed
// by exactly one release.
func (cache *segmentFileCache) acquire(path string) (*os.File, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.entries[path]
	if entry == nil {
		file, err := fsOpen(path)
		if err != nil {
			return nil, err
		}
		entry = &segmentFileEntry{file: file}
		cache.entries[path] = entry
	}
	entry.refs++
	cache.clock++
	entry.used = cache.clock
	cache.evictLocked()
	return entry.file, nil
}

// release unpins a handle and lets the budget reclaim it.
func (cache *segmentFileCache) release(path string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.entries[path]
	if entry == nil {
		return
	}
	if entry.refs > 0 {
		entry.refs--
	}
	cache.evictLocked()
}

// evictLocked closes unpinned handles, least recently used first, until the
// cache fits its budget. Pinned handles are skipped, so a burst of concurrent
// readers can briefly exceed the budget rather than break an in-flight read.
// The scan is linear in the cache size and only runs while over budget.
func (cache *segmentFileCache) evictLocked() {
	for len(cache.entries) > cache.limit {
		var victimPath string
		var victim *segmentFileEntry
		for path, entry := range cache.entries {
			if entry.refs != 0 {
				continue
			}
			if victim == nil || entry.used < victim.used {
				victimPath, victim = path, entry
			}
		}
		if victim == nil {
			return
		}
		delete(cache.entries, victimPath)
		_ = fileClose(victim.file)
	}
}

// forget drops any cached handle for a path that is about to be deleted.
func (cache *segmentFileCache) forget(path string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.entries[path]
	if entry == nil {
		return
	}
	delete(cache.entries, path)
	_ = fileClose(entry.file)
}

// closeAll releases every cached handle as the store shuts down.
func (cache *segmentFileCache) closeAll() error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	var closeErr error
	for path, entry := range cache.entries {
		closeErr = errors.Join(closeErr, fileClose(entry.file))
		delete(cache.entries, path)
	}
	return closeErr
}

// size reports the cached descriptor count for diagnostics and tests.
func (cache *segmentFileCache) size() int {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return len(cache.entries)
}

// acquireSegmentFile returns a readable handle for one segment and the release
// that must follow it. The active segment keeps the writer handle it was
// created with; sealed segments come from the store's bounded cache, or from a
// direct open when a partition runs without a store.
func (p *Partition) acquireSegmentFile(target *segment) (*os.File, func(), error) {
	if target.file != nil {
		return target.file, func() {}, nil
	}
	if p.store != nil && p.store.segmentFiles != nil {
		cache := p.store.segmentFiles
		file, err := cache.acquire(target.path)
		if err != nil {
			return nil, nil, err
		}
		return file, func() { cache.release(target.path) }, nil
	}
	file, err := fsOpen(target.path)
	if err != nil {
		return nil, nil, err
	}
	return file, func() { _ = fileClose(file) }, nil
}

// forgetSegmentFile drops a cached handle for a segment leaving this store.
func (p *Partition) forgetSegmentFile(path string) {
	if p.store != nil && p.store.segmentFiles != nil {
		p.store.segmentFiles.forget(path)
	}
}

// releaseSealedHandles closes the writer handles of every segment except the
// active one. Recovery opens each segment to rebuild its batch map; keeping
// those descriptors afterwards is what made open descriptors grow with the log.
func (p *Partition) releaseSealedHandles() error {
	if len(p.segments) == 0 {
		return nil
	}
	var closeErr error
	for _, sealed := range p.segments[:len(p.segments)-1] {
		if sealed.file == nil {
			continue
		}
		closeErr = errors.Join(closeErr, fileClose(sealed.file))
		sealed.file = nil
	}
	return closeErr
}
