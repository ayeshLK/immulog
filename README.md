# immulog

<p align="center">
  <a href="https://pkg.go.dev/github.com/ayeshLK/immulog"><img src="https://pkg.go.dev/badge/github.com/ayeshLK/immulog.svg" alt="Go Reference"></a>
  <a href="https://github.com/ayeshLK/immulog/actions/workflows/ci.yml"><img src="https://github.com/ayeshLK/immulog/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://img.shields.io/github/license/ayeshLK/immulog"><img src="https://img.shields.io/github/license/ayeshLK/immulog" alt="Apache-2.0 license"></a>
</p>

> A local, durable, append-only, partitioned event log for Go applications.

`immulog` embeds a bounded event stream directly in an application. It stores
records in a durable filesystem log, assigns monotonic offsets per partition,
and reopens with conservative recovery after a process restart.

> [!WARNING]
> `immulog` is pre-v1. The public API and on-disk format are still evolving.
> Linux is the only currently qualified platform. Review the [usage
> guide](docs/usage.md) and [production guide](docs/production.md) before using
> it for important data.

## Why use immulog?

- Keep durable event history close to the application without operating a
  separate broker.
- Partition records for ordered, independently bounded append and fetch paths.
- Use caller-owned records, bounded admission, and explicit backpressure rather
  than unbounded queues.
- Resume local consumer progress from durable offsets after reopening the store.
- Apply time- or size-based retention while preserving a durable log-start
  boundary.
- Inspect append outcomes, lag, capacity, retention cleanup, and lifecycle state
  through bounded diagnostics.

## What it is—and is not

`immulog` is a filesystem-backed library for a local, single-process workload.
It is a good fit for embedded event history, local ingestion pipelines, durable
work queues within one process, and applications that need replay after restart.

It is not a network service, distributed log, replication system, high
availability layer, or multi-process coordination protocol. It does not provide
transport security, authentication, authorization, or encryption of segment
files. Applications and deployments remain responsible for filesystem
permissions, at-rest encryption, backups, and storage devices that honor flush
requests.

## Install

`immulog` requires Go 1.26 or newer. Until the first tagged release, pin a
reviewed commit or use the current module version during development:

```sh
go get github.com/ayeshLK/immulog
```

Import the public contracts and storage engine as separate packages:

```go
import (
	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)
```

## Quick start

The basic lifecycle is: open a directory, create a topic, append a record,
fetch it by offset, and close the store.

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/ayeshLK/immulog/api"
	"github.com/ayeshLK/immulog/storage"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "immulog-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := storage.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Print(err)
		}
	}()

	topic, err := store.CreateTopic("orders", 1, storage.PartitionOptions{})
	if err != nil {
		log.Fatal(err)
	}
	partitions, err := store.OpenTopic(topic.Name)
	if err != nil {
		log.Fatal(err)
	}

	record, err := partitions[0].Append(ctx, api.AppendRequest{
		Topic:     topic.ID,
		Partition: 0,
		Key:       []byte("order-1"),
		Value:     []byte(`{"status":"new"}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := partitions[0].Fetch(ctx, record.Offset, api.FetchOptions{
		MaxRecords: 10,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("fetched %d record(s), next offset is %d", len(result.Records), result.NextOffset)
}
```

`Append` copies the request, assigns the partition offset, and returns after
the durable append path completes. A context cancellation after the request
has entered the writer can return `api.ErrAppendOutcomeUnknown`; callers must
not assume that the record was rolled back or that its offset can be reused.

## Core concepts

- **Store:** owns one data directory and its stable `LOCK` file. Only one open
  store may own a directory at a time.
- **Topic:** a durable catalog entry with an immutable name, ID, and partition
  configuration.
- **Partition:** an ordered append-only stream. Offsets are assigned per
  partition and are never reused.
- **Durable end (`H`):** the next offset after the records known to be durable.
- **Log start (`L`):** the first retained offset after retention advances the
  boundary.
- **Consumer:** a same-process assignment that polls records and explicitly
  commits a next offset. Delivery is at-least-once.

Records and fetch results are caller-owned. Copy data that must outlive the
operation or consumer handler; do not retain internal references.

## Choose a guide

- [Usage guide](docs/usage.md): open stores, create topics, append and fetch
  records, read with cursors, consume with commits, handle errors, and reopen.
- [Production guide](docs/production.md): choose limits, plan disk capacity,
  understand durability and recovery, operate retention, monitor diagnostics,
  and shut down safely.
- [Contributing](CONTRIBUTING.md): development setup, validation commands,
  pull-request expectations, and release hygiene.
- [Benchmark evidence](BENCHMARKS.md): reproducible performance commands,
  dated results, soak metrics, and qualification limits.
- [Security policy](SECURITY.md): private vulnerability reporting and the
  library's security boundary.
- [Code of Conduct](CODE_OF_CONDUCT.md): expectations for respectful project
  participation and private conduct reporting.
- [Go package reference](https://pkg.go.dev/github.com/ayeshLK/immulog): the
  complete exported API.

## Development checks

Run the standard checks from the repository root:

```sh
go test ./...
go vet ./...
go test -race ./...
```

Fuzzing, benchmark, and the mixed-workload soak are intentionally separate
from ordinary CI. Their commands are in [CONTRIBUTING.md](CONTRIBUTING.md),
and benchmark interpretation and results are in
[BENCHMARKS.md](BENCHMARKS.md).

## License

Copyright 2026 Ayesh Almeida. Licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE).
