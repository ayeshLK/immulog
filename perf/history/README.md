# Long-history recovery runner

`perf/history` is an opt-in evidence harness for issue #68. It creates a
fresh store with a controlled number of records and segment rolls, closes it,
and measures a cold `storage.Open`. The JSON report includes the generated
segment count and log bytes, reopen duration, Go heap measurements, Linux RSS
measurements when available, and snapshot validation diagnostics.

Run a small matrix outside the repository:

```sh
perf/history/run.sh \
  --run-dir /tmp/immulog-history-$(date +%Y%m%d-%H%M%S) \
  --records 1000,10000,100000 \
  --partitions 1 \
  --segment-bytes 65536 \
  --batch-records 32 \
  --snapshots
```

The runner produces baseline, snapshot-present, and snapshot-invalid/replay
cases when `--snapshots` is set. Every case has its own preserved `data`
directory and `report.json`; do not delete the artifacts before interpreting
the result. Use the same commit, filesystem, Go version, payload, segment
size, and partition count when comparing cases.

The measurements are process observations, not a library memory bound. Heap
values include the harness and Go runtime, RSS is Linux-only, and a single
open measurement is not sufficient for qualification. Repeat cases and use
the reports to identify scaling or regression candidates before recording
results in `BENCHMARKS.md`.
