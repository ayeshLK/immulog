# Long-history recovery runner

`perf/history` is an opt-in evidence harness for issue #68. It creates a
fresh store with a controlled number of records and segment rolls, closes it,
and measures a cold `storage.Open`. It can also grow the catalog and consumer
offset logs so snapshot comparisons exercise the system-log projections they
actually accelerate. The JSON report includes the generated segment count and
log bytes, reopen duration, Go heap measurements, Linux RSS measurements when
available, and snapshot validation diagnostics.

Run a small matrix outside the repository:

```sh
perf/history/run.sh \
  --run-dir /tmp/immulog-history-$(date +%Y%m%d-%H%M%S) \
  --records 1000,10000,100000 \
  --partitions 1 \
  --segment-bytes 65536 \
  --batch-records 32 \
  --catalog-topics 100 \
  --consumer-commits 1000 \
  --runs 3 \
  --snapshots
```

`--catalog-topics` is the total topic count, including the data-bearing topic.
`--consumer-commits` first consumes partition zero and then writes that many
monotonically increasing durable commits; it cannot exceed the smallest record
case. Use zero for either option when isolating user-segment history.

The runner produces baseline, valid-snapshot, and invalid-snapshot/replay cases
when `--snapshots` is set. Invalid cases corrupt both system snapshots so both
fallback paths are observable. Each independent run and case has its own
preserved `data` directory and `report.json`. The run root also contains the
effective configuration, exact commands, statuses, commit/worktree state, Go
and host details, and filesystem metadata. Do not delete the artifacts before
interpreting the result.

The measurements are process observations, not a library memory bound. Heap
values include the harness and Go runtime, RSS is Linux-only, and a single open
measurement is not sufficient for qualification. Compare like-for-like runs
using the same commit, filesystem, Go version, payload, segment size, topic and
commit history, and partition count. Prefer a clean worktree for recorded
evidence, and use the repeated reports to identify scaling or regression
candidates before recording results in `BENCHMARKS.md`.
