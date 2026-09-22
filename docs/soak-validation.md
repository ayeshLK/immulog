# Soak validation guide

Use the soak in layers. A long run is valuable evidence, but it should not be
used as the only test for a concurrency or liveness fix.

## 1. Run deterministic regression tests first

For consumer lease and admission changes, run the focused tests repeatedly:

```sh
go test ./storage \
  -run 'Test(ConsumerPollKeepsLeaseWhileWaitingForStoreAdmission|GroupConsumerPollKeepsLeaseWhileWaitingForStoreAdmission)' \
  -count=100

go test -race ./storage \
  -run 'Test(ConsumerPollKeepsLeaseWhileWaitingForStoreAdmission|GroupConsumerPollKeepsLeaseWhileWaitingForStoreAdmission)' \
  -count=25
```

These tests hold `store.mu` longer than the progress timeout and verify that an
operation which started before expiry does not lose its assignment while waiting
for admission. They provide direct coverage of the fix without waiting for a
multi-hour workload to reach the same state.

## 2. Use a one-hour integration soak

A one-hour sustained run is a sensible fast integration gate. With the default
10-minute reopen interval it exercises several store lifecycles while producing
useful filesystem and retention pressure:

```sh
perf/soak/run.sh \
  --profile sustained \
  --duration 1h \
  --warmup 10m \
  --runs 2 \
  --producer-rate 1000 \
  --reopen-interval 10m \
  --churn-interval 0 \
  --analyze \
  --timeout 1h30m \
  --run-dir /path/to/dedicated/soak-validation
```

Use a fresh, dedicated directory for every run. Keep the automatic resource
preflights enabled. The `--runs 2` setting creates independent evidence
artifacts rather than reusing one process or data directory.

A one-hour run is an integration gate, not four-hour qualification evidence.
The failed admission run reached approximately 11 GiB and over 21,000 user
segments after about 3h41m; a shorter run may not reach the same cumulative
filesystem pressure.

## 3. Check the evidence

Accept a one-hour run only when every independent run satisfies all of these:

- process exit status is zero;
- `metrics.json` contains `"completed": true` and no failure text;
- no assignment loss or replacement timeout occurs;
- `samples_dropped` is zero;
- sampled backlog slope is non-positive;
- delivery and commit lag remain bounded;
- free space, open files, RSS, heap, and goroutines retain headroom.

Run the analyzer against each `metrics.json` or inspect the generated
`summary.md`. Preserve `metrics.json`, `summary.md`, `runs.tsv`, logs,
checkpoints, and environment captures with the result. Incomplete reports are
marked invalid and must not be used as qualification evidence.

## 4. Perform final qualification separately

After the deterministic tests and one-hour integration runs pass, perform the
release or milestone qualification using the selected rate and the same seed,
profile, filesystem, and workload configuration:

```sh
perf/soak/run.sh \
  --profile sustained \
  --duration 4h \
  --warmup 30m \
  --runs 3 \
  --producer-rate 1000 \
  --reopen-interval 10m \
  --churn-interval 0 \
  --analyze \
  --timeout 4h30m \
  --run-dir /path/to/dedicated/qualification
```

The four-hour run is still required when claiming four-hour durability or
liveness evidence. Do not replace it with a higher producer rate: that may
accelerate resource pressure, but it changes the workload and is stress
exploration rather than equivalent qualification.
