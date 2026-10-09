# Durability qualification matrix

This matrix records the distinction between deterministic durability tests,
native CI coverage, and a qualified deployment claim. It is maintained for
issue #67 and is intentionally conservative: a passing process-crash or fault
injection test does not reproduce every power-loss or device failure mode.

## Current qualification status

| Platform / filesystem | Native build and test CI | Deterministic storage faults | Process-crash recovery | Power-loss / torn-device experiment | Deployment status |
|---|---|---|---|---|---|
| Linux / ext4 | Covered by the core Linux jobs, including race and shuffled tests | Covered in `storage` through filesystem seams | Covered by `TestProcessCrashRecovery` and `TestPersistenceBoundaryCrashRecovery` | Not independently reproduced on a real device; sync semantics are assumed to be honored | Qualified only for the documented Linux contract and an honoring storage device |
| macOS / native filesystem | Covered by native macOS CI | Test coverage exists where the filesystem seam is portable | **Not completed**; native qualification evidence pending | Not tested | Build/test coverage only; not a qualified durability target |
| Windows / native filesystem | Covered by native Windows CI | Test coverage exists where the filesystem seam is portable | **Not completed**; native qualification evidence pending | Not tested | Build/test coverage only; not a qualified durability target |

The Linux status matches the normative specification: Linux is the only
currently qualified runtime. macOS and Windows CI demonstrate build and test
portability, not equivalent persistence guarantees.

## Linux baseline evidence — 2026-10-10

The merged `main` commit `8728522540ccdc9e258ae84a0e7703eec51e9514` passed the
deterministic Linux baseline on an Intel Core i7-10510U host with 8 logical
CPUs, Go `1.26.2`, Linux `7.0.0-38-generic`, `amd64`, and ext4 (68% used at
capture time). The worktree was clean; the open-file limits were 4096 soft and
1,048,576 hard.

| Command | Result |
|---|---|
| `go test ./storage -shuffle=on` | PASS (`23.487s`) |
| `go test -race ./storage` | PASS (`19.863s`) |
| `go test ./storage -run 'Test(ProcessCrashRecovery\|PersistenceBoundaryCrashRecovery)$' -count=10` | PASS (`13.753s`) |
| `go test ./...` | PASS |
| `go vet ./...` | PASS |

The repeated crash run passed ten repetitions of the process-crash and
persistence-boundary scenarios. Raw command output and environment capture are
preserved outside the repository as the run artifact for this evidence set.

This baseline validates deterministic fault injection and abrupt process
termination on Linux/ext4. It does not reproduce physical power loss,
controller write-cache failure, or every torn-write mode on a real device; the
Linux qualification claim remains conditional on a storage device honoring the
required write and synchronization operations.

Native macOS and Windows durability qualification is **not completed**. Their
CI jobs remain build/test coverage only; Docker-based Linux containers are not
substitutes for native filesystem and kernel evidence on those platforms.

## Existing deterministic coverage

The current suite covers these persistence transitions without mutating real
user data:

- append write and file-sync failures, including unknown outcomes and partition
  fencing;
- short writes and recovery of a verified incomplete final tail;
- segment creation, publication, rename, and directory-sync failures;
- bootstrap, catalog, topic-preparation, and snapshot publication failures;
- system-log compaction checkpoint, manifest, suffix, and cleanup boundaries;
- retention catalog/cleanup failures and durable log-start preservation;
- consumer commit failures, fencing, and durable resume behavior;
- process termination after append, sync, segment publication, retention,
  compaction, and recovery-boundary actions;
- refusal to repair complete corrupt batches, invalid manifests, or failed
  recovery mutations.

Representative tests include:

```text
TestProcessCrashRecovery
TestPersistenceBoundaryCrashRecovery
TestAppendPersistenceFaultsFencePartition
TestShortAppendWriteRecoversPermittedTail
TestSegmentRollPublicationFaultsPreservePriorRecord
TestRetentionRemoveFailurePreservesBoundaryAndReportsDebt
TestRetentionDirectorySyncFailurePreservesBoundary
TestCompactSystemLogsPublicationFailuresFenceUntilReopen
TestCompactSystemLogsPreservesRetentionBoundaryAndCleanupInventory
TestConsumerUnknownCommitFencesAllDependentGroupWork
```

Run the deterministic storage suite with:

```sh
go test ./storage -shuffle=on
go test -race ./storage
```

The process-crash tests use child processes and platform-specific termination
helpers. They validate recovery after abrupt process termination, not after
removing power from a physical device.

## Remaining qualification work

The following evidence is still required before expanding the deployment
claim:

1. Execute the deterministic matrix on the Linux release environment and
   retain the tested commit, filesystem, mount/device assumptions, commands,
   and artifacts.
2. Run opt-in controlled crash experiments across the append, commit,
   retention, and compaction boundaries; verify acknowledged records and
   commits after reopen without repairing complete corruption.
3. Decide whether macOS and Windows are deployment targets. If they are,
   repeat the durability matrix using their native filesystems and record
   platform-specific limitations; otherwise keep them explicitly outside the
   qualified durability contract.
4. Document the limits of process-kill and fault-injection evidence relative to
   actual power loss, controller caches, write barriers, and filesystem/device
   behavior.

No destructive device experiment is part of ordinary CI. Such experiments
MUST be opt-in, isolated from user data, and reported as evidence for a named
environment rather than as a device-independent guarantee.
