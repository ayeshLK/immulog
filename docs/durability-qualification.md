# Durability qualification matrix

This matrix records the distinction between deterministic durability tests,
native CI coverage, and a qualified deployment claim. It is maintained for
issue #67 and is intentionally conservative: a passing process-crash or fault
injection test does not reproduce every power-loss or device failure mode.

## Current qualification status

| Platform / filesystem | Native build and test CI | Deterministic storage faults | Process-crash recovery | Power-loss / torn-device experiment | Deployment status |
|---|---|---|---|---|---|
| Linux / ext4 | Covered by the core Linux jobs, including race and shuffled tests | Covered in `storage` through filesystem seams | Covered by `TestProcessCrashRecovery` and `TestPersistenceBoundaryCrashRecovery` | Not independently reproduced on a real device; sync semantics are assumed to be honored | Qualified only for the documented Linux contract and an honoring storage device |
| macOS / native filesystem | Covered by native macOS CI | Test coverage exists where the filesystem seam is portable | Native qualification evidence not yet recorded | Not tested | Build/test coverage only; not a qualified durability target |
| Windows / native filesystem | Covered by native Windows CI | Test coverage exists where the filesystem seam is portable | Native qualification evidence not yet recorded | Not tested | Build/test coverage only; not a qualified durability target |

The Linux status matches the normative specification: Linux is the only
currently qualified runtime. macOS and Windows CI demonstrate build and test
portability, not equivalent persistence guarantees.

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
