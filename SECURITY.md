# Security policy

## Reporting a vulnerability

Do not disclose suspected vulnerabilities in a public issue. Use GitHub private
vulnerability reporting or the Security Advisory interface for this repository.
Include affected versions, reproduction steps, impact, and suggested mitigation.
If private reporting is unavailable, contact the repository owner privately
without including exploit details in the first message.

## Supported versions

Before the first tagged release, only the current default branch receives
security fixes. Supported release lines will be documented after releases begin.

## Security boundary

`immulog` is a local, single-process durable storage library. It does not
provide network transport, authentication, authorization, encryption of segment
files, or protection against an operator who can modify the data directory.
Applications and deployments remain responsible for filesystem permissions,
at-rest encryption, and storage devices that honor flush requests.

Malformed records, corrupted storage, unbounded caller workloads, retained
internal references, ignored cancellation, and incorrect lock/retention handling
are security and availability concerns. Report them privately rather than
publishing a reproduction first.
