# Cluster coordination design

**Status:** future architecture proposal. This document does not add network,
replication, or distributed-coordination behavior to the current storage slice.

## Purpose and boundary

`immulog` is an embedded local storage engine. One `Store` owns one data
directory and its stable `LOCK`; one process owns each local partition writer.
The local engine is responsible for authoritative filesystem bytes, local
offset order, segment rolling, fsync, recovery, retention, and same-process
fetch/consumer APIs.

Cluster mode should add a broker/runtime layer around that engine rather than
make the storage package aware of sockets, membership, or consensus. A node
should continue to own its own data directory. Distributed ownership is a
logical cluster concern and must never be implemented by allowing multiple
processes to open the same directory.

```text
client application
        |
        v
client transport endpoint
        |
        v
broker / coordination runtime <---- peer transport ----> other nodes
        |
        +---------------------> replication manager
        |                               |
        v                               v
cluster control plane              local storage
```

Client applications and peer nodes communicate through transport endpoints.
The broker/coordination runtime is the only layer that translates those
messages into storage operations. The storage engine has no client or transport
knowledge. The detailed designs are consolidated below in this document.

The transport is a delivery mechanism. Coordination is the protocol and state
machine that uses it. Keeping those concepts separate allows an in-process,
Unix-domain, or network transport without coupling the storage engine to one
choice.

## Document map

This is the single authoritative coordination proposal. Its detailed sections
are ordered from the external boundary inward:

1. Transport extension point.
2. Broker/runtime boundary.
3. Metadata coordination state machine.
4. Metadata quorum lifecycle.
5. Partition replication protocol.

The current storage engine remains standalone throughout. All client and
peer communication terminates in transport endpoints; only broker, cluster,
and replication runtime components call storage APIs.

## Responsibilities by layer

### Local storage engine

The existing `storage` package remains responsible for:

- authoritative segmented log files and stable `LOCK` ownership;
- one ordered durable writer per local partition;
- local durable end (`H`), log-start boundary (`L`), and offset continuity;
- explicit write, file-sync, recovery, and unknown-outcome semantics;
- rebuildable indexes, snapshots, tail cache, and retention artifacts; and
- local read and consumer primitives for broker/runtime adapters.

It must not make cluster membership, leader election, or network calls.

### Transport plugin

A future transport package should own both application-facing and node-to-node
communication. It should expose generic message delivery capabilities,
illustratively:

```go
type Transport interface {
	Request(context.Context, NodeID, Message) (Message, error)
	OpenStream(context.Context, NodeID, StreamKind) (Stream, error)
}
```

The final API may differ, but the transport contract should cover:

- authenticated node identity and peer authorization hooks;
- request/response messages and long-lived replication streams;
- deadlines, cancellation, bounded buffering, and backpressure;
- connection establishment, closure, and failure classification; and
- protocol/version negotiation.

The transport must not interpret topics, offsets, leaders, replicas, epochs, or
quorum decisions. It should not silently retry non-idempotent messages.

### Broker/runtime boundary

The broker/runtime layer owns client-facing handlers and routing. It validates
client requests, consults committed cluster metadata, applies authorization and
fencing rules, and translates accepted operations into replication and storage
calls. It returns domain results and errors through the client transport.

Client applications never open a storage directory, read log files, or call
storage internals. Storage receives operations only from trusted local runtime
components, regardless of whether the original request came from a client or a
peer.

### Cluster control plane

The control plane owns a replicated metadata state machine. It coordinates:

- node registration and liveness sessions;
- cluster and metadata-log epochs;
- topic and partition configuration;
- replica assignments and placement constraints;
- partition leader and leader epoch;
- replica membership and reassignment progress; and
- fencing and administrative state transitions.

Kafka's KRaft controller quorum is a useful reference: metadata is committed by
a control quorum, while ordinary record traffic stays on the partition data
path. The controller must not sit in the hot path of every append.

### Partition replication runtime

The replication runtime binds a local `Partition` to a cluster assignment. It
owns the partition state machine and uses the transport to exchange data with
replicas:

```text
unassigned -> recovering -> follower
                              |
                              v
                            leader
                              |
                              v
                         draining/fenced
```

Only the current leader accepts client writes. Followers append the leader's
explicitly ordered batches and report their durable position. A leader may
acknowledge a replicated append only after the configured replica quorum has
locally persisted it.

### Consumer-group coordination

Consumer-group membership, assignment, heartbeats, and committed next offsets
are a separate coordination domain. It may use the same metadata quorum and
transport, but it should not be coupled to partition leader election or
replication state. Consumer visibility should normally be limited to the
partition's committed/high-watermark boundary, not merely its local durable
end.

## Metadata model

The minimum cluster metadata for each partition is:

```text
partition key
replica set
leader node
leader epoch
replica role/state
ISR or acknowledged replica set
assignment revision
committed/high watermark
```

Updates must be versioned and applied monotonically. A broker must reject an
update whose assignment revision or leader epoch is stale. Metadata publication
should be atomic from the broker's perspective: a new assignment is not
routable until the control plane has committed it.

The metadata quorum should be the authority for ownership. Local files are the
authority for bytes that a node has durably stored, but local bytes alone do
not prove that the node is still the current leader.

## Quorum-durable append

For a replication factor of `N`, a configured quorum `Q` acknowledges a batch
only after:

1. the leader validates its current assignment and leader epoch;
2. the leader appends and synchronizes the batch locally;
3. followers append the same offsets and synchronize locally;
4. at least `Q` assigned replicas report the required durable position;
5. the leader advances the committed/high-watermark boundary; and
6. the client acknowledgement is emitted.

Local durable end `H` and committed end `C` are distinct:

```text
L <= C <= H
```

A locally durable but uncommitted record may be retained for recovery, but
ordinary consumers must not observe it as committed data. If quorum is lost,
new writes may be rejected or remain unacknowledged according to the chosen
policy; they must not be reported as quorum-durable.

## Fencing and split-brain prevention

Network partitions make stale leadership the central safety risk. Every data
operation that can change or expose partition state must carry the leader epoch
or an equivalent fencing token. The local append boundary must validate that
token close enough to the serialized writer operation that a stale leader
cannot pass a check and append after being fenced.

A safe transition is:

```text
controller commits new leader epoch
        |
        v
old leader is fenced/rejects new data operations
        |
        v
new leader recovers an allowed replica prefix
        |
        v
new leader advances commitment after quorum forms
```

Fencing must be monotonic and durable wherever a restart could otherwise revive
an old owner. A lease or heartbeat alone is not sufficient unless its expiry
and fencing interaction are defined for pauses and network partitions.

## Storage capabilities needed by a future runtime

The current local APIs already provide useful foundations such as explicit
batch base offsets, durable append completion, fetch, and recovery. A cluster
runtime will eventually need narrow, transport-neutral capabilities for:

- appending a follower batch at an explicit offset and validating its range;
- reading ordered batches from a replication cursor;
- exposing local durable end and committed visibility separately;
- fencing or sealing a partition for a stale leader epoch;
- truncating a follower's divergent uncommitted suffix safely; and
- applying assignment/lifecycle transitions without bypassing `LOCK`, disk
  admission, recovery, or completion semantics.

These capabilities should be designed as storage contracts or broker-owned
adapters. They should not accept a transport object or perform RPCs. Any new
mutation must preserve caller ownership, offset non-reuse, crash recovery, and
unknown-outcome rules.

## Failure behavior

### Broker crash

The control plane detects the failed session, advances the partition epoch,
and elects an eligible replica. The new leader recovers its local log, finds
the committed prefix, and serves only after the assignment is committed.

### Network partition

A leader without quorum cannot claim quorum-durable acknowledgement. A stale
leader receiving requests after its epoch is fenced must reject them. The
control plane must prevent two sides from both becoming authoritative leaders.

### Follower lag or failure

The leader may continue only while the configured acknowledgement policy is
satisfied. Replica lag, removal from the ISR, catch-up, and re-entry must be
visible in metadata. Rejoining replicas must reconcile from a committed or
explicitly permitted prefix rather than blindly append divergent data.

### Unknown outcomes

A timeout after a leader or follower may have written is not evidence that the
batch did not exist. Retries require an idempotency strategy or reconciliation
against the partition's committed log. The cluster layer must not roll back or
reuse offsets merely because a transport response was lost.

## Recommended package boundaries

A future implementation should keep dependencies pointed toward abstractions:

```text
storage       local filesystem engine; no transport dependency
transport     pluggable node-to-node communication primitives
cluster       metadata quorum, membership, epochs, assignments
replication   leader/follower protocol using storage + transport contracts
broker        client-facing runtime that binds the pieces
```

The exact package names are not frozen. The important constraints are that
`storage` remains usable without cluster packages, transport remains unaware of
log semantics, and the control plane is not required for every local fetch or
append once ownership and policy are already established.

## Staged implementation gates

1. **Contracts only:** define node identity, assignment, epochs, metadata
   revisions, transport messages, and storage visibility/fencing boundaries.
2. **Deterministic coordinator:** implement an in-memory/static coordinator and
   model crash, pause, stale epoch, reassignment, and quorum decisions without
   networking.
3. **Metadata quorum:** choose and qualify a consensus-backed metadata log;
   prove monotonic revisions, restart behavior, and split-brain fencing.
4. **Replica protocol:** add follower recovery, explicit-offset replication,
   high-watermark advancement, truncation, and quorum acknowledgements.
5. **Transport adapters:** qualify an in-process test transport first, then any
   Unix-domain or network transport with authentication and backpressure.
6. **Broker API:** add client routing, consumer-group coordination, and
   operational reassignment only after the lower layers pass durability and
   failure qualification.

No stage should weaken the standalone engine or introduce network code into the
current storage slice. Kafka wire compatibility is optional and should not be a
prerequisite for validating the coordination model.

## Open design decisions

Before implementation, record explicit decisions for:

- metadata quorum size and consensus implementation;
- replica factor and quorum definition;
- whether uncommitted leader writes are retained after failover;
- idempotent producer/retry identity;
- assignment placement and rebalancing policy;
- consumer visibility and offset-commit semantics during failover; and
- authentication, authorization, encryption, and transport limits.

The default recommendation is a quorum-backed metadata control plane, crash-
stop and network-partition safety, quorum-durable acknowledgements, monotonic
leader epochs, and a purpose-built transport-neutral API rather than immediate
Kafka protocol compatibility.


## Detailed design sections

### Transport extension point

boundary and a deterministic test model; it does not add production networking
to `immulog`.

### Purpose

Client applications and cluster nodes communicate through transport endpoints.
The transport layer owns those communication boundaries, while the broker and
coordination runtime translates received messages into domain operations.

```text
client application ---> client transport endpoint ---+
                                                      |
other node <------ peer transport endpoint -----------+--> broker/runtime
                                                               |
                                                               v
                                                             storage
```

The local storage engine remains usable without a network dependency. It does
not know whether a request originated from a client, another node, or an
in-process caller. It receives only storage/runtime operations from the broker
or replication layer.

The transport moves protocol messages. It does not interpret topics, offsets,
partition leaders, replica sets, epochs, ISR, or quorum decisions.

### Goals

The first transport contract must provide:

- client-facing request/response communication;
- peer request/response communication for control operations;
- long-lived bounded peer streams for replication;
- authenticated peer identity where a peer endpoint is used;
- deadlines, cancellation, and failure classification;
- explicit backpressure and message-size limits;
- protocol/version negotiation hooks; and
- a deterministic in-memory implementation for tests.

### Non-goals

The transport contract does not define:

- metadata consensus;
- leader election or fencing;
- quorum acknowledgement;
- record encoding or offset assignment;
- retry policy for non-idempotent operations;
- consumer-group assignment; or
- Kafka wire compatibility.

Those belong to the cluster, replication, or broker layers.

### Illustrative contract

The final public names are not frozen, but the shape should remain small and
transport-neutral. Client and peer communication are distinct endpoint roles
within the transport layer:

```go
type NodeID [16]byte

type StreamKind uint8

const (
	StreamControl StreamKind = iota
	StreamReplication
)

type Envelope struct {
	Type      uint16
	RequestID uint64
	Payload   []byte
}

type ClientTransport interface {
	Request(context.Context, Envelope) (Envelope, error)
	Close() error
}

type PeerTransport interface {
	PeerID() NodeID
	Request(context.Context, NodeID, Envelope) (Envelope, error)
	OpenStream(context.Context, NodeID, StreamKind) (Stream, error)
	Serve(context.Context, PeerHandler) error
	Close() error
}

type Stream interface {
	Send(context.Context, Envelope) error
	Recv(context.Context) (Envelope, error)
	Close() error
}

type PeerHandler interface {
	HandleRequest(context.Context, NodeID, Envelope) (Envelope, error)
	HandleStream(context.Context, NodeID, StreamKind, Stream) error
}
```

A client transport may use a route, broker address, or session established by
its implementation; it does not need a `NodeID`. The broker/runtime owns the
handlers that translate client messages into cluster, replication, and storage
operations. The storage package is not a handler and never imports either
transport endpoint.

The contract may use typed codecs instead of raw payloads, but the transport
must not import storage or cluster domain types. Payload ownership must be
explicit: implementations must not retain caller-owned envelope bytes after a
method returns unless documented as copied or transferred.

### Semantic rules

#### Request/response

- A request has a caller-supplied context and request identity.
- The transport must not automatically retry a request after an ambiguous
  write; the cluster protocol owns idempotency and reconciliation.
- Requests to one peer have no ordering guarantee unless a higher-level
  protocol provides one.
- A response may be unavailable even when the peer processed the request.
- Context cancellation stops waiting locally; it is not proof that the peer
  stopped processing remotely.

#### Streams

- Frames on one stream direction are delivered in send order.
- Ordering across independent streams is unspecified.
- `Send` applies bounded backpressure and must return an error when the stream
  or context is closed.
- `Recv` returns a classified close/error result rather than blocking forever.
- A stream close is idempotent; implementations must release all resources.
- Replication protocols must include their own sequence, epoch, and offset
  fields; stream ordering alone is not a durability or fencing guarantee.

#### Cancellation and shutdown

Transport shutdown must:

1. stop accepting new requests and streams;
2. unblock pending `Request`, `Send`, and `Recv` calls;
3. return a stable close error to operations not completed; and
4. wait for handler goroutines before `Close` returns, or document an explicit
   asynchronous close contract.

A canceled operation may have reached the peer. Higher layers must classify
that outcome as unknown when it can have caused a durable mutation.

#### Errors

The transport should expose a small stable taxonomy, for example:

```text
ErrPeerUnavailable
ErrRequestTimeout
ErrCanceled
ErrTransportClosed
ErrStreamClosed
ErrMessageTooLarge
ErrProtocolViolation
ErrAuthentication
```

`ErrNotLeader`, stale epoch, insufficient quorum, and offset mismatch are
protocol/domain errors and must be carried by cluster or replication messages,
not disguised as transport failures.

### Identity and security boundary

A transport implementation authenticates the remote peer and exposes its
validated `NodeID` to handlers. It may provide TLS, Unix credentials, or an
in-process identity mechanism, but the cluster layer decides whether that node
is authorized for a requested operation.

The transport should support a negotiated protocol version and bounded maximum
frame size. Authentication, authorization, encryption, and rate limits must be
observable configuration rather than implicit behavior.

### Backpressure and resource limits

Every transport implementation must bound:

- concurrent requests per peer;
- queued request bytes;
- queued stream frames;
- maximum envelope/frame size; and
- active streams.

When a bound is reached, `Send` or `Request` should block until context
cancellation or return a classified resource error according to the configured
policy. It must not grow an unbounded queue behind the storage writer.

Control streams should be isolated from replication streams so a large or slow
replication transfer cannot prevent membership, fencing, or metadata traffic.

### Loopback transport model

Before using a real network, implement an in-memory loopback transport for
protocol tests. A pair of endpoints should share only a deterministic test
scheduler and bounded channels:

```text
endpoint A <-> bounded link <-> endpoint B
```

The loopback implementation should support test-controlled:

- delivery delay;
- drop, duplicate, and reorder actions;
- request and stream backpressure;
- peer close and restart;
- cancellation while a frame is queued; and
- maximum frame/queue limits.

The normal mode should deliver frames in order with no unnecessary scheduling
noise. Fault actions must be explicit per test so failures are reproducible.

The loopback transport is not a production network simulator and must not be
used to claim network performance. Its purpose is to validate cluster and
replication state machines against deterministic transport outcomes.

### Required contract tests

Every transport implementation should pass the same contract suite:

- request/response success and peer identity;
- request timeout and cancellation;
- ambiguous request completion without automatic retry;
- stream ordering in each direction;
- bounded send backpressure;
- frame-size rejection;
- peer close unblocking pending operations;
- transport close unblocking all operations;
- handler panic/error containment;
- protocol-version mismatch; and
- resource cleanup with no goroutine or stream leak.

The loopback suite should additionally inject drop, delay, duplicate, reorder,
and restart events. Replication tests should prove that duplicate or reordered
frames are rejected or safely reconciled by the higher-level protocol.

### Client boundary

A client application is coupled to a client transport endpoint, not to a
storage directory or storage API. Client requests terminate at the broker or
service runtime, which performs routing, authorization, epoch checks, quorum
handling, and translation to local storage/replication operations.

The client transport may be embedded in an application library or implemented
by a remote protocol client, but both are transport concerns. Client-visible
errors such as not-leader, stale metadata, quorum-unavailable, and unknown
outcome are produced by the broker protocol; they are not storage errors.

### Package and dependency rules

A future implementation should follow these dependency directions:

```text
transport  <- concrete communication implementations
cluster    <- transport interfaces and metadata state machine
replication <- transport interfaces + storage capabilities
broker     <- cluster + replication + client API
storage    <- no transport, cluster, or broker package
```

The public transport interface should stabilize before the cluster and
replication APIs. A loopback implementation can live in a test-support package
until a real external transport needs a supported production implementation.

### Open decisions

Before implementation, decide:

- whether `Serve` is part of the public transport interface or owned by a
  separate listener/server abstraction;
- whether streams support half-close and independent send/receive deadlines;
- whether request IDs are transport-generated or protocol-supplied;
- whether peer authentication is mandatory for every implementation; and
- which metrics and tracing hooks are part of the stable contract.

The default recommendation is a small bidirectional transport interface with
bounded request/stream operations, explicit peer identity, no automatic retry,
and a deterministic loopback implementation used to qualify the higher-level
coordination protocols.

### Broker/runtime boundary

connects client transport endpoints to coordination, replication, and local
storage. It does not add a client protocol or broker implementation.

### Boundary

The broker/runtime is the only layer that translates client requests into
cluster and storage operations:

```text
client application
        |
        v
client transport endpoint
        |
        v
broker/runtime
   |         |          |
   v         v          v
cluster  replication  storage
control  runtime      adapter
```

A client application does not open a storage directory, inspect segment files,
call a partition object, or learn the local filesystem layout. The runtime owns
routing, authorization, admission control, request lifecycle, and translation
to internal APIs.

Peer-to-peer replication and metadata traffic use the peer transport endpoint
described in the transport extension section above. A peer message must not be accepted through a
client handler merely because its envelope has a similar shape.

### Responsibilities

The runtime owns:

- client protocol decoding and version negotiation;
- client identity, authentication, and authorization context;
- topic/partition routing using committed metadata;
- request deadlines, cancellation, and admission control;
- translation between client messages and internal domain operations;
- mapping internal outcomes to stable client-visible errors;
- broker lifecycle states such as serving, draining, and fenced; and
- client-facing observability and quotas.

The runtime does not own:

- segment encoding or filesystem recovery;
- consensus log ordering;
- partition quorum calculation;
- direct peer replication state; or
- authoritative cluster metadata.

Those responsibilities remain in storage, the metadata control plane, and the
replication runtime respectively.

### Broker lifecycle

A broker should expose client service only after its local projection is valid:

```text
starting
   -> loading metadata
   -> recovering local storage
   -> serving
   -> draining
   -> stopped

serving --fenced--> degraded/fenced
```

#### Starting

The broker validates cluster identity, loads committed metadata, recovers local
storage, and installs assignment/epoch guards. Client requests that require
cluster authority are rejected until this completes.

#### Serving

The broker accepts requests for assignments authorized by its current metadata
revision and node session. A partition is routable only when its local runtime
has installed the required role and leader epoch.

#### Draining

The broker stops accepting new work that would extend its ownership, finishes
or explicitly resolves in-flight requests, and reports its state to the control
plane. Existing committed reads may continue according to policy.

#### Degraded or fenced

The broker rejects operations that require current authority. It may expose
clearly marked read-only committed data if its last committed projection is
still valid. It must not silently route writes using stale metadata.

### Request lifecycle

Every client request follows one controlled path:

```text
receive transport envelope
        |
        v
authenticate and decode
        |
        v
validate protocol, limits, and deadline
        |
        v
load committed metadata snapshot
        |
        v
route to local or remote owner
        |
        v
authorize and apply admission policy
        |
        v
invoke coordination/replication/storage adapter
        |
        v
translate outcome into client response
```

The storage adapter receives a domain request, not a client envelope. It must
not receive a network connection, client socket, client credential object, or
transport implementation.

The runtime should capture the metadata revision and leader epoch used for
routing. If either becomes stale before the serialized operation boundary, the
operation must be rejected or reconciled rather than applied under an old view.

### Client operations

The initial client-facing contract should be deliberately small and domain
oriented. Illustrative operations are:

#### Metadata lookup

Returns the committed partition assignment, leader hint, assignment revision,
and relevant protocol capabilities. A metadata response is a routing hint, not
a permission to bypass broker authorization or epoch checks.

#### Append

The client supplies a topic/partition key or routing key and records. The
runtime:

1. resolves the current leader from committed metadata;
2. routes to the leader if local ownership is absent;
3. validates request identity and limits;
4. invokes the replication runtime;
5. waits for the configured quorum-durable result; and
6. returns offsets only when their acknowledgement contract is satisfied.

The response must distinguish committed success, known rejection, and unknown
outcome. It must not claim local success merely because the leader buffered or
locally persisted a batch when the client requested quorum durability.

#### Fetch

The client supplies a topic/partition and offset. The runtime routes to an
authorized broker and reads only through the committed high watermark `C`:

```text
visible range = [L, C)
```

A local `H > C` must not become client-visible through fetch, long polling, or
cached tail responses. The client does not choose a filesystem segment or local
storage path.

#### Consumer operations

Consumer-group membership, assignment, polling, and commits enter through the
runtime but are handled by the consumer coordinator. Storage receives only the
resulting local read/commit operations and never manages client heartbeats or
group generations.

### Routing and metadata refresh

A client may connect to any broker that provides the client transport. The
runtime may:

- serve a request locally;
- return a leader/metadata hint for client refresh; or
- proxy the request to the current owner through peer transport.

The first design should prefer explicit client metadata refresh and bounded
redirection over invisible multi-hop proxying. Proxying can be added when its
backpressure, identity propagation, and timeout behavior are specified.

A routing response should include enough information to refresh safely:

```text
cluster identity
partition
leader node/address hint
assignment revision
leader epoch or opaque generation
retry-after or refresh guidance
```

The hint is not authority. The receiving broker rechecks its own committed
projection and rejects stale or unauthorized requests.

### Error taxonomy

The runtime maps internal results to stable domain errors. Transport errors and
domain errors remain distinct.

#### Routing and authority

```text
NotLeader
StaleMetadata
StaleLeaderEpoch
BrokerDraining
BrokerFenced
PartitionUnavailable
MetadataQuorumUnavailable
```

These errors may include a metadata refresh hint but must not expose internal
storage paths or consensus details.

#### Durability and offsets

```text
QuorumUnavailable
AppendOutcomeUnknown
OffsetOutOfRange
RetentionBoundary
OffsetConflict
RecordTooLarge
```

`AppendOutcomeUnknown` means the request may have become durable. It must never
be translated into a safe-to-retry rejection without an idempotency contract.

#### Request and policy limits

```text
RequestCanceled
RequestDeadline
Unauthorized
RateLimited
TooManyInflight
MessageTooLarge
ProtocolVersionUnsupported
```

The error taxonomy should be stable across transport implementations. A
transport timeout must not be confused with `QuorumUnavailable`, and a stale
leader response must not be reported as a socket failure.

### Retry and idempotency

The runtime must classify operations by retry safety:

| Operation | Automatic retry | Reason |
|---|---:|---|
| Metadata lookup | bounded | read-only and refreshable |
| Committed fetch | bounded | read-only, offset-based |
| Append without request identity | no | outcome may be ambiguous |
| Append with defined idempotency key | policy-controlled | requires deduplication semantics |
| Consumer heartbeat | bounded | session protocol defines identity |
| Offset commit | generation-controlled | stale commits must be rejected |

A client deadline does not cancel a remote durable mutation retroactively. The
runtime must return or preserve an unknown-outcome result when processing may
have continued after local cancellation.

### Authentication and authorization boundary

The client transport authenticates the application or client session. The
runtime maps that identity to authorization decisions such as:

- read or append permission for a topic/partition;
- consumer-group membership and commit permission;
- administrative metadata permission; and
- quotas and request-size limits.

Peer authentication and node authorization are separate. A client must not be
able to claim a `NodeID`, leader epoch, replica role, or internal message type
through a client envelope.

Authorization is checked before invoking storage or replication. The storage
engine need not understand client identities or ACLs.

### Deadlines, cancellation, and backpressure

The runtime propagates a request deadline into internal operations but does not
interpret cancellation as proof that a remote mutation stopped. It must bound:

- client request body size;
- per-client and per-partition in-flight work;
- proxy/peer queue depth;
- response buffering; and
- long-poll waiters.

Admission control should reject or delay work before unbounded memory or disk
pressure reaches storage. A slow client must not block the partition writer or
metadata control path indefinitely.

Control-plane requests, client reads, client writes, and replication traffic
should have independently observable budgets. Replication backpressure must not
starve fencing or metadata refresh, and an untrusted client must not consume
all control-plane capacity.

### Versioning and compatibility

The client protocol needs explicit version negotiation separate from the peer
replication protocol. A broker may support several client versions during a
rolling upgrade, but it must reject incompatible semantics rather than silently
reinterpret a request.

Version negotiation should cover:

- envelope and error encoding;
- append/fetch feature capabilities;
- maximum sizes and flow-control features; and
- authentication mechanisms.

Internal storage formats and client wire formats evolve independently. A
client protocol change must not require a storage package dependency on the
client codec.

### Observability

The runtime should expose operation-level signals without exposing payloads by
default:

- request counts, latency, cancellations, and deadline outcomes;
- routing redirects and stale metadata rates;
- append committed/rejected/unknown outcomes;
- fetch visibility boundary and retention errors;
- per-client and per-partition backpressure;
- authorization failures and protocol mismatches; and
- broker lifecycle and assignment state.

Correlation/request IDs may cross transport and runtime boundaries. They must
not be confused with producer idempotency keys or consensus command IDs.

### Safety invariants

The broker/runtime boundary must preserve:

1. Clients never access storage paths, files, or internal storage objects.
2. Storage receives only validated local domain operations.
3. A client request cannot assert cluster authority through its payload.
4. Routing uses committed metadata and rechecks current epoch/assignment.
5. Fetch never exposes local `H` beyond committed `C`.
6. Quorum success is not reported as local-only success.
7. Ambiguous mutations are not automatically retried without idempotency.
8. Stale client generations and consumer commits are rejected.
9. Transport failures remain distinguishable from domain failures.
10. Client backpressure cannot starve fencing or metadata control.

### Qualification scenarios

A future broker/runtime model should test:

- client connected to a non-leader and receiving refreshable routing data;
- metadata revision changing between routing and append;
- leader fencing during an in-flight append;
- cancellation after local or quorum durability;
- fetch while `H > C`;
- proxy timeout with an unknown remote outcome;
- stale consumer generation commit;
- client authorization attempting an internal message type;
- request-size and in-flight limits under pressure; and
- broker drain/restart while requests are pending.

These tests should use the deterministic client and peer loopback transports,
while asserting that storage sees only local domain operations.

### Deferred decisions

The following remain open:

- whether the first client protocol is request/response only or includes client
  streams;
- explicit proxying versus client-directed leader routing;
- producer idempotency-key format and retention;
- authentication and authorization mechanism;
- quota policy and fairness model; and
- public client API versus protocol-only support.

The default boundary is client transport -> broker/runtime -> internal
coordination/replication/storage adapters, with no client-facing concepts in
the storage package.

### Metadata coordination state machine

model; it does not implement consensus, transport, replication, or broker APIs.

### Scope

The metadata state machine is the authoritative control-plane model for a
cluster. It decides which nodes and replicas are eligible to serve partitions
and which leader epoch is current. It does not append user records and does not
sit in the normal record-ingress hot path.

The state machine is intended to run on a consensus-backed metadata quorum.
The consensus implementation supplies ordered, committed commands; this model
supplies deterministic validation and state transitions.

Local storage remains authoritative for local filesystem bytes. Each broker
projects committed metadata into its local runtime and continues to own its own
`Store` directory and `LOCK`.

### State layers

Metadata has three distinct kinds of state:

1. **Committed durable state** — replicated through the metadata quorum and
   replayable after controller restart.
2. **Session/liveness state** — derived from broker registration and lease
   activity; it may expire without being a user-visible topic mutation.
3. **Runtime projection state** — a broker's local view of assignments and
   epochs; it is rebuildable from committed metadata and must not become the
   authority by itself.

A metadata revision identifies the committed order of durable state changes.
A node session identifies one incarnation of a broker. A partition leader epoch
fences stale owners and is independent of the metadata revision.

### Core identifiers and versions

Every cluster object should use explicit identifiers:

```text
ClusterID             stable cluster identity
NodeID                stable node identity
NodeSessionID         one process/incarnation registration
TopicID               immutable topic identity
PartitionKey          TopicID + partition number
AssignmentRevision    metadata revision that committed an assignment
LeaderEpoch           monotonically increasing partition fencing token
```

Rules:

- `NodeID` survives a process restart; `NodeSessionID` does not.
- A new session for the same node cannot inherit authority from the old
  session without a fresh committed transition.
- `AssignmentRevision` is monotonic for the cluster metadata log.
- `LeaderEpoch` is monotonic for each partition and never repeats.
- A lower revision or epoch is stale and cannot overwrite newer state.

### Cluster and node state

The committed cluster state contains the cluster identity, supported protocol
versions, node records, topic specifications, partition assignments, and
administrative operations.

A node progresses through:

```text
unknown -> joining -> active -> draining -> removed
                    |          |
                    v          v
                  fenced     fenced
```

#### Joining

A node proves its identity and capabilities to the metadata quorum. It has no
partition authority until an assignment transition is committed for its active
session.

#### Active

An active node may host assignments committed for its current session and
leader epoch. Liveness alone does not make a node a leader.

#### Draining

A draining node receives no new leadership assignments. Existing partitions
are reassigned through the normal add/catch-up/promote/remove sequence.

#### Fenced or removed

A fenced session may not serve cluster data-path operations. A removed node
must register a new session and receive fresh assignments before rejoining.

### Partition assignment state

For each partition, committed metadata records:

```text
replica set             ordered or explicitly set NodeIDs
leader                  one NodeID or none during transition
leader epoch            current fencing token
assignment revision     metadata revision of this assignment
replica eligibility     catching_up, eligible, draining, removed
minimum quorum          required durable replica count
```

The desired assignment and runtime replica progress are separate. A replica can
be assigned but not yet eligible for quorum acknowledgement.

The logical assignment lifecycle is:

```text
unassigned
    -> preparing
    -> recovering
    -> eligible
    -> serving
    -> reassigning
    -> draining
    -> removed
```

Only a committed assignment can create authority. A local broker may prepare
files or recover a replica before it is eligible, but it must not expose those
bytes as cluster-committed data.

### Commands and transitions

Commands are proposed to the metadata quorum. The state machine validates them
against the committed state and either emits a deterministic event or rejects
them. Commands must carry an operation identity so a retried proposal is
idempotent.

#### RegisterNode

Inputs:

```text
NodeID, NodeSessionID, capabilities, protocol versions, placement labels
```

Validation:

- cluster identity matches;
- node identity is authorized;
- session is newer than the currently active session; and
- capabilities satisfy the cluster's minimum protocol requirements.

Effect: create or replace the node's joining session. Registration alone grants
no partition ownership.

#### ActivateNode

Effect: commit the node as active for the registered session. Assignments may
then target it.

#### DrainNode

Effect: mark the node draining and schedule removal from assignments. Existing
leaders remain valid until a separate leader transition commits a new epoch.

#### CreateTopic / UpdateTopic

Effect: commit immutable topic identity and validated partition configuration.
Configuration changes must have explicit compatibility rules; a runtime view
must never infer a new assignment from an uncommitted local file.

#### PrepareAssignment

Effect: add target replicas in `recovering` state at a new assignment revision.
No target becomes quorum-eligible until it proves it has caught up to the
required committed prefix.

#### MarkReplicaEligible

Effect: record that a target replica has reconciled to the required prefix and
may participate in quorum decisions. The data-plane report must be checked
against the current assignment revision and leader epoch.

#### ElectLeader

Effect:

1. choose an eligible replica containing the required committed prefix;
2. increment the partition's leader epoch;
3. commit the new leader and assignment revision; and
4. fence the previous leader/session.

The new leader cannot serve until its local runtime has installed the committed
epoch and recovered the allowed prefix.

#### RemoveReplica

Effect: remove a replica only after the replacement policy and minimum quorum
constraints remain satisfied. Removal of a current leader requires a separate
leader transition first.

#### FenceSession

Effect: mark a node session unable to serve assignments. Fencing is monotonic;
a later session or epoch cannot be unfenced by replaying an older command.

### Safety invariants

The state machine and its projections must preserve these invariants:

1. **Single committed leader:** a partition has at most one committed leader at
   one leader epoch.
2. **Monotonic epochs:** leader epochs never decrease or repeat.
3. **Assignment authority:** only nodes in the committed assignment may serve
   the partition.
4. **Session fencing:** an old node session cannot regain authority after a
   newer session is committed.
5. **Leader eligibility:** a leader is an eligible replica of the current
   assignment and contains the required committed prefix.
6. **Revision monotonicity:** brokers never apply a lower assignment revision
   over a higher one.
7. **Quorum floor:** an assignment transition cannot leave a partition without
   its configured minimum eligible quorum unless it explicitly enters an
   unavailable state.
8. **No false commitment:** metadata state alone never claims that a user
   record is durable on a quorum; the replication layer supplies durable
   positions and computes the high watermark.
9. **No storage reset:** metadata replay or projection failure never deletes,
   truncates, or resets authoritative local history implicitly.
10. **Idempotent commands:** replaying a committed command or retrying its
    proposal produces the same state and event identity.

### Leader transition protocol

A safe leader transition is a multi-stage operation:

```text
metadata quorum commits new epoch
        |
        v
new leader installs assignment and epoch locally
        |
        v
new leader recovers committed prefix
        |
        v
replica quorum becomes eligible
        |
        v
broker begins serving committed reads/writes
```

The old leader may still be alive or paused. It must be rejected at the local
serialized append boundary when its epoch is stale. A transport disconnect is
not itself proof that the old process has stopped.

A leader without the required quorum may retain local bytes for recovery, but
it must not acknowledge a quorum-durable append or expose bytes beyond the
committed high watermark.

### Reassignment protocol

Planned reassignment is intentionally ordered:

1. commit the target replica in `recovering` state;
2. stream and reconcile the target to the committed boundary;
3. validate target identity, assignment revision, epoch, and durable position;
4. mark the target eligible;
5. commit a new assignment or leader epoch as required;
6. drain the old replica; and
7. remove the old replica after quorum constraints remain satisfied.

A failed or canceled reassignment is resumable by operation identity. It must
not remove the old replica merely because the target directory exists.

### Failure model

#### Broker crash

The node session expires or is fenced through the metadata quorum. A new
leader is elected only from eligible replicas with the required committed
prefix. The new leader starts with a new epoch.

#### Network partition

A minority-side leader cannot acquire quorum-durable acknowledgements. A
majority-side election advances the epoch and fences the old session. Delayed
messages from the old epoch are rejected by both metadata and data-path checks.

#### Controller/quorum loss

Existing broker projections may continue serving only according to the last
committed assignment and lease policy. New ownership changes, elections, and
quorum claims must stop without metadata quorum authority. The system should
prefer a safe unavailable state over speculative leadership.

#### Corrupt or incomplete projection

The broker must stop the affected assignment, preserve authoritative local
files, and rebuild its projection from committed metadata. Projection repair
must not silently invent a leader epoch or truncate data.

### Local broker projection

A broker applies committed metadata in revision order. Projection application
should be atomic per assignment:

```text
read committed assignment
  -> validate revision/session/epoch
  -> install local role and fencing state
  -> start or stop replication/runtime workers
  -> expose the new serving state
```

The local `Store` remains the source of local bytes and local recovery state.
The projection is not a second catalog authority. If local state conflicts with
committed metadata, the broker enters a fenced/recovery state and reports the
conflict to the control plane.

### Consensus integration boundary

The metadata state machine should be independent of the eventual consensus
library. Consensus supplies:

- ordered command delivery;
- committed versus uncommitted entries;
- quorum availability;
- durable metadata-log recovery; and
- leadership for the metadata quorum.

The state machine supplies deterministic command validation and resulting
metadata events. It must be testable with a single-threaded in-memory log
before a consensus implementation is selected.

### Deterministic design tests

Before networking or consensus implementation, model tests should cover:

- duplicate registration and newer node sessions;
- stale assignment revisions;
- stale leader epochs;
- delayed `ElectLeader` and `FenceSession` commands;
- failed target catch-up and resumable reassignment;
- removal that would violate the quorum floor;
- crash during each reassignment stage;
- replay of every committed command;
- partition election with missing or divergent prefixes; and
- metadata quorum loss during an ownership transition.

Each model test should assert the safety invariants rather than only the final
field values. The same command/event vectors can later qualify a consensus
adapter and broker projection.

### Deferred decisions

This specification intentionally leaves these decisions open:

- consensus implementation and metadata quorum membership policy;
- whether node liveness uses leases, session heartbeats, or both;
- exact replica-progress representation in metadata versus the data plane;
- placement constraints and rack/zone awareness;
- uncommitted-tail retention and truncation mechanics; and
- administrative API and authorization policy.

The default safety posture is clean quorum-only election, monotonic epochs,
explicit fencing, no uncommitted visibility, and unavailable rather than
speculatively writable partitions when authority is ambiguous.

### Metadata quorum lifecycle

lifecycle; it does not select or implement a consensus library.

### Purpose and boundary

The metadata quorum is the authoritative control plane for cluster ownership.
It commits node membership, partition assignments, leader epochs, and
administrative transitions. It does not replicate user records and does not
participate in every record append.

```text
metadata quorum
    | commits assignments, epochs, membership
    v
broker metadata projection
    | authorizes
    v
partition replication and local storage
```

The quorum must provide a single committed order for metadata commands. The
metadata state machine described in the metadata coordination section validates those
commands and applies them deterministically.

The quorum lifecycle must preserve the distinction between:

- **consensus term/leadership:** authority within the metadata quorum;
- **metadata revision:** committed order of metadata state changes; and
- **partition leader epoch:** fencing token for one partition's data path.

These values must not be reused interchangeably.

### Quorum composition

The default design uses a majority quorum of dedicated metadata voters:

```text
metadata voters: M
required quorum: floor(M / 2) + 1
```

An odd number of voters avoids adding a node without increasing fault
 tolerance. Three voters tolerate one voter failure; five tolerate two, subject
to storage and placement constraints.

Metadata voters should normally be separate logical roles from partition
replicas, even when one broker process hosts both. A data-heavy replication
stream must not starve metadata elections, fencing, or membership commits.

A metadata quorum must not claim a committed transition without a durable
majority. A minority may serve a stale read-only projection only if the
broker's API clearly marks the view as non-authoritative; it must not elect,
assign, fence, or claim new ownership.

### Cluster identity and genesis

A cluster has a stable `ClusterID` and an initial voter configuration. Bootstrap
must be explicit and one-time:

1. generate or receive the cluster identity;
2. validate the initial voter set and placement;
3. initialize the metadata log/snapshot format and protocol version;
4. persist the genesis configuration before serving control requests; and
5. require quorum agreement before committing the first mutable metadata state.

A node must reject metadata from a different `ClusterID`. A data directory
containing an existing cluster identity must not be silently reinitialized with
a new genesis configuration.

Automatic discovery must not create competing clusters. Discovery may locate
candidate nodes, but an authenticated bootstrap configuration or an already
committed quorum must authorize membership.

### Quorum member states

A metadata voter progresses through these logical states:

```text
uninitialized
    -> joining
    -> learner
    -> voter
    -> draining
    -> removed

voter runtime states:

follower <-> candidate <-> leader
       \-> recovering
       \-> unavailable
```

#### Uninitialized and joining

The node has no authority until it validates cluster identity and obtains a
committed membership transition. It may fetch a snapshot or log prefix as a
learner but must not vote or serve authoritative metadata.

#### Learner

A learner receives metadata log entries and snapshots but does not count toward
quorum and cannot become metadata leader. It must catch up before promotion.

#### Voter

A voter participates in elections and commits. Its durable log and snapshot
must obey the selected consensus implementation's rules.

#### Draining and removed

A draining voter is intentionally removed through a committed membership
change. A removed node must discard no authoritative data automatically, but it
must stop voting and serving quorum authority. Rejoining requires an explicit
new session and membership transition.

#### Follower, candidate, leader

These are consensus runtime states. Only the current metadata leader may
propose ordinary commands, and only committed entries may affect broker
projections.

#### Recovering and unavailable

A node recovering after restart must rebuild consensus state before serving
metadata. A quorum without a current leader may be unavailable for writes even
if individual nodes have readable old projections.

### Bootstrap lifecycle

#### Initial bootstrap

Initial configuration should contain:

```text
ClusterID
metadata voters and stable NodeIDs
metadata protocol version
initial placement/failure-domain constraints
```

Bootstrap should fail closed on conflicting configurations. Reusing an old
metadata directory with a new cluster ID, or using the same cluster ID with an
incompatible genesis voter set, requires an explicit administrative recovery
procedure rather than an automatic repair.

#### Adding a new voter

A new node first joins as a learner:

```text
candidate identity authenticated
        |
        v
learner added to current configuration
        |
        v
snapshot/log catch-up
        |
        v
learner is promotion-eligible
        |
        v
membership configuration commits voter promotion
```

The learner must not vote before its promotion is committed. Catch-up should
be measured against the metadata commit index, not merely the latest message
received.

### Metadata leader lifecycle

A metadata leader must have a current consensus term and a committed cluster
membership view. Its lifecycle is:

```text
follower -> candidate -> leader
                         |
                         v
                      follower
```

A candidate may become leader only through the consensus protocol's quorum
rules. A node must not self-promote because it has the newest local log or the
most recent broker heartbeat.

The leader should establish authority for its term before accepting metadata
commands. It must reject or reconcile proposals from stale terms and return a
classified not-authoritative result to callers.

Metadata leadership changes do not themselves change any partition leader
epoch. Partition epochs advance only when the metadata state machine commits a
partition ownership transition.

### Metadata command lifecycle

A control operation follows:

```text
client/admin request
        |
        v
metadata leader validates request and identity
        |
        v
consensus appends proposed command
        |
        v
majority durably commits command
        |
        v
metadata state machine applies command
        |
        v
broker projections observe revision in order
```

The command identity and result must be recoverable. A lost response after
commit is an unknown client outcome; querying the committed metadata revision
or operation identity must reveal whether the command took effect.

The leader must not report success merely because an entry is present in its
local log. A broker must not apply an uncommitted entry to partition ownership,
epoch, or fencing state.

### Membership changes

Membership changes alter the quorum itself and require stronger rules than
ordinary metadata commands. The default design should use a consensus-defined
joint configuration or equivalent two-phase transition:

```text
old configuration
        |
        v
joint configuration (old + new)
        |
        v
new configuration
```

During the joint phase, a membership decision must satisfy both configurations
as required by the consensus protocol. This prevents a single transition from
creating two independent majorities.

Recommended procedure for replacing a voter:

1. add the replacement as a learner;
2. catch it up to the metadata commit index;
3. enter the joint configuration;
4. commit the new voter set;
5. remove the old voter after the transition is durable; and
6. mark the old node removed and fence its session.

Never remove enough voters in one step to lose the ability to form the required
quorum. Placement constraints must be checked before committing a change.

A membership change should be serialized with cluster bootstrap, another
membership change, and any emergency recovery operation. Partition assignments
may continue independently unless they depend on the changing node.

### Restart and durable recovery

A metadata node must persist the consensus state needed to prevent stale
leadership after restart, including at least:

```text
ClusterID
current consensus term/epoch
voted-for identity, if required
metadata log or snapshot
commit index
last applied revision
membership configuration
```

Recovery sequence:

1. acquire the node's stable local lock and validate cluster identity;
2. verify snapshot and log checksums;
3. restore the latest valid snapshot;
4. replay committed log entries after the snapshot;
5. reconcile consensus term and membership state;
6. catch up any remaining committed metadata entries; and
7. expose the broker projection only after revision order is valid.

A node may retain uncommitted log entries for consensus reconciliation, but it
must not apply them to broker authority. Corrupt or incomplete metadata state
must cause a fenced/recovery state, not an automatic rewrite of the log.

### Snapshots and log compaction

The metadata log is authoritative for committed control transitions, but it
cannot grow without bound. Snapshots should contain:

- cluster identity and protocol version;
- complete committed metadata state;
- membership configuration;
- metadata commit index and revision;
- consensus term/epoch required by the implementation; and
- integrity metadata and format version.

Snapshot creation must occur at a committed boundary. Installation must be
staged, checksummed, and atomically promoted so a crash cannot leave an
apparently valid partial snapshot.

Log compaction may discard entries only when every required recovery path has
an equivalent snapshot or retained prefix. A follower or learner behind the
compaction point must receive a snapshot/bootstrap transfer rather than asking
for deleted entries.

Snapshots are control-plane state, not replacements for authoritative user-log
segments. They must never cause user data truncation or offset reuse.

### Quorum loss and degraded operation

When the metadata quorum cannot form a majority:

- no new ownership, election, fencing, or membership transition commits;
- no broker may claim a new partition leader epoch;
- existing projections may serve only operations allowed by the last committed
  assignment and explicit lease/fencing policy;
- quorum-durable data acknowledgements must not rely on a stale control view;
- administrative writes return a classified quorum-unavailable error; and
- recovery waits for quorum or an explicit, separately authorized disaster
  recovery procedure.

Read-only committed data may remain available on brokers with a valid recent
projection, but the API must distinguish it from authoritative control-plane
operations. The system must not silently convert stale metadata into a new
source of truth.

### Broker projection and fencing

A broker applies metadata revisions in order. For each partition assignment,
it validates:

```text
ClusterID
AssignmentRevision
NodeSessionID
LeaderEpoch
replica role and eligibility
```

The projection installs a local fencing guard before enabling data-path work.
If a newer assignment or epoch arrives, the broker first stops accepting stale
operations, then transitions replication/runtime workers, then exposes the new
state.

A projection that misses revisions must catch up from the metadata quorum or a
verified snapshot. It must not apply revision `R+1` while assuming revision `R`
was applied if the transition semantics depend on it.

### Controller failover and partition interaction

Metadata leader failover and partition leader failover are related but distinct:

- metadata leader failover changes who can commit control state;
- partition leader failover changes a partition's data-path epoch.

A new metadata leader may discover that a partition needs recovery, but it must
commit the assignment/epoch transition before the broker claims ownership. A
partition leader must not use an uncommitted controller observation as proof of
authority.

If a metadata leader fails during a partition transition, replaying committed
metadata produces the last valid state. Uncommitted transition proposals are
reconciled by the consensus protocol and do not become broker authority.

### Administrative recovery

Normal operation must not contain an unsafe force-start or force-majority
shortcut. A separate disaster-recovery procedure may be designed later, but it
must require explicit operator authorization and clearly state possible data or
availability loss.

Any recovery procedure must record:

```text
old ClusterID and configuration
recovery operator/request identity
chosen surviving metadata state
new membership and term/epoch
affected partition assignments
```

It must fence nodes that could still believe the old quorum is authoritative.
A node cannot be safely reintroduced until its metadata state and session are
reconciled with the recovered cluster.

### Security and placement requirements

Metadata voters need stronger guarantees than ordinary data connections:

- stable authenticated node identity;
- authorization to propose or vote;
- encrypted transport where nodes are not mutually trusted;
- auditability of membership and fencing operations; and
- placement across independent failure domains where possible.

Authentication does not replace consensus. A correctly authenticated node with
stale metadata remains stale and must be fenced by term, revision, and session
rules.

### Safety invariants

The metadata-quorum lifecycle must preserve:

1. Exactly one committed metadata history for a cluster identity.
2. No node votes or leads outside the committed voter configuration.
3. No membership change creates two independently valid majorities.
4. Only committed metadata affects broker authority.
5. Consensus terms/epochs never move backward after restart.
6. Metadata revisions are applied in order and never regress.
7. A stale metadata leader cannot commit after losing authority.
8. A learner cannot count toward quorum before promotion commits.
9. Snapshot installation cannot expose partial or unverified state.
10. Quorum loss cannot create speculative ownership or fencing decisions.
11. Metadata recovery cannot truncate or reset authoritative user-log history.
12. Every committed administrative operation can be identified and replayed.

### Qualification scenarios

A future model and consensus adapter should test:

- first-cluster bootstrap and conflicting genesis configuration;
- leader election with delayed, duplicated, or reordered metadata messages;
- metadata leader crash before and after commit;
- voter restart with stale term or stale snapshot;
- learner catch-up and promotion;
- joint membership changes and failed replacement nodes;
- quorum loss during membership transition;
- snapshot creation, transfer, corruption, and interrupted installation;
- compaction while a learner is behind;
- partition assignment transition during metadata leader failover;
- stale broker projections receiving newer epochs; and
- explicit disaster-recovery fencing of old cluster members.

### Deferred decisions

The following require later selection and qualification:

- consensus algorithm/library and storage format;
- metadata voter placement and automated membership policy;
- session heartbeat and lease timing;
- snapshot cadence and transfer protocol;
- read consistency for stale projections;
- administrative recovery tooling; and
- metadata authorization and audit schema.

The default posture is an explicitly bootstrapped majority quorum, learner-first
membership changes, joint configuration transitions, committed-only authority,
checksummed snapshots, clean recovery, and unavailable rather than speculative
control-plane behavior.

### Partition replication protocol

it does not add replica networking or change the current local storage engine.

### Purpose and boundary

The replication layer connects a partition's local durable log to the cluster
assignment and transport layers:

```text
metadata assignment + leader epoch
              |
              v
      replication state machine
          |             |
          v             v
   local storage     transport stream
```

Metadata coordination decides which nodes are assigned and which leader epoch
is current. Replication decides how an assigned leader transfers ordered
batches, how followers report durable positions, and when the committed
high-watermark can advance.

The replication layer must preserve the local storage guarantees:

- the filesystem remains authoritative for local bytes;
- an acknowledged local write is synchronized before local success;
- unknown outcomes are not rolled back or assigned new offsets; and
- indexes, snapshots, and tail caches remain rebuildable.

### Terminology

Use end-exclusive positions in the protocol:

```text
L  retained log start
H  local durable end (next offset after the local durable prefix)
C  committed/high-watermark end
```

For a serving replica:

```text
L <= C <= H
```

A batch with `[base, end)` contains records starting at `base` and ending
before `end`. A follower's durable position describes a contiguous prefix, not
an arbitrary collection of individual records.

`LeaderEpoch` identifies the ownership generation under which a batch was
produced. `AssignmentRevision` identifies the metadata assignment against
which the replica is operating.

### Replica roles and states

A replica runtime has one committed assignment role and one operational state:

```text
unassigned
    -> recovering
    -> follower
    -> eligible
    -> draining
    -> removed

leader election promotes an eligible replica:

eligible follower -> leader recovering -> leader serving
```

#### Recovering follower

A recovering follower may receive bootstrap data and reconciliation commands,
but it is not eligible for quorum acknowledgement or committed reads.

#### Follower

A follower accepts replication only from the current leader epoch and checks
every batch against its local prefix. It reports durable progress after local
persistence, not merely after buffering in memory.

#### Eligible follower

An eligible follower has caught up to the required committed boundary and may
count toward the configured quorum. Eligibility is a metadata/control-plane
fact; a temporarily failed stream must not silently create quorum authority.

#### Leader recovering

A newly elected leader validates its local prefix, installs the new epoch,
and reconstructs safe committed progress before accepting cluster writes.

#### Leader serving

A serving leader may accept cluster writes only while its assignment, session,
and leader epoch remain valid. It may acknowledge a batch only according to the
configured quorum policy.

#### Draining or fenced

A draining or fenced replica cannot accept new client writes and cannot count
as an eligible quorum member. A stale leader must reject data-path operations
at the serialized local append boundary.

### Protocol messages

The exact wire encoding is deferred, but messages need explicit fields and
versioning. Illustrative message families are:

#### Replication handshake

```text
partition
assignment revision
leader epoch
leader node/session
follower durable end
follower retained start
protocol capabilities
```

The handshake establishes the current assignment and lets the leader choose
between live catch-up and bootstrap. It does not itself make a follower
eligible.

#### Batch append

```text
partition
assignment revision
leader epoch
base offset
end offset
batch sequence or request identity
encoded batch
checksum
```

The sequence/request identity supports duplicate detection and reconciliation;
it does not make an ambiguous client append automatically safe to retry.

#### Durable acknowledgement

```text
partition
assignment revision
leader epoch
matched prefix end
durable end
status
```

The follower reports a contiguous locally synchronized prefix. It must not
report a position that is only queued in memory.

#### Catch-up request/response

A follower can request batches from a position with bounded byte/record limits.
The response includes the same assignment, epoch, offsets, and checksums as a
live append. The leader may reject the request if the requested position is no
longer retained or if the follower needs bootstrap.

#### Reconcile/truncate

A leader or recovery controller may request that a follower discard a divergent
uncommitted suffix. The request must include:

```text
partition
assignment revision
leader epoch
required common-prefix end
operation identity
```

Truncation is permitted only within an explicitly authorized uncommitted
suffix. It must never remove committed data or bypass the storage recovery and
filesystem durability rules.

### Leader-to-follower flow

A normal replication cycle is:

```text
leader validates assignment and epoch
        |
        v
leader appends batch locally and synchronizes
        |
        v
leader sends batch to eligible followers
        |
        v
followers validate prefix and epoch
        |
        v
followers append and synchronize locally
        |
        v
followers report durable positions
        |
        v
leader advances C when quorum is satisfied
        |
        v
leader releases the client quorum acknowledgement
```

The leader may pipeline batches, but the protocol must preserve per-partition
ordering and make the durability point observable. A follower acknowledgement
for batch `n` must not imply that a later batch is durable unless its reported
end explicitly includes that later prefix.

### Prefix and offset validation

A follower handles an incoming batch according to its local durable end `Hf`:

```text
base == Hf       append the batch
base <  Hf       verify overlap; treat identical bytes as duplicate
base >  Hf       reject as a gap and request catch-up
```

If an overlapping range differs, the follower reports a divergence instead of
overwriting blindly. The higher-level recovery protocol finds a common prefix,
truncates only the uncommitted divergent suffix, and resumes from that point.

A batch from a lower leader epoch or stale assignment revision is rejected. A
batch from a higher epoch is not accepted merely because the number is higher;
the local metadata projection must first authorize the new assignment.

Checksums protect framing and corruption detection. They do not replace offset,
epoch, assignment, or quorum validation.

### Quorum and high watermark

Let `Q` be the configured minimum durable replica count. For a majority policy,
`Q` is a majority of the assigned replicas, subject to the assignment's
eligibility rules.

The committed end is the greatest prefix `x` for which at least `Q` eligible
replicas have durably stored the same partition prefix through `x`:

```text
C = greatest x such that Q eligible replicas have durable prefix >= x
```

The leader must also confirm that those positions belong to the current
assignment and leader epoch. A numeric offset reported by a stale or divergent
replica does not advance `C`.

Properties:

- `C` never advances past the leader's local durable end;
- `C` is monotonic within a leader epoch;
- a published cluster high watermark never regresses across leader changes;
- a new leader starts from the last committed boundary it can safely prove; and
- if the prior committed boundary cannot be proven, the partition remains
  unavailable rather than exposing a speculative prefix.

The leader should publish high-watermark changes to committed readers without
requiring readers to inspect follower state. The broker read path must cap
visible records at `C`.

### Acknowledgement contract

The first cluster API should expose quorum-durable acknowledgement only:

1. validate the current assignment, node session, and leader epoch;
2. allocate the next local offset range under the partition writer;
3. append and synchronize locally;
4. replicate to eligible followers;
5. advance `C` after the quorum condition is satisfied; and
6. acknowledge the client.

A lost response after step 3, 4, or 5 is an unknown outcome. The client may
reconcile using an idempotency mechanism defined by a later producer protocol,
but the broker must not blindly append the same logical request or reuse its
offsets.

Leader-only acknowledgement is intentionally deferred. Adding it later would
require explicit visibility, failover, and durability semantics rather than a
boolean shortcut.

### Loss of quorum and lagging followers

A follower that stops making progress may be removed from the eligible set by
a committed control-plane transition. The leader can continue only if the
remaining eligible replicas satisfy `Q`.

When fewer than `Q` eligible replicas are available:

- no new quorum-durable append is acknowledged;
- the leader must not advance `C` using a minority;
- existing committed reads may continue through the last known `C`, subject to
  lease/fencing policy; and
- the partition enters a visibly unavailable or degraded state.

The replication stream may continue catching up a lagging follower, but stream
activity does not make it eligible until the control plane records successful
reconciliation.

### Catch-up and bootstrap

#### Incremental catch-up

If the follower's retained prefix overlaps the leader's committed prefix, the
leader sends bounded batches from the common position. Each batch is validated
with assignment, epoch, offsets, and checksum.

#### Divergent suffix

If the follower has bytes after the common prefix that do not match the leader,
the runtime marks the suffix uncommitted and requests controlled truncation.
The follower must not expose that suffix to committed readers.

#### Bootstrap

If the follower's requested position precedes the leader's retained start, or
if incremental reconciliation is too expensive, the leader uses a snapshot or
segment bootstrap protocol. Bootstrap must:

1. identify a committed source boundary;
2. transfer data in bounded, checksummed chunks;
3. install it atomically or through a recoverable staging process;
4. verify the resulting prefix and offsets; and
5. resume live replication from the verified boundary.

A bootstrapping follower is not quorum-eligible until all steps complete.

### Leader recovery

After election, the new leader must:

1. validate its committed assignment and new leader epoch;
2. recover local filesystem state and determine `H`;
3. identify the safe committed prefix at or below `H`;
4. retain or quarantine any uncommitted suffix;
5. establish follower progress for the new epoch; and
6. serve committed reads and quorum writes only after quorum is available.

The recovery policy is clean election. A replica missing the committed prefix
cannot become leader merely because it is alive or has a larger uncommitted
suffix.

### Backpressure and batching

Replication streams must be bounded independently from client ingress. The
leader must not accumulate unlimited unsent batches for a slow follower.

Recommended controls:

- maximum in-flight batches per follower;
- maximum in-flight bytes per follower;
- separate control and replication stream budgets;
- cancellation when a follower is fenced or removed; and
- explicit catch-up rate limits so recovery cannot starve ordinary traffic.

Batching may combine contiguous records for throughput, but it must not delay
fencing or metadata/control messages behind an unbounded data queue.

### Retention and committed visibility

Retention operates on the local retained start `L`, but cluster mode must
preserve the committed visibility invariant:

```text
L <= C <= H
```

A follower that falls behind the retained start must bootstrap rather than
expect unavailable historical bytes from the leader. Retention must not delete
bytes that are still required to establish the committed prefix for an eligible
replica without a replacement/bootstrap plan.

Committed readers see only `[L, C)`. Local diagnostic or recovery tooling may
inspect additional local bytes under explicit privileged semantics, but those
bytes must not enter ordinary cluster APIs.

### Failure and ambiguity rules

#### Stale messages

Messages with stale assignment revisions, sessions, or leader epochs are
rejected without changing durable state. Rejection must identify the current
expected generation so the sender can reconcile.

#### Duplicate messages

A duplicate batch whose bytes and offsets match an already durable prefix is
idempotently acknowledged at the follower protocol level. A duplicate client
request is not automatically safe unless it carries a producer/request identity
with defined deduplication semantics.

#### Reordered messages

A batch that creates a gap is rejected or deferred. The follower requests the
missing prefix; it must not fill the gap by writing later offsets.

#### Lost acknowledgements

The sender treats a lost acknowledgement as unknown. It queries durable
positions or reconciles through the protocol rather than assuming failure and
reusing offsets.

#### Follower crash

The follower restarts from filesystem recovery, reports its durable prefix, and
re-enters recovery. It is not eligible until it catches up and metadata marks it
eligible again.

#### Leader crash

The metadata quorum advances the leader epoch and elects only a clean eligible
replica. Old replication messages are rejected by epoch checks.

### Storage boundary

The replication layer needs storage capabilities, but storage must remain
cluster-agnostic. The future capability boundary should support:

- explicit-offset append with contiguous-prefix validation;
- durable-end inspection;
- bounded reads from an offset;
- a supplied visibility ceiling for committed reads;
- safe fencing at the serialized writer boundary; and
- controlled truncation of an authorized uncommitted suffix.

These operations must preserve `LOCK` ownership, disk admission, fsync,
recovery, offset continuity, and unknown-outcome semantics. Storage must not
call transport or consensus and must not decide whether a replica is eligible.

### Safety invariants

The protocol is correct only if it preserves:

1. A follower never acknowledges a prefix it has not synchronized locally.
2. A stale epoch cannot append, acknowledge, or advance `C`.
3. A gap cannot become a durable contiguous prefix.
4. A divergent suffix is never exposed as committed data.
5. `C` advances only from the configured quorum of matching eligible prefixes.
6. Published `C` never regresses.
7. A quorum acknowledgement implies the batch is within the committed prefix.
8. An unknown response never causes offset reuse or implicit rollback.
9. A follower rejoining after restart cannot count toward quorum prematurely.
10. Retention and bootstrap preserve the ability to recover the committed prefix.

### Qualification scenarios

A deterministic implementation model should test:

- one leader and one follower with successful quorum advancement;
- follower delay, drop, duplicate, and reorder faults;
- follower restart after local append before acknowledgement;
- leader response loss after local and quorum durability;
- stale epoch messages after a clean leader transition;
- divergent follower suffix and controlled truncation;
- follower lag beyond the retained start and bootstrap;
- loss and restoration of quorum;
- monotonic high-watermark publication across failover; and
- retention while a follower is catching up.

Every scenario should assert both externally visible behavior and the safety
invariants above. Throughput benchmarking belongs later, after the protocol's
failure semantics are fixed.

### Deferred decisions

The following remain intentionally open:

- exact batch encoding and checksum algorithm;
- snapshot/segment bootstrap format;
- replica-progress persistence and reporting cadence;
- producer idempotency and retry identity;
- whether follower acknowledgements carry a high watermark or only a durable
  prefix; and
- dynamic quorum configuration during reassignment.

The default design is a majority quorum, clean election, explicit epochs,
end-exclusive offsets, bounded replication streams, and committed reads capped
by the high watermark.
