# Avenue Clustering

Avenue's cluster mode turns a set of independent pub/sub nodes into a single logical broker. A
message published to any node is delivered to every interested subscriber on every node. The
cluster is **peer-to-peer with no coordinator** — every node runs the same code, discovers peers via
SWIM gossip, and forwards publishes directly over authenticated TCP links.

This document describes the design as it stands after Phase F. It covers SWIM membership,
at-least-once delivery, interest-based routing, the security handshake, the consistency / ordering
guarantees, all configuration keys, the admin introspection endpoints, and the known limits.

> **Wire format.** All node-to-node cluster packets (SWIM, publishes, ACK/resume/gap, interest,
> handshake) are framed as a length-prefixed **Protobuf `ClusterEnvelope`** — see
> [protocol.md](protocol.md). Field names in this document refer to the corresponding `cluster.proto`
> messages.

> Clustering is **off by default** (`cluster.enabled=false`). A single node runs exactly as before
> when clustering is disabled.

---

## Topology & lifecycle

- Every node binds a **client port** (`server.port`) for application clients and a **cluster port**
  (`cluster.port`) for peer links.
- A node bootstraps by connecting to one or more **static seeds** (`cluster.peers`,
  `host:port,...`). After the SWIM join it learns the full membership and the
  **LinkSupervisor** forms a full mesh of peer links — no further static configuration is needed for
  nodes discovered at runtime.
- Each peer link is a single authenticated TCP connection owning a handful of virtual threads
  (reader, writer, heartbeat sender, ACK sender). All cluster I/O uses Java 21 virtual threads.
- Tie-break: for any node pair, the lexicographically **lower** node id initiates the outbound link;
  the higher one accepts. Duplicate links are dropped, so exactly one link exists per pair.

```
            client port                         client port
   clients ───►  node-1  ◄───── cluster link ─────►  node-2  ◄─── clients
                   ▲                                    ▲
                   └──────────── cluster link ──────────┘
                                  node-3
```

---

## SWIM Gossip Membership (Phase E)

Membership and failure detection use a **SWIM**-style protocol (Scalable Weakly-consistent
Infection-style Process Group Membership) running as application-level packets over the existing
authenticated TCP links — **no UDP**, so failure detection is independent of TCP keepalive.

### Member states

| State     | Meaning                                                                       |
|-----------|-------------------------------------------------------------------------------|
| `ALIVE`   | The member answers direct or indirect probes.                                 |
| `SUSPECT` | A probe failed; the member is suspected but may still refute.                 |
| `DEAD`    | Suspicion timed out without a refute. **Sticky** — only a strictly higher incarnation ALIVE revives it. |
| `LEFT`    | The member announced a graceful leave. Authoritative and final.               |

The local node is never stored in the registry; the registry tracks only *remote* members.

### Probe / indirect probe / suspicion

1. **Direct probe.** Every `cluster.swim.probe-interval-ms` a node pings one random ALIVE member it
   holds a live link to and waits up to `cluster.swim.probe-timeout-ms` for an ack.
2. **Indirect probe.** On a direct timeout it asks `cluster.swim.indirect-probe-count` random
   helpers to ping the target on its behalf (ping-req). Any helper success rescues the target. This
   tolerates a single broken link without false-positives.
3. **Suspect.** If both fail, the target is marked `SUSPECT` and the fact is gossiped.
4. **Dead.** A reaper promotes `SUSPECT -> DEAD` after `cluster.swim.suspicion-timeout-ms`. DEAD/LEFT
   members are removed after `cluster.swim.dead-member-timeout-ms`.

A broken TCP link is the *first* line of detection: the transport heartbeat watchdog calls
`onLinkClosed`, which immediately marks a currently-ALIVE peer `SUSPECT` so SWIM never lags behind
the transport.

### Gossip dissemination

Every SWIM packet **piggybacks** a bounded set of "hot" membership facts
(`cluster.swim.gossip-fanout` per packet). Each fact is retired after it has been disseminated
roughly `3*log2(n+1)` times, so facts spread epidemically across the cluster in `O(log n)` rounds
without flooding.

### Incarnation numbers & refute

Each member carries a monotonic **incarnation**. The registry merge rules use it so gossiped facts
converge regardless of arrival order and a stale fact never overwrites a fresher one:

- `ALIVE(j)` wins if `j > current` (always), or `j == current` only to refute a `SUSPECT`.
- `SUSPECT(j)` applies if `j >= current` and the member is not DEAD.
- `DEAD(j)` applies if `j >= current` and is sticky.
- `LEFT` always applies.

If a node hears a `SUSPECT`/`DEAD` gossip **about itself**, it **refutes**: it bumps its own
incarnation past the gossiped value and re-asserts `ALIVE`. This defeats false positives (a slow
node that was wrongly suspected) and **restart ghosts** (a restarted node with the same node id —
its JoinAck reveals its last-known incarnation, which it bumps past, so the old DEAD record cannot
shadow it).

### Join & leave

- **Join.** A joining node sends a `SwimJoinPacket` over the seed link; the seed answers with a
  `SwimJoinAckPacket` carrying the full member snapshot, which seeds discovery + the
  anti-ghost incarnation bump.
- **Leave.** A graceful `stop()` floods a `SwimLeavePacket` (with a bumped incarnation) to all peers
  before tearing links down, so peers converge to `LEFT` almost instantly instead of waiting out the
  suspicion timeout.

---

## At-Least-Once Delivery (Phase C/D)

Cluster forwarding is **at-least-once**: a publish accepted by a node is delivered to every
interested peer at least once, surviving link drops, partitions and reconnects, with **end-to-end
de-duplication** so subscribers never see a logical message twice.

### Two independent sequence spaces

Because interest-based routing (below) means a peer only receives the publishes of topics it cares
about, the *origin* sequence stream over any one link is sparse (full of holes). The reliability
machinery therefore separates two concerns:

- **`originSeq` — de-duplication.** Each node stamps every publish with
  `(originNodeId, originEpoch, originSeq)`, a dense per-origin sequence. The receiver de-dups on this
  identity: a logical message is delivered to local subscribers **at most once**, no matter how many
  times or over how many paths it arrives.
- **`linkSeq` — reliability.** For each *target*, the sender assigns a dense, gap-free per-target
  `linkSeq` to every frame actually handed to that target. The contiguous ACK / backfill / gap
  machinery operates purely on `linkSeq`, so it stays gap-free even though the origin stream the peer
  sees is sparse.

`epoch` distinguishes incarnations of the same node id (a restart), so post-restart sequences are
never confused with pre-restart ones.

### ReplayBuffer, ACK, Backfill, Gap

- **ReplayBuffer.** Each target has a per-target `ReplayBuffer` of sent-but-unacked frames, keyed by
  `linkSeq`, capped at `cluster.replay.capacity`. It **survives link teardown**, so a reconnect to
  the same node id can backfill everything the peer never acknowledged. It is freed only after
  `cluster.origin.expiry-ms` with no link.
- **Cumulative ACK.** The receiver periodically (`cluster.ack-interval-ms`) sends its contiguous
  `linkSeq` high-water mark; the sender evicts everything up to it from the ring. ACKs are only sent
  when the frontier advanced (no idle spam).
- **Resume + Backfill.** On (re)connect the receiver sends a resume request with its last contiguous
  `linkSeq`; the sender replays every buffered frame beyond it. The receiver's link tracker drops any
  frame it already accepted, so backfill is idempotent.
- **Gap.** If the requested range was already evicted (or a frame was dropped under backpressure), a
  `ClusterGapPacket` tells the receiver to resync its frontier forward past the lost range. This is
  the **bounded, explicit** data-loss path; it is counted (`cluster_gapEvents`) and logged
  (`event=gap` / `event=data-loss`).

### Backpressure

Backpressure is absorbed **only** on each link's writer thread via its ReplayBuffer
(`cluster.replay.backpressure.policy` = `BLOCK` then degrade to a gap, or `DROP_GAP`). The local
publish path is **never** blocked by a slow or partitioned peer.

---

## Interest-Based Routing (Phase D)

Instead of broadcasting every publish to every peer, a node forwards a publish **only to peers that
have a subscriber for that topic**. This is driven by a cluster-wide `InterestRoutingTable`
(`topic -> interested remote node ids`).

- **Delta propagation.** A node's first local subscriber for a topic (or the loss of its last)
  bumps a monotonic interest version and floods a best-effort `ClusterInterestPacket` delta to all
  peers.
- **Anti-entropy.** A periodic full-state `ClusterInterestSyncPacket`
  (`cluster.interest.sync-interval-ms`) heals any lost delta; a new link gets an immediate full sync
  on connect. The shared monotonic version per node makes deltas and full syncs idempotent and
  out-of-order-safe.
- **Exact match.** Interest matching is **exact** on the normalized topic key — there is
  intentionally **no wildcard / prefix matching**.
- **Subscribe/publish race.** Optionally, `cluster.interest.broadcast-grace-ms` opens a short window
  after a topology change during which the affected topic is flooded to *all* peers, closing the
  ~RTT race where a fresh remote subscription's interest has not yet reached the publisher. `0`
  disables it (no extra traffic).
- **Partition safety.** A peer's interest is **not** dropped on link teardown — it is reclaimed only
  when the target's replay state expires (a genuine leave). This keeps partition-time publishes
  flowing into the peer's replay buffer so they can be backfilled on heal.

Each publish *not* sent to a connected-but-uninterested peer is counted as
`cluster_interestRoutedSkipped` (the direct saving over the old broadcast).

---

## Security — HMAC handshake (Phase B)

Every peer link is authenticated with a mutual **HMAC-SHA-256 challenge-response** handshake using
the shared `cluster.secret`; the secret is never transmitted. Optionally the whole channel can run
over TLS (`cluster.tls.*`). See **[security.md](security.md)** for the full transcript, domain
separation and the TLS relationship.

---

## Consistency & Ordering Guarantees

Avenue clustering is an **AP** system (available + partition-tolerant; eventually consistent). It
deliberately favours availability and low latency over global ordering.

| Guarantee                         | Holds?  | Notes                                                                 |
|-----------------------------------|---------|-----------------------------------------------------------------------|
| Per-origin FIFO                   | **Yes** | Messages from one origin are delivered in publish order on the lossless path; with `cluster.strict-ordering=true` even forward gaps are held until contiguous. |
| Cross-origin ordering             | **No**  | Messages from *different* origins are unordered relative to each other. |
| At-least-once delivery            | **Yes** | Surviving reconnect/backfill, except an explicit bounded gap after capacity-exceeding loss. |
| No duplicate delivery (de-dup)    | **Yes** | The origin tracker drops logical duplicates end to end.               |
| Total / global order              | **No**  | There is no global sequencer; this is not a log/consensus system.     |
| Membership consistency            | Eventual | SWIM converges every node's view in `O(log n)` gossip rounds.         |

Under a partition each side stays available and keeps serving its local clients; on heal, buffered
messages are backfilled (bounded by replay capacity / origin expiry) and membership re-converges.

---

## Configuration

All keys live in `default.properties` (or a `config/*.properties` file, or the matching
`UPPER_SNAKE_CASE` env var). Clustering is off unless `cluster.enabled=true`.

| Key                                    | Default     | Meaning                                                              |
|----------------------------------------|-------------|----------------------------------------------------------------------|
| `cluster.enabled`                      | `false`     | Master switch for cluster mode.                                      |
| `cluster.node-id`                      | random UUID | Stable unique id for this node (**required** when enabled).         |
| `cluster.port`                         | `0`         | Cluster TCP port (`0` = ephemeral).                                  |
| `cluster.peers`                        | *(empty)*   | Comma-separated `host:port` static seeds for bootstrap.             |
| `cluster.secret`                       | *(empty)*   | Shared HMAC secret for the peer handshake.                          |
| `cluster.heartbeat-interval-ms`        | `5000`      | Peer-link heartbeat interval; watchdog fires at ~3x.               |
| `cluster.advertised-host`              | *(auto)*    | Host advertised to peers (empty = auto-detect local IP).            |
| `cluster.packet.max-size`              | `1048576`   | Max cluster frame size (larger than the client packet size for JoinAcks). |
| **At-least-once (Phase C)**            |             |                                                                      |
| `cluster.replay.capacity`              | `65536`     | Per-target replay buffer size (un-acked frames).                    |
| `cluster.replay.backpressure.policy`   | `BLOCK`     | Ring-full policy: `BLOCK` (wait, then gap) or `DROP_GAP`.           |
| `cluster.replay.offer-timeout-ms`      | `1000`      | How long a writer waits for ring space under `BLOCK`.              |
| `cluster.ack-interval-ms`              | `200`       | Cumulative ACK send interval.                                       |
| `cluster.strict-ordering`              | `false`     | Hold out-of-order `linkSeq`s until contiguous before delivery.      |
| `cluster.origin.expiry-ms`             | `300000`    | How long a replay buffer survives without a link before being freed.|
| **Interest routing (Phase D)**         |             |                                                                      |
| `cluster.interest.sync-interval-ms`    | `10000`     | Anti-entropy full interest-state sync period.                       |
| `cluster.interest.broadcast-grace-ms`  | `0`         | Grace window flooding a topic to all peers after a topology change (`0` = off). |
| **SWIM membership (Phase E)**          |             |                                                                      |
| `cluster.swim.probe-interval-ms`       | `1000`      | Direct probe period.                                                 |
| `cluster.swim.probe-timeout-ms`        | `500`       | Direct/indirect probe timeout.                                      |
| `cluster.swim.indirect-probe-count`    | `3`         | Number of helpers for an indirect probe.                            |
| `cluster.swim.suspicion-timeout-ms`    | `5000`      | How long a member stays SUSPECT before DEAD.                        |
| `cluster.swim.gossip-fanout`           | `4`         | Max gossip facts piggybacked per SWIM packet.                       |
| `cluster.swim.dead-member-timeout-ms`  | `30000`     | How long DEAD/LEFT members are retained before reaping.            |
| **TLS (Wave 5)**                       |             | See [security.md](security.md).                                     |
| `cluster.tls.enabled`                  | `false`     | TLS on the cluster transport.                                       |
| `cluster.tls.keystore-path` / `-password`       | *(empty)* | Keystore used by the accepting side.                      |
| `cluster.tls.truststore-path` / `-password`     | *(empty)* | Truststore used by the initiating side.                   |
| **Admin HTTP (Phase F)**               |             |                                                                      |
| `admin.http.enabled`                   | `false`     | Enable the read-only introspection endpoint.                        |
| `admin.http.port`                      | `0`         | Admin HTTP port (`0` = ephemeral).                                  |
| `admin.http.bind-address`              | `127.0.0.1` | Bind interface (loopback by default).                              |
| `admin.http.secret`                    | *(empty)*   | Optional `X-Admin-Secret` header value (empty = no auth).          |

---

## Admin HTTP Introspection (Phase F)

When `admin.http.enabled=true`, the node exposes a **read-only**, dependency-free HTTP endpoint
(built on the JDK `com.sun.net.httpserver`, **no new dependency**) bound to `127.0.0.1` by default.
Every endpoint is a side-effect-free `GET` that returns an **immutable snapshot** of internal state —
no live, mutable internals are ever exposed. An optional `X-Admin-Secret` header gates access
(constant-time compared).

| Endpoint               | Returns                                                                         |
|------------------------|---------------------------------------------------------------------------------|
| `GET /health`          | `{status, nodeId, activePeers, membersAlive, membersSuspect, membersDead}`. `200` healthy, `503` self-unhealthy. |
| `GET /metrics`         | Flat JSON of every `AvenueMetrics` + `ClusterMetrics` getter. With `Accept: text/plain`, Prometheus text format. |
| `GET /metrics/prometheus` | The same metrics in Prometheus text exposition format.                       |
| `GET /cluster/members` | SWIM member list (`nodeId, state, incarnation, host, clusterPort, stateChangedAtMillis`) + aggregate counts. |
| `GET /cluster/routing` | Interest routing table (`topic -> interested nodeIds`) + `routingTableTopicCount`. |
| `GET /cluster/peers`   | Per-peer-link details (`nodeId, running, replayDepth, ingestDepth, outboundLinkSeq, contiguousLinkSeq`). |

Example:

```bash
curl -s http://127.0.0.1:9180/health
curl -s -H 'Accept: text/plain' http://127.0.0.1:9180/metrics
curl -s -H 'X-Admin-Secret: <secret>' http://127.0.0.1:9180/cluster/members
```

### Structured cluster event logs

The most important cluster lifecycle transitions are emitted through a dedicated logger
`de.kyle.avenue.cluster.events` in a consistent `key=value` format that is trivial to grep/parse:

```
INFO  event=join nodeId=node-2 incarnation=7 host=10.0.0.2 port=7100
INFO  event=peer-connected nodeId=node-2 host=10.0.0.2 port=7100
INFO  event=backfill-completed nodeId=node-2 messages=128 fromLinkSeq=4096
WARN  event=suspect nodeId=node-3 incarnation=4
WARN  event=heartbeat-timeout nodeId=node-3
WARN  event=handshake-auth-failure remote=/10.0.0.9:51234
ERROR event=dead nodeId=node-3 incarnation=4
ERROR event=data-loss nodeId=node-3 resyncContiguousLinkSeq=8190
```

Severity maps to operational importance: INFO for normal lifecycle, WARN for degraded-but-recoverable,
ERROR for durable problems (dead / data-loss). Configure this logger independently in `logback.xml`.

---

## Metrics

Cluster counters/gauges live in `de.kyle.avenue.metrics.ClusterMetrics` and are folded into the
periodic metrics snapshot log as well as the `/metrics` endpoint. Notable values: `messagesForwarded`,
`messagesReceived`, `messagesDeduped`, `messagesDropped`, `activePeerLinks`, `replayBufferDepth`,
`clusterBackfillMessages`, `clusterGapEvents`, `acksSent`/`acksReceived`,
`interestUpdatesSent`/`interestUpdatesReceived`, `interestRoutedSkipped`, `routingTableTopicCount`,
`membersAlive`/`membersSuspect`/`membersDead`, `joinEvents`/`leaveEvents`/`suspectEvents`/`deadEvents`,
`indirectProbesSent`, `gossipMessagesSent`/`gossipMessagesReceived`, `incarnationConflicts`,
`handshakeAuthFailures`.

---

## Benchmarks & Chaos (Phase F)

Two standalone harnesses live under `src/test` as plain `main` classes (NOT `*Test`, so `mvn test`
never runs them):

- **`de.kyle.avenue.benchmark.ClusterThroughputBenchmark`** — an in-process mesh (2-3 nodes) with a
  publisher on node1 and subscriber(s) on the others, measuring cross-node delivered msg/s and
  per-message p50/p99 latency (publish timestamp embedded in the payload).

  ```bash
  mvn -q -pl avenue-server test-compile
  mvn -q -pl avenue-server exec:java -Dexec.classpathScope=test \
      -Dexec.mainClass=de.kyle.avenue.benchmark.ClusterThroughputBenchmark \
      -Dexec.args="messages=200000 nodes=3 nodelay=true"
  ```

- **`de.kyle.avenue.chaos.ClusterChaosSoak`** — a 3-node soak that injects random
  partition/slow-peer/crash-restart faults under a continuous publish stream, then checks invariants
  (no duplicate delivery, bounded loss only, membership converges, no thread leak).

  ```bash
  mvn -q -pl avenue-server exec:java -Dexec.classpathScope=test \
      -Dexec.mainClass=de.kyle.avenue.chaos.ClusterChaosSoak \
      -Dexec.args="minutes=2"
  ```

A reusable `de.kyle.avenue.integration.cluster.ClusterTestHarness` provides N-node bring-up,
polling-based convergence helpers and the partition/heal/slowPeer/crash/restart fault helpers for new
tests.

---

## Known Limits & Future Work

- **No total order / consensus.** Avenue is AP; there is no global sequencer, no linearizability and
  no quorum. Cross-origin order is not defined. If a strongly-consistent log is required it must be
  layered on top.
- **Bounded loss window.** Loss is possible (and explicit, via a gap) only when a partition outlasts
  `cluster.origin.expiry-ms` or a backlog exceeds `cluster.replay.capacity`. Size these for the
  worst-case partition you must tolerate.
- **Exact-match interest only.** No wildcard / prefix topic interest; each exact topic is routed
  independently.
- **Full-mesh links.** Every node maintains a link to every other node, so link count grows `O(n^2)`.
  Fine for small/medium clusters; a relay/super-peer topology would be needed at very large scale.
- **Shared cluster secret.** All nodes share one `cluster.secret` (no per-node credentials / cert
  pinning beyond optional TLS).
- **Write-coalescing (`cluster.batch.*`)** — *future work.* Per-link write-coalescing/batching to
  amortize syscalls and framing under very high publish rates is intentionally **not** implemented in
  Phase F (default-off design, deferred to keep the hot path simple and the change risk-free). The
  per-link writer + pre-serialized frames already make adding it localized.
- **`source` is client-declared** and spoofable (see security.md, E15).
