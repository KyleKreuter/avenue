# Avenue Clustering — Wave 4 Design Document

## Overview

Avenue v1 clustering provides a masterless, full-mesh pub-sub broadcast. Multiple Avenue
nodes form a peer group without any central coordinator. A message published to any node is
forwarded to all other connected nodes so that subscribers on any node receive it.

## Architecture

```
Client A ---[TCP:clientPort1]--- Node 1 ---[TCP:clusterPort1]---+
                                                                 | (full-mesh)
Client B ---[TCP:clientPort2]--- Node 2 ---[TCP:clusterPort2]---+
```

Each node runs two independent TCP servers:
- **Client port** (`server.port`): existing pub-sub protocol, unchanged.
- **Cluster port** (`cluster.port`): peer-to-peer links, authenticated, separate framing loop.

## Wire Format — Cluster Link

The cluster link reuses `PacketFraming` (4-byte big-endian int32 length prefix + UTF-8 JSON
payload), the same format as the client protocol.

### Handshake

On connection establishment (either direction) the connecting side sends a
`ClusterHelloPacket` immediately:

```json
{
  "header": { "name": "ClusterHelloPacket", "nodeId": "<uuid>", "secret": "<cluster.secret>" },
  "body":   {}
}
```

The accepting side validates `secret` using constant-time comparison. On mismatch the
connection is closed immediately. On success the accepting side sends its own
`ClusterHelloPacket` back (the responder hello). After both hellos have been exchanged the
link is considered **CONNECTED** and enters the normal message loop.

### ClusterPublishPacket

Forwarded publishes travel as:

```json
{
  "header": {
    "name":        "ClusterPublishPacket",
    "topic":       "<normalized topic>",
    "source":      "<original client source>",
    "originNodeId":"<uuid of the node that received the local publish>",
    "messageId":   "<uuid per message>"
  },
  "body": {
    "data": "<message data>"
  }
}
```

### ClusterHeartbeatPacket

Sent periodically from each side on an idle link:

```json
{
  "header": { "name": "ClusterHeartbeatPacket", "nodeId": "<uuid>" },
  "body":   {}
}
```

## Forwarding and Loop Prevention

### Single-Hop Rule

Avenue uses a **full-mesh** topology: every node connects to every other node. Therefore a
message only needs to travel one hop.

- When a **local client** publishes: deliver locally AND forward to all connected peers.
  Tag with `originNodeId = this.nodeId`.
- When a `ClusterPublishPacket` arrives from a **peer**: deliver locally ONLY, never
  re-forward. This is safe because all other peers are also directly connected to the
  originator and will receive (or already received) the message directly.

### Deduplication Safety Net

In addition to the single-hop rule a bounded **seen-set** of `messageId` UUIDs is maintained
per node (LRU-evicting after 10 000 entries). If a `ClusterPublishPacket` with an already-
seen `messageId` arrives it is silently dropped. This catches double-delivery that could
arise from misconfiguration (e.g., non-full-mesh, multiple static seed entries pointing at
the same node) without penalising the normal path.

## Membership and Failure Detection

### Static Seed List

v1 uses a static peer list (`cluster.peers`) in the format `host:port,...`. There is no
dynamic discovery. Both sides of a full-mesh link are configured with each other's address,
but because both may try to connect simultaneously, duplicate outbound connections are
acceptable at the TCP level — the handshake `nodeId` is used to detect and close the
second one.

### Reconnect with Exponential Backoff

A `PeerLink` that loses its TCP connection re-attempts with exponential backoff starting at
1 s, doubling up to a cap of 30 s. The peer is marked **DOWN** while disconnected; a
heartbeat timeout (configurable, default 15 s without a heartbeat) also marks it DOWN and
triggers reconnect.

### Heartbeat

Each connected peer link sends a `ClusterHeartbeatPacket` every `cluster.heartbeat-interval`
milliseconds (default 5 000 ms). The receiver resets a watchdog timer. If no heartbeat
arrives within `cluster.heartbeat-interval * 3` milliseconds the link is considered dead and
is closed, triggering the reconnect loop.

## Non-Blocking Forwarding

The forward path from the local `PublishMessageInboundPacketHandler` to peer sockets is
**decoupled** via a bounded `LinkedBlockingQueue` per `PeerLink`. The handler enqueues and
returns immediately. A dedicated virtual-thread writer drains the queue and writes frames.
If the queue is full (slow peer) the forward is **dropped for that peer** (at-most-once
delivery). The local delivery path is never blocked.

## Consistency Model

- **AP (Availability + Partition Tolerance)** from the CAP theorem.
- **At-most-once delivery**: a message may be lost during a peer outage or queue overflow,
  but is never delivered more than once to a subscriber (dedup ensures this).
- **No total ordering**: messages from different nodes may arrive in different orders at
  different subscribers. There is no sequence numbering or vector clock.
- **Split-brain**: if the cluster partitions, each partition continues serving its own local
  clients. When reconnected, there is no reconciliation of missed messages — they are simply
  lost. This is acceptable for a real-time pub-sub use-case where timeliness > durability.

## Known Limitations of v1

1. **Static membership**: adding/removing nodes requires a restart of all nodes.
2. **No TLS on cluster links**: the shared-secret handshake provides basic authentication
   but the link is unencrypted.
3. **No persistence**: messages in flight are lost on crash.
4. **No message ordering guarantees** across nodes.
5. **Linear fan-out cost**: N nodes means N-1 cluster forwarding writes per local publish.
6. **Duplicate connections**: if both A→B and B→A connections are established, only one is
   used (the second is closed on `nodeId` collision detection), but there is a brief window
   of both being open.

## Future Work (v2+)

- **Gossip / SWIM protocol** for dynamic membership and failure detection without static
  seeds.
- **Topic sharding**: route a topic's messages to a designated owner node to avoid N-1 fan-
  out; subscribers on other nodes subscribe via the owner.
- **TLS** on cluster links.
- **Durable delivery**: persistent log per topic with subscriber offsets (similar to Kafka).
- **Vector clocks / causal ordering** for multi-node publish ordering guarantees.
