# Avenue Wire Protocol

Avenue's entire wire protocol — both the client-facing plane and the node-to-node cluster control
plane — is **Protobuf** (proto3). This document describes the final wire format, the message catalogue
per envelope, the versioning rules and the measured JSON→Protobuf performance gain.

> The `.proto` schemas under [`avenue-server/src/main/proto/`](../avenue-server/src/main/proto/) are
> the **single source of truth** for the wire format. This document is a human-readable companion; if
> the two ever disagree, the schemas win.

---

## Framing

Every message on a TCP stream is length-prefixed so multiple messages can be read back-to-back from a
single connection:

```
  [4 bytes: big-endian int32 payload length][payload bytes (Protobuf envelope)]
```

- The 4-byte length prefix is written/read in exactly one place,
  `de.kyle.avenue.serialization.PacketFraming`. A negative or oversized length (beyond the configured
  `*.packet.max-size`) is treated as a desynced or hostile peer and the connection is dropped.
- The payload is the serialized bytes of a single top-level **envelope** message
  (`ClientEnvelope` on the client plane, `ClusterEnvelope` on the cluster plane), produced and parsed
  by `de.kyle.avenue.serialization.WireCodec`. The codec deals only with the bare payload; framing is
  added/removed by `PacketFraming`.

## Envelopes & oneof dispatch

There is exactly **one envelope message per frame**. Each envelope is a Protobuf `oneof`: the set
`oneof` case *is* the message type. This replaces the legacy JSON `header.name` string-based dispatch
— there is no separate type tag on the wire, the field number of the set `oneof` arm carries it.

- **`ClientEnvelope`** — client ⇄ server application traffic.
- **`ClusterEnvelope`** — node ⇄ node cluster control + forwarding traffic.

The receiver switches on `envelope.getMsgCase()` and reads the corresponding sub-message
(`getPublishInbound()`, `getClusterPublish()`, …). The `InboundPacketHandler` / cluster dispatcher
guarantee the expected case is set before a handler runs.

### Client plane — `ClientEnvelope` (`client.proto`)

| oneof field        | Message             | Purpose                                              |
|--------------------|---------------------|------------------------------------------------------|
| `auth_request`     | `AuthTokenRequest`  | Client presents the shared secret.                   |
| `auth_response`    | `AuthTokenResponse` | Server returns the session token.                    |
| `subscribe`        | `Subscribe`         | Subscribe to a topic (carries the token).            |
| `subscribe_ack`    | `SubscribeAck`      | Server acknowledges a subscription.                  |
| `publish_inbound`  | `PublishInbound`    | Client publishes a message (topic, source, token, data). |
| `publish_outbound` | `PublishOutbound`   | Server delivers a message to a subscriber (topic, source, data). |

### Cluster plane — `ClusterEnvelope` (`cluster.proto`, `common.proto`)

| oneof field             | Message                | Purpose                                              |
|-------------------------|------------------------|------------------------------------------------------|
| `cluster_publish`       | `ClusterPublish`       | Cross-node publish forward (origin identity, epoch, `seq`, `linkSeq`, data). |
| `cluster_ack`           | `ClusterAck`           | Cumulative per-origin ACK frontier (`repeated AckEntry`). |
| `cluster_resume`        | `ClusterResume`        | Resume request after (re)connect (`repeated ResumeEntry`). |
| `cluster_gap`           | `ClusterGap`           | Explicit bounded-loss resync past an evicted range.  |
| `cluster_interest`      | `ClusterInterest`      | Interest delta (ADD/REMOVE a topic).                 |
| `cluster_interest_sync` | `ClusterInterestSync`  | Full interest-state anti-entropy sync.               |
| `cluster_auth_hello`    | `ClusterAuthHello`     | HMAC handshake — initiator Hello (`nonce` as bytes). |
| `cluster_auth_challenge`| `ClusterAuthChallenge` | HMAC handshake — acceptor Challenge (`nonce`/`proof` as bytes). |
| `cluster_auth_response` | `ClusterAuthResponse`  | HMAC handshake — initiator Response (`proof` as bytes). |
| `heartbeat`             | `Heartbeat`            | Peer-link liveness heartbeat.                        |
| `swim_ping`             | `SwimPing`             | SWIM direct probe (piggybacks `GossipUpdate`s).      |
| `swim_ack`              | `SwimAck`              | SWIM probe ack (piggybacks gossip).                  |
| `swim_ping_req`         | `SwimPingReq`          | SWIM indirect-probe request.                         |
| `swim_ping_req_ack`     | `SwimPingReqAck`       | SWIM indirect-probe ack.                             |
| `swim_join`             | `SwimJoin`             | SWIM join over a seed link.                          |
| `swim_join_ack`         | `SwimJoinAck`          | Full member snapshot on join (`repeated GossipUpdate`). |
| `swim_leave`            | `SwimLeave`            | Graceful leave announcement (bumped incarnation).    |

`common.proto` holds the shared membership types used by the cluster plane: the `MemberStateProto`
enum (`ALIVE=0`, `SUSPECT`, `DEAD`, `LEFT` — `0` is `ALIVE` so the proto3 default matches the legacy
JSON default) and the `GossipUpdate` / `MemberProto` messages.

### HMAC handshake fields are `bytes`

The cluster HMAC challenge-response (`ClusterAuthHello` / `ClusterAuthChallenge` /
`ClusterAuthResponse`) carries the `nonce` and `proof` values as Protobuf **`bytes`** — binary on the
wire. Under the legacy JSON protocol the same values were hex/base64 **strings**. The HMAC computation
itself is unchanged (same transcript, same domain separation); only the on-wire representation became
raw bytes. See [`security.md`](security.md) for the handshake transcript and domain-separation rules.

---

## Versioning rules (proto3)

The schemas evolve under standard proto3 compatibility discipline so a newer and an older node can
always parse each other's frames:

- **Never reuse a field number.** A retired field's number is gone forever (reserve it with
  `reserved` if needed). Reusing a number silently corrupts data across versions.
- **New fields are additive only.** Add new fields with fresh, previously-unused numbers. Old peers
  ignore unknown fields; new peers see proto3 defaults for fields an old peer omits.
- **Extend a `oneof` only at the end**, with the next free field number. Existing `oneof` arms keep
  their numbers, so an old peer that does not know a new arm simply sees "no case set" and can drop
  or ignore the frame rather than misinterpret it.
- **Do not change a field's type or number**, and do not rename the message in a way that changes its
  identity. Renaming a field is wire-compatible (the number is what matters) but should be avoided for
  clarity.
- Enum `0` stays the safe default (`ALIVE` for `MemberStateProto`); never repurpose value `0`.

## Build

The `.proto` schemas are compiled to Java during the Maven `generate-sources` phase by the
`protobuf-maven-plugin` (`avenue-server/pom.xml`). `protoc` is pulled from Maven Central as a
platform-specific executable artifact (selected via `os-maven-plugin`'s `${os.detected.classifier}`),
so **no system `protoc` installation is required**. Generated sources land in
`target/generated-sources/protobuf` (package `de.kyle.avenue.proto`) and are added to the build
automatically. Run a normal `mvn clean test` and the wire classes are regenerated from the schemas.

---

## Performance (JSON vs Protobuf)

The migration was measured end-to-end with the standalone harnesses under
`avenue-server/src/test/java/de/kyle/avenue/benchmark/` (all runs `messages=200000`,
`nodelay=true`, JDK 21 / Apple Silicon, in-process). The JSON baseline is the measurement taken before
the cutover.

### Wire size

Mean serialized payload size of a representative message (payload bytes only; the identical 4-byte
length prefix is excluded), measured by `WireSizeComparison` (Protobuf via `WireCodec`, JSON as the
reconstructed legacy `{header,body}` form):

| Message                          | JSON `{header,body}` | Protobuf | Saving       |
|----------------------------------|----------------------|----------|--------------|
| `PublishOutbound` (client fan-out) | 175 bytes          | 94 bytes | **−46.3 %**  |
| `ClusterPublish` (cross-node)      | 266 bytes          | 125 bytes| **−53.0 %**  |

The cluster forward saves more because its frame carries more structured header fields (origin
identity, epoch, two sequence numbers) that Protobuf encodes as compact varints instead of quoted JSON
key/value pairs.

### Throughput & latency

| Scenario               | JSON baseline (measured) | Protobuf (measured)            | Delta          |
|------------------------|--------------------------|--------------------------------|----------------|
| Single-node deliveries | ~156k msg/s              | ~134k msg/s                    | comparable¹    |
| Single-node publishes  | ~39k msg/s               | ~34k publishes/s               | comparable¹    |
| Cluster, 2 nodes       | ~51k msg/s (p50 4 ms, p99 100 ms) | ~58–61k msg/s          | **+15–20 %**   |
| Cluster, 3 nodes       | ~63k msg/s               | ~58–80k msg/s                  | **on par … +25 %** |

¹ Single-node throughput is dominated by the per-message socket framing / fan-out plumbing rather than
the encoder, so it is in the same band as the JSON baseline (run-to-run variance ±10–15 % on a laptop);
the decisive win there is the **~46–53 % smaller wire size**, which lowers socket-buffer pressure and
bytes-on-the-wire. The cluster planes — which serialize a larger header on every cross-node hop — show
the clearest throughput improvement, in line with the bigger wire-size saving.

> Note on cluster latency: these runs use a publisher that blasts all 200k messages without pacing, so
> the reported p50/p99 are **throughput-bound queueing latencies** (a deep in-flight queue), not the
> steady-state per-message latency the JSON baseline's `p50 4 ms` reflects. The throughput figure is
> the apples-to-apples comparison; latency under saturation is expected to be high for both encodings.

Reproduce:

```bash
export JAVA_HOME=/path/to/jdk-21
mvn -q -pl avenue-server test-compile
mvn -q -pl avenue-server exec:java -Dexec.classpathScope=test \
    -Dexec.mainClass=de.kyle.avenue.benchmark.WireSizeComparison
mvn -q -pl avenue-server exec:java -Dexec.classpathScope=test \
    -Dexec.mainClass=de.kyle.avenue.benchmark.ThroughputBenchmark
mvn -q -pl avenue-server exec:java -Dexec.classpathScope=test \
    -Dexec.mainClass=de.kyle.avenue.benchmark.ClusterThroughputBenchmark \
    -Dexec.args="messages=200000 nodes=2 nodelay=true"   # and nodes=3
```
