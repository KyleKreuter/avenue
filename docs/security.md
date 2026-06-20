# Avenue Security & Operations — Wave 5

## Overview

This document describes Avenue's security model and the operational hardening added in
Wave 5: optional TLS for both the client and the cluster transport (E14), the shared-secret
authentication model and its known spoofing limitation (E15), the backpressure policy (E17),
client liveness / idle-timeout handling (E16), and the lightweight metrics surface (E20).

All Wave 5 features are **off / generous by default** so an existing deployment keeps its
previous behaviour until the relevant configuration is set.

---

## Transport Security (TLS, E14)

TLS can be enabled independently for the client-facing port and the cluster transport. When
enabled, the relevant `ServerSocket`/`Socket` is replaced by its `SSLServerSocket`/`SSLSocket`
counterpart through a single abstraction (`de.kyle.avenue.net.SocketFactoryProvider`); the rest
of the codebase keeps working purely against `java.net.Socket`. The plain (non-TLS) path is the
default and is byte-for-byte unchanged.

### Client-port TLS

```properties
server.tls.enabled=true
server.tls.keystore-path=/etc/avenue/server.p12
server.tls.keystore-password=<keystore-password>
```

The keystore must be a **PKCS12** store containing the server certificate and its private key.
The server presents this certificate on every accepted connection. Client certificates are not
requested (no mutual TLS on the client port).

### Cluster-transport TLS

```properties
cluster.tls.enabled=true
cluster.tls.keystore-path=/etc/avenue/cluster.p12
cluster.tls.keystore-password=<keystore-password>
cluster.tls.truststore-path=/etc/avenue/cluster-truststore.p12
cluster.tls.truststore-password=<truststore-password>
```

For a peer link the **accepting** side uses the keystore (to present its certificate) and the
**initiating** side uses the truststore (to verify the peer certificate). In a full mesh every
node both accepts and initiates, so configure both stores on every node. The cluster shared
secret (`cluster.secret`) is still exchanged in the application-level handshake on top of TLS,
giving defence in depth: TLS protects the channel, the secret authenticates the peer identity.

### Generating a keystore

A self-signed PKCS12 keystore for testing/lab use can be produced with `keytool`:

```bash
keytool -genkeypair -alias avenue -keyalg RSA -keysize 2048 -validity 365 \
        -dname "CN=your-host" -ext "SAN=DNS:your-host,IP:10.0.0.1" \
        -storetype PKCS12 -keystore server.p12 -storepass changeit -keypass changeit
```

The TLS integration test (`TlsIntegrationTest`) generates exactly such a store at runtime, so no
certificate file is checked into the repository.

---

## Authentication Model (shared secret → token)

Authentication is a two-step shared-secret exchange:

1. A client sends an `AuthTokenRequestInboundPacket` containing the shared
   `server.authentication.secret`.
2. On a match the server replies with the configured `server.authentication.token`. Every
   secured operation (publish, subscribe) must carry this token in its packet header. The token
   is compared on the server before the operation is routed; a mismatch tears down the
   offending connection (when `server.packet.drop-unknown=true`).

The secret is verified using a constant-time comparison; the cluster handshake likewise uses a
constant-time comparison for `cluster.secret`.

### Known limitation E15 — `source` is client-declared and spoofable

The `source` field on a published message is **supplied by the client** and is **not** tied to
any per-client identity. Two consequences:

- Any authenticated client can publish a message claiming an arbitrary `source` (impersonation).
- There is **no per-client identity**: all clients share the same secret and receive the same
  token. The token proves "knows the shared secret", not "is client X".

This is acceptable for trusted-network deployments but must be understood before exposing Avenue
to untrusted clients.

#### Recommendations / outlook

- **Per-client tokens / credentials**: issue a distinct token per client (e.g. signed JWT or a
  lookup table) so the server can bind `source` to the authenticated identity and reject
  spoofed values.
- **mTLS on the client port**: require client certificates and derive identity from the
  certificate subject, removing the shared-secret single point of compromise.
- **Server-side `source` stamping**: ignore the client-declared `source` and stamp the
  authenticated identity instead.

---

## Cluster Peer Authentication — HMAC Challenge–Response (Phase B)

Cluster peer links are authenticated with a mutual **HMAC-SHA-256 challenge–response** handshake
over the (optionally TLS-protected) TCP connection, performed by
`de.kyle.avenue.cluster.ClusterHandshake` / `HmacAuthenticator` before any cluster traffic flows.
Both sides must possess the shared `cluster.secret`; the secret itself is **never sent on the
wire**.

### Flow

```
initiator                                   acceptor
   |  Hello(nodeId_i, nonce_i, host, port, inc) --->                       |
   |                       <--- Challenge(nodeId_a, nonce_a, host, port, inc, proof_a)
   |  Response(proof_i)                          --->                       |
   |                       (both verify; link established)                 |
```

1. The **initiator** sends a `Hello` carrying its node id, a fresh random `nonce_i` and its
   advertised contact info (host / cluster port / incarnation).
2. The **acceptor** replies with a `Challenge` carrying its own node id, a fresh random `nonce_a`,
   its contact info and a `proof_a = HMAC(secret, transcript)`.
3. The **initiator** verifies `proof_a`, then sends a `Response` with
   `proof_i = HMAC(secret, transcript)` over the same transcript with a different domain-separation
   tag.
4. The acceptor verifies `proof_i`. Only if both proofs verify is the link established and the peer
   registered as `ALIVE`.

### Transcript & domain separation

Each proof is an HMAC over a **transcript** that binds both nonces and both node identities
(`nonce_i`, `nonce_a`, `nodeId_i`, `nodeId_a`), so a proof cannot be replayed across handshakes or
reflected back to its sender. The initiator's and acceptor's proofs use **distinct
domain-separation tags** (a fixed label mixed into the HMAC input), so the two directions produce
different MACs over the same transcript — preventing a reflection attack where an attacker echoes
the acceptor's proof back as the initiator's.

> **Wire representation.** Since the Protobuf wire-protocol migration the `nonce` and `proof` values
> are carried as Protobuf **`bytes`** (raw binary) in the `ClusterAuthHello` / `ClusterAuthChallenge`
> / `ClusterAuthResponse` messages — previously they were transmitted as hex/base64 **JSON strings**.
> The HMAC computation (transcript, domain separation, constant-time verification) is **unchanged**;
> only the on-wire encoding became binary. See [protocol.md](protocol.md).

### Properties

- **Secret never on the wire** — only HMAC proofs are transmitted; the secret is the HMAC key.
- **Mutual** — both sides prove knowledge of the secret; a node that lacks it cannot complete
  either direction.
- **Replay-resistant** — the fresh per-handshake nonces make every transcript unique.
- **Constant-time verification** — proofs are compared with `MessageDigest.isEqual`.
- **Counted** — a failed proof increments `cluster_handshakeAuthFailures` and is logged as the
  structured event `event=handshake-auth-failure`.

### Relationship to TLS

The HMAC handshake and cluster TLS are **complementary, defence-in-depth** layers and are
configured independently:

- **TLS** (`cluster.tls.*`) secures the *channel*: confidentiality + integrity on the wire and,
  via the truststore, certificate-based authentication of the endpoint.
- **HMAC** authenticates the *cluster membership identity*: it proves the peer holds the shared
  cluster secret, independent of the transport.

Running both is recommended for production: TLS stops passive eavesdropping and active tampering,
while the HMAC secret stops an unauthorized node (that somehow reaches the port) from joining the
mesh. With TLS off, the HMAC handshake still authenticates peers but the channel is cleartext.

---

## Admin HTTP Introspection Secret (Phase F)

The read-only admin HTTP endpoint (see *Clustering* docs) is **disabled by default** and binds to
loopback (`127.0.0.1`) only. When enabled it can optionally be gated behind a shared secret:

```properties
admin.http.enabled=true
admin.http.bind-address=127.0.0.1
admin.http.port=9180
admin.http.secret=<admin-secret>     # empty = no auth
```

When `admin.http.secret` is set, **every** request must carry it in the `X-Admin-Secret` header.
The value is compared in **constant time** via `MessageDigest.isEqual`; a missing or wrong secret
returns `401`. As the endpoint is strictly read-only (only `GET`, no state mutation) and
loopback-bound by default, the secret is a second line of defence rather than the primary control —
keep the bind address on loopback or a management interface and do not expose it publicly.

---

## Backpressure Policy (E17)

Each client has a bounded outbound queue drained by a dedicated virtual-thread writer, so one
slow subscriber cannot cause head-of-line blocking for the fan-out to others. When a producer
cannot enqueue a packet within `server.outbound.queue.offer-timeout-ms`, the configured policy
decides the outcome:

```properties
server.backpressure.policy=DISCONNECT_SLOW_CONSUMER   # default
# or
server.backpressure.policy=DROP_MESSAGE
```

- **`DISCONNECT_SLOW_CONSUMER` (default)** — the slow client is disconnected. This protects
  server memory and the rest of the fan-out; the client is expected to reconnect and re-subscribe.
  This is the original pre-Wave-5 behaviour, now made explicit and configurable.
- **`DROP_MESSAGE`** — the individual packet that could not be enqueued in time is dropped and
  the connection stays open. Use this for best-effort topics where losing a single update is
  preferable to dropping the subscriber.

Both outcomes are recorded in metrics (`slowConsumerDisconnects` / `droppedMessages`). An
unknown or blank value falls back to the safe default.

Related tuning:

```properties
server.outbound.queue.capacity=1024
server.outbound.queue.offer-timeout-ms=100
```

---

## Liveness / Idle-Timeout (E16)

Dead and half-open client connections are reaped via a server-side socket **read** idle-timeout:

```properties
server.client.idle-timeout-ms=60000   # default 60s; 0 disables the idle-timeout
```

If no inbound bytes arrive from a client within this window, the read times out and the
connection is closed. The timer resets on every received frame, so any client that periodically
sends (publishes, re-subscribes, or an application-level keep-alive) stays connected.

> **Note:** the idle-timeout is an *inbound* (read) cutoff. A subscriber that only ever
> *receives* and never sends a byte will, by design, be reaped once the window elapses. Such
> clients should send a lightweight periodic message (e.g. a re-subscribe or keep-alive) to stay
> alive. Set `server.client.idle-timeout-ms=0` to disable the cutoff entirely for purely
> receive-only workloads.

### Max connections (E16)

```properties
server.max-connections=10000   # default 10000; 0 means unlimited
```

When the limit is reached, a newly accepted socket is closed immediately (the client observes an
EOF) and the rejection is counted in the `connectionsRejected` metric. Active connections are
tracked thread-safely and decremented on every disconnect.

---

## Metrics (E20)

Avenue ships a dependency-free metrics registry (`de.kyle.avenue.metrics.AvenueMetrics`) built on
`AtomicLong` counters and gauges — no Micrometer/Prometheus dependency is introduced. A snapshot
is logged periodically:

```properties
server.metrics.log-interval-seconds=30   # default 30s; 0 disables periodic logging
```

Exposed values (all also available via public getters for tooling/tests):

| Metric                       | Type    | Meaning                                                        |
|------------------------------|---------|----------------------------------------------------------------|
| `messagesPublished`          | counter | Local client publishes accepted and fanned out                 |
| `messagesDelivered`          | counter | Outbound packets enqueued for a subscriber                     |
| `droppedMessages`            | counter | Packets dropped under the `DROP_MESSAGE` backpressure policy    |
| `slowConsumerDisconnects`    | counter | Clients disconnected under `DISCONNECT_SLOW_CONSUMER`           |
| `totalConnectionsAccepted`   | counter | Connections admitted since startup                             |
| `connectionsRejected`        | counter | Connections rejected (e.g. max-connections limit reached)      |
| `activeConnections`          | gauge   | Currently open client connections                              |
| `subscriptionCount`          | gauge   | Currently active (client, topic) subscriptions                 |
| `maxOutboundQueueDepth`      | gauge   | High-water mark of observed per-connection outbound queue depth |

---

## Defaults Summary

| Setting                            | Default                     | Effect when default          |
|------------------------------------|-----------------------------|------------------------------|
| `server.tls.enabled`               | `false`                     | Plain TCP on the client port |
| `cluster.tls.enabled`              | `false`                     | Plain TCP on the cluster port|
| `server.client.idle-timeout-ms`    | `60000`                     | Reap silent connections after 60s |
| `server.max-connections`           | `10000`                     | Up to 10k concurrent clients |
| `server.backpressure.policy`       | `DISCONNECT_SLOW_CONSUMER`  | Drop slow consumers          |
| `server.metrics.log-interval-seconds` | `30`                     | Log a metrics snapshot every 30s |
| `admin.http.enabled`               | `false`                     | No admin HTTP endpoint       |
| `admin.http.bind-address`          | `127.0.0.1`                 | Loopback-only bind           |
| `admin.http.secret`                | *(empty)*                   | No `X-Admin-Secret` required |
