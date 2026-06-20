# Avenue Performance-Baseline (Stufe 0)

Messgetriebenes Performance-Programm — festgehaltene Ausgangslage, gemessen mit der
neuen `LoadHarness` (`avenue-server/src/test/java/de/kyle/avenue/benchmark/LoadHarness.java`).
In Stufe 0 wurde **kein** Produktivcode geaendert; dies ist ausschliesslich der Nullpunkt,
gegen den die Folgestufen gemessen werden.

## Rahmenbedingungen

| Feld | Wert |
| --- | --- |
| Datum | 2026-06-20 |
| Branch | `feature/highperf-cluster-pubsub` |
| Commit (Baseline-Messung, vor Harness-Commit) | `ac8dc93` |
| Maschine | Apple Silicon (arm64) |
| JDK | Amazon Corretto 21.0.5 |
| Topologie | Single-Node (`SingleNodeServer`, in-proc, ephemerer Port) |
| Transport | Raw-Socket, Protobuf-`ClientEnvelope` via `WireCodec` + `PacketFraming`, `TCP_NODELAY=true` |
| Warm-up | 5 s (verworfen) je Messpunkt |
| Messfenster | 10 s Steady-State je Messpunkt |

## Wie gemessen wurde

Zwei Modi der `LoadHarness`:

- **Durchsatz** (`rate=0`): Publisher feuern ungebremst. Getrennt ausgewiesen werden die
  akzeptierte **Publish-Rate** (Publishes/s, die im Messfenster auf den Draht gingen,
  aggregiert ueber alle Publisher) und die **Delivery-Rate** (empfangene Frames/s ueber
  alle Subscriber, inklusive Fan-out).
- **Latenz** (`rate>0`): Jeder Publisher ist per Nanosekunden-Pacing auf eine feste Rate
  begrenzt. Jeder Payload traegt den Publish-Zeitstempel; der Subscriber berechnet die
  End-to-End-Latenz und die Harness liefert p50/p99/p999/max aus einem sortierten `long[]`.

Beide Modi durchlaufen zuerst eine separate Warm-up-Phase (Ergebnisse verworfen), bevor das
Steady-State-Fenster zaehlt. Die Bereitschaft ist deterministisch: jeder Subscriber blockiert
auf seinem `SubscribeAck`, bevor irgendein Publisher startet (kein Sleep als Sync-Mittel).

### Reproduktion

```bash
export JAVA_HOME=/Users/.../corretto-21.0.5/Contents/Home

# (a) Durchsatz-Baseline
/opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
    -Dexec.classpathScope=test \
    -Dexec.mainClass=de.kyle.avenue.benchmark.LoadHarness \
    -Dexec.args="publishers=4 subscribers=4 msgSize=64 seconds=10"

# (b) Latenz-Baseline (fixe Last 50 000 msg/s je Publisher)
/opt/homebrew/bin/mvn -q -pl avenue-server exec:java \
    -Dexec.classpathScope=test \
    -Dexec.mainClass=de.kyle.avenue.benchmark.LoadHarness \
    -Dexec.args="publishers=4 subscribers=4 msgSize=64 rate=50000 seconds=10"
```

## Gemessene Baseline-Zahlen

Topologie fuer beide Laeufe: `publishers=4 subscribers=4 topics=1 msgSize=64` (Fan-out = 4).

### (a) Durchsatz (`rate=0`, seconds=10)

| Metrik | Wert |
| --- | --- |
| Publish-Rate (akzeptiert, aggregiert) | **156 087 msg/s** |
| Delivery-Rate (inkl. Fan-out) | **378 024 msg/s** |

CSV: `4,4,64,156087,378024,...`

### (b) Latenz (`rate=50000` je Publisher = 200 000 msg/s Ziel, seconds=10)

| Metrik | Wert |
| --- | --- |
| Erreichte Publish-Rate | 107 941 msg/s |
| Delivery-Rate (inkl. Fan-out) | 434 893 msg/s |
| Latenz p50 | **25.876 ms** |
| Latenz p99 | **271.672 ms** |
| Latenz p999 | **311.846 ms** |
| Latenz max | 322.582 ms |

CSV: `4,4,64,107941,434893,25.876,271.672,311.846,322.582,4000000`

> Hinweis: `rate=50000` je Publisher (200 000 msg/s Ziel aggregiert) liegt **oberhalb** der
> Single-Node-Publish-Kapazitaet (~156 k/s). Die erreichte Publish-Rate (107 941/s) bleibt daher
> unter dem Ziel und die Latenzen sind Saettigungs-/Queueing-Latenzen, kein Service-Niveau.
> Als Gegenprobe (nicht Teil der Pflicht-Baseline) liefert `rate=5000` je Publisher
> (20 000 msg/s aggregiert, klar unter Saettigung) saubere Service-Latenzen:
> **p50 = 0.111 ms, p99 = 2.398 ms, p999 = 13.704 ms** bei Publish 19 765/s und
> Delivery 79 051/s (exakt 4x Fan-out). Das belegt, dass das Latenz-Pacing der Harness korrekt
> arbeitet und die hohen 50k-Zahlen tatsaechlich die Saettigungsgrenze des aktuellen Hot-Paths
> markieren.

## Stufen-Fortschritt

Folgestufen tragen ihre Zahlen hier nach (gleiche Topologie `4/4/64`; Latenz aus dem
50k-Lauf, damit die Stufen am selben Saettigungs-Arbeitspunkt vergleichbar sind).

| Stufe | Publish (msg/s) | Delivery (msg/s) | p50 (ms) | p99 (ms) |
| --- | --- | --- | --- | --- |
| 0 — Baseline | 156 087 | 378 024 | 25.876 | 271.672 |
| 1 — Mechanische Hot-Path-Wins | | | | |
| 2 — Protokoll-Pipelining / Batched Publish | | | | |
| 3 — Event-Loop-IO-Rewrite | | | | |
| 4 — Allokations-/GC-Disziplin | | | | |
