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

## Nordstern-Metrik: 1:1, ~100 B, Durchsatz bei p99 < 1 ms

Die Delivery-Rate mit Fan-out ist eine Vanity-Zahl (durch mehr Subscriber beliebig aufblasbar).
Die **echte, Redis-vergleichbare** Metrik ist **1 Publisher -> 1 Subscriber (Fan-out 1)**, ~100 B,
gemessen als **Publishes/s bei p99 < 1 ms**. Bei Fan-out 1 sind Publish- und Delivery-Rate
identisch — keine Mehrdeutigkeit.

Gemessen (`publishers=1 subscribers=1 topics=1 msgSize=100`):

| Last (1:1) | Publish/s | p50 (ms) | p99 (ms) | p999 (ms) |
| --- | --- | --- | --- | --- |
| Saettigung (`rate=0`) | **253 556** | — | — | — |
| 20 000/s | 19 953 | 0.033 | 0.081 | 1.313 |
| 50 000/s | 49 905 | 0.034 | 0.090 | 0.286 |
| 80 000/s | 79 460 | 0.038 | 0.106 | 1.135 |
| 120 000/s | 119 168 | 0.046 | **0.114** | 0.301 |

**Erkenntnis:** Latenz ist NICHT das Problem — p99 bleibt bis 120 k/s bei ~0.1 ms (10x Reserve zur
1-ms-Schranke; die p999-Ausreisser ~1 ms sind GC -> Stufe 4). Der einzige echte Hebel ist der
**Durchsatz-Deckel von ~253 k/s = ~3.9 us pro Nachricht** durch die Pipe.

**Ziel (Redis-Liga):** ~1M msg/s 1:1 bei p99 < 1 ms = **~1 us/Nachricht**. Heutige Luecke: **~4x
Durchsatz**, bei bereits vorhandener Latenz-Reserve.

## Stufen-Fortschritt (Nordstern: 1:1, 100 B)

Folgestufen tragen die 1:1-Headline hier nach: Saettigungs-Durchsatz, der p99 bei einem festen
Arbeitspunkt (`rate=120000`), und die abgeleiteten ns/Nachricht (= 1e9 / Saettigungsrate).

| Stufe | 1:1 Saettigung (msg/s) | ns/msg | p99 @120k (ms) |
| --- | --- | --- | --- |
| 0 — Baseline | 253 556 | ~3944 | 0.114 |
| 1 — Mechanische Hot-Path-Wins | 246 400 | ~4058 | 0.115 |
| 2 — Protokoll-Pipelining / Batched Publish | | | |
| 3 — Event-Loop-IO-Rewrite | | | |
| 4 — Allokations-/GC-Disziplin | | | |

> **Stufe 1 — ehrliche Einordnung.** Die vier Hot-Path-Optimierungen (encode-once Fan-out, Inline-
> Delivery statt Executor-Hop, gepufferter Read, einmalige Topic-Normalisierung) sind umgesetzt und
> alle 87 Tests bleiben gruen. Beim **Nordstern (1:1, Fan-out 1)** ist der **Saettigungs-Durchsatz
> unveraendert**: ~246k/s nach den Aenderungen vs. **frisch auf derselben Maschine gemessener**
> Baseline von ebenfalls ~244-250k/s (die 253k in Zeile 0 stammen von einem frueheren Lauf; tagaktuell
> liegt die Maschine bei ~247k). Der Grund ist strukturell: bei Fan-out 1 spart encode-once nichts
> (N=1, eine Serialisierung), und der Engpass ist die **1-Nachricht-in-flight-Ping-Pong-Latenz** des
> Request/Response-Benchmarks, nicht Server-CPU. Messbar verbessert hat sich die **Median-Latenz**:
> p50 @120k faellt von ~0.047 ms auf ~0.033 ms (≈ -30 %), weil die Inline-Delivery den Thread-Hop
> ueber den Executor pro Publish entfernt. p99 @120k bleibt im Rauschband (~0.11-0.13 ms).
>
> Wo encode-once tatsaechlich greift, ist hoeherer Fan-out: bei **1:16-Saettigung** steigt die
> Delivery-Rate von ~577k/s (Baseline) auf ~595k/s (Stufe 1), ~+3 % — der Gewinn skaliert mit dem
> Fan-out, ist hier aber noch klein, weil der Per-Subscriber-Socket-Write dominiert. Der echte
> Durchsatz-Hebel fuer den Nordstern liegt damit weiter in Stufe 2/3 (Pipelining / Event-Loop-IO),
> nicht in mechanischen CPU-Einsparungen.

> Zusatz-Diagnose (kein Erfolgskriterium): die Fan-out-4-Werte aus dem 50k-Lauf oben bleiben als
> Sekundaermessung erhalten, um Fan-out-Skalierung zu beobachten.
