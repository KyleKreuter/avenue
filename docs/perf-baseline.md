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
| 1.5 — `data` als `bytes` (opaker Passthrough) | 246 000 – 261 000 (Streuband) | ~3900 | 0.134 |
| 4a — Outbound-Encode ohne Builder (JFR: Alloc-Win) | ~290 000 (Paare, flach) | n/a | ~0.13 |
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

> **Stufe 1.5 — `data` als `bytes` (opaker ByteString-Passthrough), ehrliche Einordnung.**
> Profiler-Befund war: unter Last gingen relevante CPU-Anteile in
> `String.charAt`/`StringLatin1.charAt`/`String.<init>(byte[])` — also protobuf, das das
> `data`-`string`-Feld bei jedem Parse (bytes->String) und jedem Encode (String->bytes) Zeichen fuer
> Zeichen UTF-8-transcodierte, obwohl der Server die Nutzlast nie anschaut. Der Fix: `data` in
> `client.proto` (`PublishInbound`, `PublishOutbound`) und `cluster.proto` (`ClusterPublish`) von
> `string` auf `bytes` umgestellt (Feldnummern unveraendert, hartes Cut-over). Die Nutzlast fliesst
> jetzt als **immutable `ByteString`** vom Inbound-Parse bis zum Outbound-Encode durch — dieselbe
> Instanz wird mit dem encode-once-Fan-out-Envelope und dem Cluster-Forward geteilt; sie wird auf dem
> Server-Hotpath **nie** zu einem Java-`String`. `topic`/`source`/`token` bleiben `string`.
>
> **JFR-Gegencheck (isolierter Server, P=8-Last, 505 On-CPU-Samples):** Kein einziges
> `charAt`/`Utf8.encode`/`String.<init>`-Sample beruehrt noch die Nutzlast — die payload-bezogene
> Transcodierung ist **vollstaendig verschwunden** (0 Samples mit `getData`/payload im charAt/Utf8-
> Stack). Die Nutzlast erscheint jetzt als `ByteString.LiteralByteString.<init>` / `copyOfRangeByte`
> (reiner Byte-Copy). Der verbliebene String-/UTF-8-Anteil im Profil ist **per Design** Topic + Source
> (weiter `string`-Felder) plus `TopicSubscriptionHandler.normalize` (`toLowerCase` auf dem Topic) —
> also genau die Felder, die `string` bleiben sollten, NICHT die Nutzlast.
>
> **Durchsatz (out-of-process, isolierter Server):** Bei **1:1-Saettigung** liegt der Durchsatz im
> selben Streuband wie vorher (246k–261k/s vs. Baseline 230–253k) — der Gewinn ist hier **innerhalb
> des Mess-Rauschens**, weil bei 100-B-Nutzlast und Fan-out 1 die Server-CPU vom Lock-/Queue-/Park-
> und protobuf-Builder-Overhead dominiert wird, nicht von der Payload-Transcodierung; und der
> 1:1-Benchmark ist ohnehin ping-pong-latenz- statt CPU-limitiert. **Unabhaengige Paare P=8** liegen
> stabil bei ~302–305k/s (Baseline ~298k). p99 @120k bleibt im Rauschband (~0.13 ms). Alle 87 Tests
> bleiben gruen (≥3x `mvn clean test`).
>
> **Fazit ehrlich:** Der erhoffte grosse Makro-Durchsatzsprung bleibt bei dieser Nutzlastgroesse aus —
> die ~40%-`charAt`-Zahl aus dem Ursprungsbefund stammte vermutlich aus einem Lauf mit groesserer
> Nutzlast/anderem Sampling. Was der Fix **belegbar** leistet: Er entfernt die Payload-UTF-8-
> Transcodierung als CPU-Block restlos und macht die Nutzlast zu einem geteilten, immutablen
> Byte-Blob (passt zum encode-once-Fan-out). Der Nutzen waechst linear mit der Nutzlastgroesse und mit
> dem Fan-out; bei grossen Payloads / hohem Fan-out ist das ein echter, dann auch im Durchsatz
> sichtbarer Hebel.

## Out-of-process Messung (Server und Lastgenerator getrennt)

### Warum

Die `LoadHarness` bootet im Default-Modus den `SingleNodeServer` **in-proc** — also in
**derselben JVM** wie der Lastgenerator. Dadurch teilen sich Last und Server dieselben CPU-Kerne,
denselben GC und denselben JIT. Die Maschine saettigt dann bei ~300k msg/s, und gemessen wird die
Summe aus **Last + Server**, nicht die isolierte Server-Kapazitaet. Die Frage "kann der Server
mehr, wenn ich mehr Publisher draufgebe?" laesst sich so nicht beantworten, weil der Lastgenerator
selbst CPU verbraucht.

Der `connect=`-Modus der Harness + die neue Klasse `BenchServer` trennen beides in **zwei
getrennte JVMs/Scheduler**: `BenchServer` laeuft in einem eigenen `java`-Prozess auf festem Port,
die Harness in einem zweiten Prozess verbindet sich per `connect=host:port`. Warm-up, getrennte
Raten, Latenz-Pacing, `SubscribeAck`-Bereitschaft, Teardown und Ausgabe sind identisch zum
In-JVM-Modus.

### Wie man es startet

```bash
export JAVA_HOME=/Users/.../corretto-21.0.5/Contents/Home

# Erster Positionsparameter = Server-Port, Rest wird an LoadHarness durchgereicht.
# Das Skript injiziert connect=127.0.0.1:PORT selbst (also NICHT selbst connect= setzen).

# (a) 1:1-Saettigung
./scripts/bench-split.sh 4180 publishers=1 subscribers=1 topics=1 msgSize=100 warmupSeconds=4 seconds=8

# (b) Publisher-Skalierung (gemeinsames Topic)
./scripts/bench-split.sh 4180 publishers=8 subscribers=1 topics=1 msgSize=100 warmupSeconds=4 seconds=8

# (b') Unabhaengige Paare (jeder Publisher eigenes Topic + eigener Subscriber)
./scripts/bench-split.sh 4180 publishers=8 subscribers=1 topics=8 msgSize=100 warmupSeconds=4 seconds=8

# (c) 1:1-Latenz bei festem Arbeitspunkt
./scripts/bench-split.sh 4180 publishers=1 subscribers=1 msgSize=100 rate=120000
```

`scripts/bench-split.sh` (a) baut den Test-Classpath via
`mvn -q -pl avenue-server dependency:build-classpath` + `target/classes:target/test-classes`,
(b) startet `BenchServer` als eigenen `java`-Prozess (NICHT `mvn exec:java`, damit es eine wirklich
getrennte JVM ist) und wartet auf die `BENCH SERVER READY`-Zeile, (c) startet `LoadHarness` als
zweiten `java`-Prozess mit `connect=127.0.0.1:PORT`, (d) killt am Ende den Server-Prozess.

`BenchServer` nutzt eine Direktwert-Test-Config (fester Port, `secret`/`token` passend zur Harness,
1-MiB-Packetsize, **idle-timeout 0** (kein Reaping waehrend der Messung), `TCP_NODELAY=true`,
unlimitierte Connections, grosse Outbound-Queue) und blockiert nach dem Binden, bis der Prozess
gekillt wird.

### Gemessene Zahlen (Out-of-process, msgSize=100, warmupSeconds=4, seconds=8)

Maschine: Apple Silicon (arm64), Corretto 21.0.5, Loopback (127.0.0.1), Server und Last in
getrennten JVMs.

| Szenario | Out-of-process (msg/s) | In-JVM-Vergleich (msg/s) |
| --- | --- | --- |
| 1:1 Saettigung | **230 000 – 253 000** (Lauf-Streuung) | ~226 000 (tagaktuell) / 253 000 (frueher) |
| Publisher-Skalierung, gemeinsames Topic, P=2 | 342 000 | — |
| Publisher-Skalierung, gemeinsames Topic, P=4 | 199 000 – 237 000 (stark streuend) | — |
| Publisher-Skalierung, gemeinsames Topic, P=8 | 329 000 | — |
| **Unabhaengige Paare P=2** (topics=2, sub=1) | **292 000** | — |
| **Unabhaengige Paare P=4** (topics=4, sub=1) | **295 000** | — |
| **Unabhaengige Paare P=8** (topics=8, sub=1) | **298 000 – 300 000** | ~265 000 (In-JVM, 8 unabh. Paare) |

1:1-Latenz bei `rate=120000` (Arbeitspunkt unterhalb Saettigung):

| Metrik | Out-of-process | In-JVM-Baseline |
| --- | --- | --- |
| Erreichte Publish-Rate | 118 923 msg/s | ~119 168 msg/s |
| p50 | 0.035 ms | ~0.046 ms |
| p99 | **0.133 ms** | **0.114 ms** |
| p999 | 0.553 ms | 0.301 ms |
| max | 2.619 ms | — |

### Schlussfolgerung: ~300k ist der echte Server-Deckel, nicht Last-Limitierung

Die entscheidende Messung sind die **unabhaengigen Paare** (jeder Publisher hat sein eigenes Topic
und seinen eigenen Subscriber, keine Fan-in-/Fan-out-Verzerrung). Hier **skaliert der Durchsatz
NICHT** mit der Publisher-Anzahl: P=2, P=4 und P=8 liefern alle ~292–300k msg/s — praktisch flach.
Waere die bisherige In-JVM-Messung last-limitiert gewesen (also der Server haette noch Reserven, nur
der Lastgenerator war der Engpass), muesste der Durchsatz in der getrennten Topologie mit mehr
Publishern deutlich steigen. Das tut er nicht.

Damit ist **~300k msg/s der echte Deckel des aktuellen Single-Node-Hot-Paths**, nicht ein Artefakt
des In-JVM-Messaufbaus. Die getrennte Messung verschiebt die Headline-Zahl nur marginal (1:1 von
~226k auf ~230–253k im selben Streuband; unabhaengige Paare von ~265k In-JVM auf ~298k
out-of-process — ein kleiner, aber kein qualitativer Sprung). Die Latenz bleibt unveraendert gut
(p99 ~0.13 ms bei 120k). Der naechste echte Hebel bleibt damit Stufe 2/3 (Pipelining /
Event-Loop-IO), nicht der Messaufbau.

> **Ehrlicher Hinweis zur Belastbarkeit.** Loopback + zwei Prozesse auf **einer** Maschine ist
> besser als In-JVM (getrennte Scheduler, getrennte Heaps/GC), teilt sich aber weiterhin die CPU
> derselben Maschine — der Lastgenerator konkurriert also nach wie vor um Kerne. Fuer wirklich
> belastbare **Absolutzahlen** sollte der Lastgenerator auf einer **zweiten Maschine** ueber das
> Netz laufen (statt Loopback). Bewusst **kein Docker**: auf macOS laeuft Docker in einer Linux-VM
> mit virtualisierter NIC, was Latenz und Durchsatz verfaelschen wuerde. Die hier gemessenen Zahlen
> sind daher als "besser als In-JVM, aber noch nicht netzwerk-isoliert" einzuordnen — die
> qualitative Aussage (Durchsatz skaliert nicht mit Publishern -> echter Server-Deckel) ist davon
> unberuehrt.
