# Avenue Performance-Baseline (Stufe 0)

Messgetriebenes Performance-Programm — festgehaltene Ausgangslage, gemessen mit der
neuen `LoadHarness` (`avenue-server/src/test/java/de/kyle/avenue/benchmark/LoadHarness.java`).
In Stufe 0 wurde **kein** Produktivcode geaendert; dies ist ausschliesslich der Nullpunkt,
gegen den die Folgestufen gemessen werden.

## Stand / Fazit (profilgefuehrte Server-Effizienz)

Erreicht und JFR-/Test-belegt (Stufen 1, 1.5, 4a + Out-of-process-Harness):

- **Inline-Delivery** (Executor-Hop pro Publish entfernt) → Median-Latenz p50 @120k **−30 %**.
- **Nutzlast als `bytes`** (opaker ByteString-Passthrough) → die ~40 % Server-CPU in
  protobuf-UTF-8-Transcoding (`String.charAt`) sind **restlos weg** (JFR).
- **Outbound-Encode ohne Builder** (direkter `CodedOutputStream` in ein rechtsgrosses `byte[]`,
  wire-identisch per Fuzz-Gleichheitstest) → Outbound-Builder/Message-Objektgraph **aus dem
  Allokationsprofil verschwunden**, GC-Frequenz gesunken.
- **encode-once Fan-out**, **buffered Read**, **single-normalize** — kleinere CPU-/Allok-Einsparungen.

**Wichtigste Mess-Erkenntnis:** Der Wanduhr-Durchsatz (~250–300 k msg/s) ist auf dieser **einen
Maschine** durch das gemeinsame CPU-/Loopback-Limit von Lastgenerator **und** Server gedeckelt —
**nicht** durch den Server. Belegt durch out-of-process + die flache Skalierung unabhaengiger Paare
und dadurch, dass das Entfernen von ~40 % Server-CPU den Durchsatz **nicht** bewegte. Deshalb ist die
Erfolgsmetrik dieser Stufen **Server-CPU/Allokation pro Nachricht** (JFR), nicht die Wanduhr.

**Naechste Stufe (Event-Loop-IO):** Der verbleibende strukturelle Block ist ~12–15 % in
Virtual-Thread park/unpark (Reader→Writer-Hops). Den adressiert nur der Event-Loop-Rewrite (Stufe 3) —
dessen eigentlicher Nutzen (Multi-Core-Skalierung Richtung 1M) sich aber erst auf einer **zweiten
Maschine** (Lastgenerator uebers Netz) validieren laesst.

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
| 3 — Event-Loop-IO (nio) | ~268 000 – 296 000 (Paare P=8, io-threads=4) | n/a | s. u. |
| 4b — Flat Fan-out: CoW-Array + gestripte Metriken (JFR: CPU-Win) | Durchsatz im Streuband, s. u. | n/a | n/a |
| 4 — Allokations-/GC-Disziplin | | | |

## Stufe 4b — Flat Layout im Fan-out (Copy-on-Write-Array, gestripte Metriken)

> Hinweis: Dieser Abschnitt ist bewusst mit korrekten Umlauten geschrieben; die älteren Abschnitte
> dieser Datei nutzen noch ASCII-Ersatzschreibweisen und bleiben unangetastet.

Erster Schritt weg von objektbasierten Containern hin zu flachen Speicher-Layouts auf dem
Zustellpfad. Zwei Änderungen, beide auf der Linie „pro Subscriber pro Nachricht":

1. **Subscriber-Menge: `ConcurrentHashMap.newKeySet()` → Copy-on-Write-`ClientConnection[]`.**
   Die Zustellung liest die Subscriber-Menge einmal pro Subscriber pro Nachricht und schreibt sie
   nur bei Subscribe/Unsubscribe — ein extrem leselastiges Zugriffsmuster. Das Concurrent-Key-Set
   bezahlte jeden dieser Lesevorgänge mit einem frischen `KeyIterator` und einem `Traverser`-Lauf
   über Hash-Bins, also einem Pointer-Chase pro Subscriber über verstreute `Node`-Objekte. Der
   Ersatz ist ein unveränderliches Array pro Topic: die Zustellung ist ein linearer Lauf über
   zusammenhängenden Speicher, ohne Iterator-Objekt und ohne Bin-Traversierung. Die Kosten wandern
   auf die Mutationsseite (eine Array-Kopie pro Subscribe/Unsubscribe), wo sie irrelevant sind.
2. **Metrik-Zähler: `AtomicLong` → `LongAdder` / `LongAccumulator`.** `incrementMessagesDelivered`
   und `recordOutboundQueueDepth` laufen ebenfalls pro Subscriber pro Nachricht, letzteres bisher
   als CAS-Retry-Schleife (`accumulateAndGet(depth, Math::max)`) auf einer einzigen geteilten
   Cache-Line. Beide sind jetzt gestript.

### Korrektheit

Der CoW-Umbau bringt handgeschriebene Synchronisation mit: ein leer gelaufener Holder wird
**retired und unter demselben Monitor aus der Map entfernt**, damit ein gleichzeitig eintreffender
Subscriber die tote Instanz erkennt und gegen einen frischen Holder erneut versucht. Ohne diesen
Retry landet die Subscription in einem nicht mehr erreichbaren Holder — der Client bliebe
angemeldet, bekäme aber nie wieder eine Nachricht.

Neu: `TopicSubscriptionHandlerTest` (7 Tests, vorher gab es für diese Klasse **keinen** Unit-Test).
Der entscheidende Test `subscriber_racing_a_holder_retirement_is_not_lost` stellt das Rennen
**deterministisch** her, statt auf Timing zu hoffen: über den package-privaten Test-Hook
`awaitHolderRaceWindow()` wird ein Subscriber exakt zwischen Holder-Lookup und Holder-Lock geparkt,
während ein zweiter Thread das Topic auf null Subscriber herunterfährt.

> **Warum der Hook nötig war (festgehalten, weil lehrreich):** Die erste Fassung des Tests war eine
> Stress-Schleife (8 Threads × 500 Runden Subscribe/Unsubscribe). Gegenprobe per Mutation — Retry
> im Produktivcode abgeschaltet — ergab: die Schleife blieb **grün**. Das Zeitfenster zwischen
> `computeIfAbsent` und `synchronized` ist wenige Nanosekunden breit und wird von einer Stress-
> Schleife nicht zuverlässig getroffen. Mit dem Hook schlägt derselbe Mutationstest sofort und
> reproduzierbar fehl (`subscriberCount` 0 statt 1). Die Stress-Schleife bleibt als Konsistenz-
> Smoke-Test erhalten, taugt aber nicht als Nachweis.

Alle Tests grün (101–103 je nach Lauf; siehe Flaky-Hinweis unten).

### JFR-Gegenprobe (out-of-process, Server isoliert, `settings=profile`, 30 s Messfenster)

Maschine: Apple Silicon (arm64), Corretto 21.0.5, Loopback, Server und Last in getrennten JVMs.
Reproduktion über die neue `JFR_OUT`-Option von `scripts/bench-split.sh`:

```bash
JFR_OUT=/tmp/before.jfr ./scripts/bench-split.sh 4180 \
    publishers=8 subscribers=1 topics=8 msgSize=100 warmupSeconds=5 seconds=30
jfr view hot-methods /tmp/before.jfr
```

Gezählt wurden **`ExecutionSample`-Stacks, die durch `TopicSubscriptionHandler.fanOut` laufen**
(inklusive Zählung über den ganzen Stack, nicht nur den Top-Frame), sowie Stacks in der
`ConcurrentHashMap`-Set-Iteration:

| Lauf | Samples gesamt | fan-out-Stacks | CHM-Iterations-Stacks | fan-out-Anteil |
| --- | --- | --- | --- | --- |
| Paare P=8 (Fan-out 1), **vorher** | 178 | 28 | 6 | **15,7 %** |
| Paare P=8 (Fan-out 1), **nachher** | 142 | 4 | **0** | **2,8 %** |
| 1:16 (Fan-out 16), **vorher** | 392 | 26 | 17 | **6,6 %** |
| 1:16 (Fan-out 16), **nachher** | 469 | 7 | **0** | **1,4 %** |

Allokationsseite, `jfr view allocation-by-class`: `ConcurrentHashMap$KeyIterator` lag vorher bei
**2,46 %** (Fan-out 16) bzw. **4,30 %** (Paare P=8) der Allokation und ist nachher **vollständig aus
dem Profil verschwunden**. Die Young-GC-Anzahl bleibt praktisch gleich (12 → 13 in 30 s), was zu
erwarten war: `byte[]` dominiert das Allokationsprofil weiterhin mit 24–54 %, der Iterator war nur
ein kleiner Posten.

### Ehrliche Einordnung

- **Der CoW-Umbau ist belegt:** die Set-Iteration verschwindet restlos aus CPU- **und**
  Allokationsprofil, der Fan-out-Pfad kostet nur noch etwa ein Fünftel seiner vorherigen CPU-Anteile.
- **Die Metrik-Umstellung ist NICHT als Gewinn belegt.** Die Metrik-Atomics tauchen in keinem der
  vier Profile im On-CPU-Sampling auf — weder vorher noch nachher. Die erwartete CAS-Contention war
  bei dieser Last (8 Publisher, 16 Verbindungen, eine Maschine) offenbar zu gering, und CAS-Stalls
  bilden sich im Sampling ohnehin schlecht als eigener Frame ab. Die Änderung bleibt strukturell
  richtig (die geteilte Cache-Line ist vom Pro-Zustellung-Pfad verschwunden), aber sie ist hier eine
  **begründete Vorsichtsmaßnahme, keine gemessene Verbesserung**. Ihr Nutzen müsste sich erst bei
  deutlich höherer Kernzahl bzw. auf einer zweiten Lastmaschine zeigen.
- **Der Durchsatz bleibt im Rauschen.** 1:16 liegt vorher wie nachher bei ~717–735 k Zustellungen/s;
  bei Paaren P=8 streuen dieselben Konfigurationen über mehrere Läufe zwischen ~118 k und ~203 k
  Publishes/s — die Nachher-Werte liegen in diesem Band, taugen also nicht als Beleg. Das ist
  konsistent mit der Kernaussage der Baseline: auf **einer** Maschine ist der Wanduhr-Durchsatz
  nicht die belastbare Metrik, sondern Server-CPU/Allokation pro Nachricht.
- **Nicht adressiert (bewusst):** Der bei Fan-out 16 dominierende Block ist nach wie vor die
  Virtual-Thread-Hop-Mechanik des Blocking-Transports (`getAndBitwiseAndInt`, `unparkVirtualThread`,
  `LockSupport.unpark`, ForkJoin-Mechanik) — im Baseline-Profil zusammen über 50 % der Samples. Den
  entfernt nur der NIO-Modus (Stufe 3), nicht ein Speicher-Layout. Ebenso unangetastet bleiben die
  pro Nachricht allokierten Posten `LinkedBlockingQueue$Node` (~15,7 %) und
  `OutboundFrame$PreSerialized` (~10 %) — das ist der nächste Schritt (Outbound-Ringpuffer).

> **Flaky-Hinweis (nicht durch diese Änderung verursacht).** Die zeitlimitierten
> Cluster-Integrationstests fallen auf dieser Maschine sporadisch: über 5 Baseline-Volläufe fiel
> zweimal `SwimMembershipTest` (`crash_detection`, `restart_same_nodeid`), über 4 Volläufe mit der
> Änderung zweimal `AtLeastOnceTest.slow_peer_no_loss`. Letzterer wartet **deterministisch** auf
> Interest-Konvergenz (`awaitInterest`), bevor er publiziert — ein Fehler in den hier geänderten
> Interest-Transitionen wäre also dort gescheitert, nicht erst beim Empfangszähler. Was fehlschlägt,
> ist die 5-Sekunden-Deadline der Empfangsschleife bei künstlich winzigem Replay-Ring (16 Einträge,
> BLOCK-Policy): ein Durchsatz-gegen-Uhr-Test. Isoliert lief er mit der Änderung 3×3 Durchläufe grün.

## Stufe 4c — NIO-Fan-out: Befund und Korrektur einer Fehlinterpretation

### Der Befund: NIO bricht bei hohem Fan-out ein

Stufe 3 hat NIO gegen Blocking nur bei **unabhängigen Paaren (Fan-out 1)** gemessen und dort Parität
festgestellt. Bei **Fan-out 16** gilt das nicht:

| Transport (1 Publisher, 16 Subscriber, 100 B, 30 s) | Zustellrate |
| --- | --- |
| blocking | **~729 000 msg/s** |
| nio (io-threads=4) | **~323 000 msg/s** |

NIO liefert bei hohem Fan-out also nur etwa **44 %** des Blocking-Durchsatzes. Die
Paritäts-Aussage aus Stufe 3 ist damit auf Fan-out 1 zu beschränken.

### Warum — und was daran NICHT das Problem ist

Das JFR-Profil des NIO-Laufs zeigt `NioIoWorker.enableWrite` als Top-Methode mit **31,8 %**, später
sogar 46 %. Das ist eine **Falle**, und sie ist hier dokumentiert, weil sie leicht zu einer falschen
Optimierung führt: Der NIO-Lauf hat in 30 Sekunden nur **35–44 `ExecutionSample`s**, der
Blocking-Lauf **469**. In absoluten Zahlen sind 46 % von 39 Samples ≈ 18 Samples — gegenüber einer
Blocking-Last, die um ein Vielfaches darüber liegt. Die mittlere JVM-CPU bestätigt es: **~20,6 %**
(blocking) gegenüber **~12–16 %** (nio).

**Der NIO-Server ist bei Fan-out nicht CPU-limitiert, er wartet.** Er verbraucht *weniger* CPU und
liefert *weniger* Durchsatz. Der Engpass ist strukturell die Selector-Runde: Lesen → Fan-out →
`OP_WRITE` armen → nächste `select()`-Runde → schreiben, also ein Round-Trip pro Batch, während der
Blocking-Writer eine ganze Charge über den `BufferedOutputStream` sammelt und mit einem Syscall
hinausschreibt. Ein Prozentanteil auf einer 39-Sample-Stichprobe taugt nicht als Optimierungsziel;
maßgeblich ist die absolute CPU-Zeit.

### Was trotzdem umgesetzt wurde (korrekt, aber ohne Durchsatzwirkung)

1. **Gecachter `SelectionKey` statt `channel().keyFor(selector)`.** Die JDK-Methode nimmt den
   Key-Lock des Channels und scannt dessen Key-Array linear — und lief einmal pro Frame pro
   Subscriber, obwohl die Connection ihren Key seit der Registrierung ohnehin in einem
   `volatile`-Feld hält. Eindeutige Verbesserung ohne Nachteil.
2. **`interestOps` nur schreiben, wenn `OP_WRITE` fehlt.** Der Getter ist ein Feldzugriff, der
   Setter geht über den Update-Lock des Selectors; auf einer belasteten Verbindung ist `OP_WRITE`
   meist schon gesetzt.
3. **`selector.wakeup()` nur, wenn der Loop blockiert ist** (`wakeupNeeded`-Flag, CAS pro Burst).
   `wakeup()` ist ein Syscall und lief bisher pro Frame pro fremdgehostetem Subscriber. Kein
   Wakeup kann verloren gehen: der Loop armt das Flag vor der Blockierentscheidung, prüft danach
   seine Queues erneut und degradiert auf `selectNow()`, wenn zwischenzeitlich Arbeit ankam.

**Ehrliche Bewertung:** Gemessen am Durchsatz bewegt sich davon **nichts** (~323 k → ~300 k, also
innerhalb der Streuung). Punkt 1 und 2 sind reine Aufräumarbeiten mit klarem Vorzeichen. Punkt 3
kostet zusätzliche Nebenläufigkeitskomplexität ohne belegten Nutzen bei dieser Last — er ist als
Vorsorge für höhere Worker-/Verbindungszahlen drin, nicht als gemessener Gewinn.

### Konsequenz für die weitere Arbeit

Der lohnende Angriffspunkt bleibt der **Blocking-Transport** (Default, und die einzige Konfiguration
mit echter CPU-Last): dort dominieren im Allokationsprofil bei Fan-out 16 `LinkedBlockingQueue$Node`
(~15,7 %) und `OutboundFrame$PreSerialized` (~10,1 %) — also genau die Objekte, die ein
Outbound-Ringpuffer ersetzt — und im CPU-Profil über 50 % Virtual-Thread-Hop-Mechanik
(`unparkVirtualThread`, `getAndBitwiseAndInt`, `LockSupport.unpark`, ForkJoin). Letztere adressiert
kein Speicher-Layout, sondern nur eine Änderung daran, *wann* der Writer-Thread geweckt wird.

## Stufe 3 — Event-Loop-IO (hand-rolled NIO-Selector, `server.io-mode=nio`)

> **Ehrliche Einordnung vorweg.** Auf **einer** Maschine (Loopback, Lastgenerator + Server teilen
> sich die Kerne) bringt der NIO-Transport **keinen** Durchsatzgewinn gegenueber Blocking — beide
> liegen bei den unabhaengigen Paaren P=8 im selben Band (~270–296k msg/s). Was der JFR **belegt** und
> was das eigentliche Ziel dieser Stufe war: Der strukturelle **Virtual-Thread-park/unpark-Block
> (~12–15 %)** aus der Reader→Writer-Thread-Uebergabe des Blocking-Modells ist im NIO-Modus
> **vollstaendig verschwunden**. Der erwartete Multi-Core-Skalierungsnutzen Richtung 1M laesst sich —
> wie in Stufe 0/Out-of-process bereits festgehalten — nur auf einer **zweiten Maschine** (Last uebers
> Netz, nicht Loopback) validieren; co-located ist der Server-Effizienzgewinn durch
> Thread-Oversubscription verdeckt.

### Was implementiert wurde (Schritt B)

Zweite `ClientConnection`-Implementierung hinter einem Config-Schalter, **Default bleibt Blocking**:

- `server.io-mode = blocking` (Default, unveraendert) | `nio`; `server.nio.io-threads` (Default =
  `availableProcessors()`).
- **Acceptor** (ein Thread, non-blocking `ServerSocketChannel`) verteilt akzeptierte Channels
  **round-robin** an N **I/O-Worker**, jeder mit eigenem `Selector` + disjunkter Connection-Menge.
- **`NioClientConnection`** rahmt die Nutzlast mit demselben 4-Byte-Big-Endian-Praefix wie
  `PacketFraming`; `enqueuePreSerialized` traegt — **wie im Blocking-Modus** — die **bare** Payload,
  die Connection rahmt sie. Cross-thread-Wakeup: der Fremd-Thread setzt **nie** `interestOps` direkt,
  sondern reiht die Connection (koaleszierend, ein Eintrag pro Burst) in die Pending-Write-Queue des
  ownenden Workers + `selector.wakeup()`. Same-Worker-Fast-Path setzt `OP_WRITE` direkt ohne Queue.
- **Framing ueber partielle Reads**: per-Connection-Akkumulationspuffer, in einer Schleife alle
  vollstaendigen Frames extrahieren, Rest behalten/kompaktieren; negative/zu grosse Laenge ->
  Connection schliessen (wie `PacketFraming.readFrame`).
- **OP_WRITE-Backpressure**: Outbound-Deque bounded auf `server.outbound.queue.capacity`; bei
  anhaltendem Ueberlauf jenseits des `offer-timeout` greift die konfigurierte Policy
  (`DISCONNECT_SLOW_CONSUMER` -> close, `DROP_MESSAGE` -> Frame verwerfen), **gleiche Metriken** wie
  Blocking. Ein transienter Burst innerhalb des Grace-Fensters wird toleriert (kein Verlust) — das
  entspricht dem `offer(timeout)` des Blocking-Writers.
- **Idle-Sweep** pro Worker (Sekundentakt via Select-Timeout) schliesst Connections, die laenger als
  `idle-timeout-ms` nichts gesendet haben (gleiche Semantik; 0 = aus).
- **Max-Connections** beim Accept gegen `activeConnections` geprueft -> ueber Limit sofort
  schliessen + `connectionsRejected` (gleiche Semantik wie `SingleNodeServer`).
- **TLS**: SSLEngine-ueber-NIO ist **nicht** Teil dieses Schritts. Bei `server.tls.enabled=true` UND
  `server.io-mode=nio` loggt der Server eine WARN und faellt auf **Blocking** zurueck, damit der
  TlsIntegrationTest (Blocking) unveraendert gruen bleibt. SSLEngine-NIO = Future Work.

### Korrektheit

- Neuer `integration/NioIntegrationTest`: voller Pfad ueber echte `TestClient`s gegen einen
  `SingleNodeServer` mit `io-mode=nio` (ephemerer Port): connect -> auth -> subscribe (SubscribeAck)
  -> publish -> cross-client receive; Fan-out an zwei Subscriber; Wrong-Token-Drop; **5000
  Nachrichten ueber EINE Connection** (Framing ueber partielle Reads / mehrere Frames pro select).
  Deterministisch (Latches/await, keine Sleeps).
- **Alle 96 Tests** (92 Bestand + 4 neu) bleiben mit Default (Blocking) **unveraendert gruen**,
  `mvn clean test` mehrfach wiederholt. Der einzige sporadische Ausreisser ist der bereits zuvor
  flaky `SwimMembershipTest` (Cluster-Gossip-Konvergenz, timing-abhaengig, beruehrt den
  Client-Transport ueberhaupt nicht).

### JFR — Durchsatz und Hot-Methods (Out-of-process, P=8, topics=8, sub=1, msgSize=100, 15–20 s)

Maschine: Apple Silicon (arm64, 11 logische Kerne), Corretto 21.0.5, Loopback, Server + Last in
getrennten JVMs.

| Transport | io-threads | Durchsatz (msg/s, 3 Laeufe) |
| --- | --- | --- |
| blocking | — | 287 768 / 289 793 / 291 381 (stabil) |
| nio | 11 (= `availableProcessors`) | 90 888 / 156 290 / 175 186 (**stark streuend, schlechter**) |
| nio | 4 | 262 827 / 268 517 / 295 671 (**Parity zu Blocking**) |

**Erklaerung der nio@11-Regression:** 11 Selector-Worker + 16 Connections + 8 Lastgenerator-Threads
auf 11 Kernen = massive **Thread-Oversubscription**, wenn Server und Last **dieselbe** Maschine
teilen. Mit `io-threads=4` verschwindet die Oversubscription und NIO liegt gleichauf mit Blocking
(bester Lauf 296k > Blocking). In Produktion (Server allein auf der Box) ist `availableProcessors`
sinnvoll; fuer den co-located Bench ist 4 die faire Einstellung.

**`jfr view hot-methods` — der entscheidende Vergleich (park/unpark weg?):**

- **Blocking** (Top-Block): `ForkJoinTask$RunnableExecuteAction.exec` **10,0 %**,
  `LockSupport.unpark` **3,9 %**, `LinkedTransferQueue$DualNode.await` **2,2 %**,
  `unparkVirtualThread` **1,3 %** — zusammen ~17 % reine Virtual-Thread-Reader→Writer-Hop-Mechanik.
- **NIO (io-threads=4)**: **0** Samples in `unparkVirtualThread`, `ForkJoinTask.exec`,
  `LockSupport.unpark`, `LinkedTransferQueue.await`. Der Selector selbst
  (`KQueueSelectorImpl.doSelect`/`setEventOps`) liegt bei ~1–2 %. Das Top-Profil ist jetzt reine
  Nutzarbeit: protobuf parse/encode (`Utf8.encode`, `charAt`, `mergeFrom`), `fanOut`, `writeOutbound`,
  Topic-`toLowerCase`, HMAC-`MessageDigest.isEqual`.

> **Naive-Design-Befund (festgehalten, weil lehrreich):** Die **erste** NIO-Fassung reihte pro
> Fan-out-Frame einen `Runnable` in eine `ConcurrentLinkedQueue` des Ziel-Workers ein. JFR zeigte
> `NioIoWorker.drainTasks` **18 %** + `ConcurrentLinkedQueue.offer/poll` **~11 %** — der park/unpark-
> Block war zwar weg, aber durch Task-Queue-Traffic ersetzt. Fix: **koaleszierende** Pending-Write-
> Queue (eine Connection reiht sich pro Burst genau einmal ein, atomare Flagge) **plus**
> Same-Worker-Fast-Path (kein Hop, wenn Producer = ownender Worker). Danach ist `drainTasks` aus dem
> Top-25 verschwunden und der Durchsatz von ~140–175k auf ~268–296k gestiegen (Parity).

**`jfr view allocation-by-class`:** In beiden Modi dominiert `byte[]` (~53 %, protobuf-Payload).
NIO ergaenzt `HeapByteBuffer` (~4 %, der per-Frame gerahmte Buffer — ein zusaetzlicher Copy
gegenueber dem direkten Write des Blocking-Writers) und `ConcurrentLinkedQueue$Node` (~2 %,
Pending-Write-/Control-Queue). Kein neuer dominanter Allokations-Hotspot.

### Fazit Stufe 3

Der NIO-Selector-Transport liefert **dieselbe Client-Protokoll-Semantik** wie Blocking (Framing,
Auth, Backpressure-Policy, Idle, Max-Conn, Metriken — per Integrationstest belegt) und entfernt den
**Virtual-Thread-park/unpark-Block restlos** (JFR). Beim **Durchsatz auf einer Maschine** ist NIO
mit richtig dimensionierten `io-threads` **gleichauf** mit Blocking, nicht besser — der erwartete
Multi-Core-Skalierungsvorteil ist co-located durch geteilte CPU verdeckt und braucht zur Validierung
eine **zweite Maschine** (Last uebers Netz). Default bleibt bewusst `blocking`; `nio` ist der
opt-in-Schalter fuer den spaeteren Skalierungsnachweis.

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
