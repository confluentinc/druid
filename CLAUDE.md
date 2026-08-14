# Druid (Confluent fork) — Kafka ingestion & query path map

Branch: `34.0.0-confluent` (Apache Druid 34.0.0 + Confluent patches).

This is an index. Each module on the two paths below carries **two** docs:

- **`CLAUDE.md`** — the class map: per-responsibility class listings and step-by-step call chains.
- **`Code-deepdive-Claude.MD`** — the runtime: happy-path traces at method-body granularity (thread
  ownership, state mutated, return values), nuances, a failure-mode table with the real catch/retry/timeout
  code, the optimizations present and what each one costs, the concurrency/lock model, and a config-knob
  table mapping each property to the branch it changes.

Every `path/to/File.java:LINE` reference in all twelve files is machine-verified to exist and be in range.

| Module | Class map | Deep dive | What it owns on these paths |
|---|---|---|---|
| `druid-indexing-service` | [`CLAUDE.md`](indexing-service/CLAUDE.md) | [deep dive](indexing-service/Code-deepdive-Claude.MD) | Overlord task lifecycle, locking, task actions; the generic `seekablestream` ingestion engine + supervisor framework; the transactional publish actions |
| `druid-kafka-indexing-service` | [`CLAUDE.md`](extensions-core/kafka-indexing-service/CLAUDE.md) | [deep dive](extensions-core/kafka-indexing-service/Code-deepdive-Claude.MD) | `KafkaSupervisor`, `index_kafka` task/runner, consumer plumbing, offset state, Kafka input format, **Confluent header-based filtering** |
| `druid-processing` | [`CLAUDE.md`](processing/CLAUDE.md) | [deep dive](processing/Code-deepdive-Claude.MD) | Segment format (build + read), `IncrementalIndex`, `IndexMergerV9`/`IndexIO`, `DataSegment`, `VersionedIntervalTimeline`, the query execution engine |
| `druid-server` | [`CLAUDE.md`](server/CLAUDE.md) | [deep dive](server/Code-deepdive-Claude.MD) | Appenderator + deep-storage push, **handoff**, metadata-store transactions, Coordinator load loop, Historical load/announce/serve, Broker view + fan-out |
| `druid-s3-extensions` | [`CLAUDE.md`](extensions-core/s3-extensions/CLAUDE.md) | [deep dive](extensions-core/s3-extensions/Code-deepdive-Claude.MD) | The `→ S3 →` hop both ways: `S3DataSegmentPusher` (push), `S3LoadSpec`/`S3DataSegmentPuller` (pull) |
| `druid-services` | [`CLAUDE.md`](services/CLAUDE.md) | [deep dive](services/Code-deepdive-Claude.MD) | Process entry points (`CliOverlord`/`CliIndexer`/`CliPeon`/`CliCoordinator`/`CliHistorical`/`CliBroker`/`CliRouter`) and the **Router** implementation |

## Ingestion path, end to end

```
Kafka topic
  │   KafkaSupervisor (kafka ext, on Overlord) creates/monitors index_kafka tasks
  ▼
index_kafka task in a Peon JVM (services: CliPeon; runner in indexing-service)
  │   KafkaRecordSupplier.poll()            [kafka ext]  ← Confluent header filter applied here
  │   StreamChunkParser → InputRow          [indexing-service]
  │   OnheapIncrementalIndex.add()          [processing]   in-memory rows
  │   IndexMergerV9.persist()/merge()       [processing]   local disk segment files
  │   StreamAppenderator.push()             [server]
  │   S3DataSegmentPusher.push()            [s3-extensions] → SEGMENT BYTES IN S3
  ▼
SegmentTransactionalInsertAction / ...AppendAction      [indexing-service]
  → IndexerSQLMetadataStorageCoordinator                [server]
    ONE metadata transaction: segment rows + KafkaDataSourceMetadata (offsets)
  ▼   ◄── ***OFFSETS ARE COMMITTED HERE*** (see ordering note below)
Coordinator load loop (asynchronous)                    [server]
  DruidCoordinator → RunRules → LoadRule → StrategicSegmentAssigner
  → SegmentLoadQueueManager → HttpLoadQueuePeon --HTTP--> Historical
  ▼
Historical                                              [server]
  SegmentLoadDropHandler.addSegment → SegmentLocalCacheManager (pulls from S3
  via S3LoadSpec/S3DataSegmentPuller) → IndexIO.loadIndex [processing]
  → SegmentManager (local VersionedIntervalTimeline) → BatchDataSegmentAnnouncer
  ▼
Task's handoff wait is satisfied                        [server + indexing-service]
  CoordinatorBasedSegmentHandoffNotifier polls
  GET /druid/coordinator/v1/datasources/{ds}/handoffComplete (DataSourcesResource)
  → Appenderator.drop() local copy → TaskStatus.success
  ▼
Broker learns the placement                             [server]
  HttpServerInventoryView polls GET /druid-internal/v1/segments on each Historical
  → BrokerServerView.serverAddedSegment → Broker timeline updated
```

### ⚠️ Ordering correction

The pipeline is often described as *"segment is loaded onto the Historicals, after which the offset is
committed."* **That is inverted.** Verified in
`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:1013`
(`publishAndRegisterHandoff`):

1. `driver.publish(...)` runs first (`:1018`) — this is the atomic segment-rows + offsets metadata
   transaction. **Offsets are durable at this point.**
2. `driver.registerHandoff(...)` is chained only onto that future's **success callback** (`:1070`).
3. `runInternal` then blocks on `handOffWaitList` (`:878`–`:901`) before returning success.

So handoff gates **when the task may exit**, not offset durability. Consequences worth knowing:

- A segment can be in S3 and marked `used` in the metadata store, with offsets committed, while it is
  still not queryable anywhere.
- If no `LoadRule` matches a segment's interval (or the matching rule has zero replicas),
  `DataSourcesResource.isHandOffComplete` returns `true` immediately — the task "hands off" successfully
  without the segment ever reaching a Historical.
- Committed Kafka offsets therefore never imply queryability. Use the Coordinator/Broker views for that.

## Query path, end to end

Realtime queries are assumed disabled, so nothing is routed to the Indexer.

```
client → Router      [services]  AsyncQueryForwardingServlet → QueryHostFinder
                                 → TieredBrokerHostSelector (tier selection)
       → Broker      [server]    QueryResource → QueryLifecycle → CachingClusteredClient
                                 timeline lookup (BrokerServerView) → per-Historical subqueries
                                 → DirectDruidClient
       → Historical  [server]    QueryResource → ServerManager.getQueryRunnerForSegments
                                 → ReferenceCountedSegmentProvider.acquireReference()
                     [processing] QueryRunner decorator stack → per-type engine
                                 (Timeseries/GroupBy/Scan/TopN) → CursorFactory/CursorHolder
                                 → filter via Filter.getBitmapColumnIndex → merged results
       ← Broker merges per-Historical results (QueryToolChest) ← Router proxies response back
```

## Reading order for a newcomer

1. `extensions-core/kafka-indexing-service/CLAUDE.md` — what an `index_kafka` task actually is.
2. `indexing-service/CLAUDE.md` — the engine underneath it and the exactly-once publish transaction.
3. `server/CLAUDE.md` — handoff, the Coordinator load loop, and Broker placement propagation (the
   longest doc; the handoff and Coordinator sections are the ones that matter here).
4. `processing/CLAUDE.md` — only if you need segment internals or query execution detail.
5. `services/CLAUDE.md` / `extensions-core/s3-extensions/CLAUDE.md` — process wiring and the S3 hop.

## Confluent fork deltas on these paths

The most significant fork-specific feature on the ingestion path is **Kafka header-based record
filtering**, applied in `KafkaRecordSupplier.poll()` before any row parsing: a supervisor-configured
header name + allowed value set (`KafkaHeaderBasedFilterConfig`, `InDimFilterHandler`,
`KafkaHeaderBasedFilterEvaluator`) drops non-matching records while still advancing and committing their
offsets. It **fails open** — a missing or undecodable header includes the record — so filtering can never
stall ingestion. Details in `extensions-core/kafka-indexing-service/CLAUDE.md`; the fork also touches
worker-selection strategy and lookup-enrichment error handling in `indexing-service/` (see that doc's
"Confluent fork deltas" section).
