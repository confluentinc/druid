# druid-server

`server/` (artifactId `druid-server`) is the largest module in Druid and hosts most of the process-level
logic for the **Coordinator**, **Historical**, **Broker**, and **Router** processes, plus the task-side
**Appenderator** machinery used by ingestion tasks (`org.apache.druid.segment.realtime.*`,
`org.apache.druid.segment.handoff.*`) and the SQL-backed metadata store implementation
(`org.apache.druid.metadata.*`). It does NOT contain query engines/segment format code (`processing/`),
task/supervisor orchestration (`indexing-service/`), Kafka-specific ingestion (`extensions-core/kafka-indexing-service/`),
or deep-storage backends (`extensions-core/s3-extensions/`).

> **Execution mechanics live in [`Code-deepdive-Claude.MD`](Code-deepdive-Claude.MD)** —
> appenderator persist/push, the exactly-once metadata transaction, the Coordinator load loop and Broker view sync, traced at method-body granularity with thread ownership, failure modes and config knobs.
> This file is the class map; that one is the runtime behaviour.

**Scope of this doc**: this file documents exactly two paths through `server/`, per the pipeline below, and
nothing else in the module (compaction, lookups, SQL planning in `sql/`, Avatica, security, etc. are out of scope):

1. **Ingestion**: Indexer `index_kafka` task → Appenderator builds/persists/pushes segments to S3 →
   **segments + Kafka offsets committed to the metadata store in one transaction** → Coordinator loads the
   segments onto Historicals → task's handoff-wait is satisfied and the task exits → placement propagates
   to the Broker.
2. **Query** (realtime queries disabled): Router → Broker → Historicals.

> **Note on ordering:** offsets are committed at *publish* time, **before** any Historical loads the segment
> (see `IndexerSQLMetadataStorageCoordinator.commitSegmentsAndMetadata` below). Handoff gates only when the
> *task* can report success — it is not what makes offsets durable. `indexing-service/CLAUDE.md` documents the
> task-side proof of this ordering.

All class references below were verified by reading the source in this checkout (branch `34.0.0-confluent`,
Druid 34.0.0). Anything not directly confirmed is marked `(unverified)`.

## Two paths at a glance

```
INGESTION PATH (index_kafka task, running inside a Middle Manager/Indexer peon, task code lives partly here)
=================================================================================================

 KafkaIndexTask (extensions-core/kafka-indexing-service, not in this module)
        │  drives a SeekableStreamIndexTaskRunner (indexing-service)
        ▼
 StreamAppenderator.add()  ──────────────────────────────────────────────────────────┐
   (segment/realtime/appenderator/StreamAppenderator.java)                           │ in-memory Sink/FireHydrant
        │ persistAll() -> per-Hydrant on-disk segments                               │ (segment/realtime/sink/Sink.java,
        ▼                                                                            │  segment/realtime/FireHydrant.java)
 StreamAppenderator.push() -> mergeAndPush() -> DataSegmentPusher.push()  ────────────┘
   (StreamAppenderator.java:773,874)      (processing/segment/loading/DataSegmentPusher.java,
                                            impl = S3DataSegmentPusher in extensions-core/s3-extensions)
        │  segment bytes land in DEEP STORAGE (S3)
        ▼
 StreamAppenderatorDriver.publish() -> TransactionalSegmentPublisher.publish()
   (segment/realtime/appenderator/StreamAppenderatorDriver.java:277)   (impl = ActionBasedSegmentPublisher,
                                                                         indexing-service, calls SegmentTransactionalInsertAction
                                                                         -> IndexerSQLMetadataStorageCoordinator.commitSegmentsAndMetadata /
                                                                            commitAppendSegmentsAndMetadata)
        │ metadata store now has the segment as "used" (but not yet loaded anywhere)
        ▼
 StreamAppenderatorDriver.registerHandoff() ── polls ──> CoordinatorBasedSegmentHandoffNotifier.checkForSegmentHandoffs()
   (StreamAppenderatorDriver.java:321)                    (segment/handoff/CoordinatorBasedSegmentHandoffNotifier.java:85)
                                                                  │ HTTP GET
                                                                  ▼
                                                 CoordinatorClient.isHandoffComplete()
                                                   (client/coordinator/CoordinatorClientImpl.java:72)
                                                                  │ GET /druid/coordinator/v1/datasources/{ds}/handoffComplete
                                                                  ▼
                                              DataSourcesResource.isHandOffComplete()
                                                (server/http/DataSourcesResource.java:909)
                                                (checks CoordinatorServerView, which is fed by Historicals'
                                                 own segment announcements, see Coordinator section)
        │ handoff confirmed (segment observed as served on a Historical)
        ▼
 Appenderator.drop() the in-memory/local copy -> task's handOffWaitList future completes -> TaskStatus.success
 (offsets were ALREADY committed at the publish step above, in the same txn as the segments)


  MEANWHILE, ASYNCHRONOUSLY: THE COORDINATOR LOAD LOOP
  =====================================================
  DruidCoordinator.DutiesRunnable.run() (server/coordinator/DruidCoordinator.java:712)
    -> PrepareBalancerAndLoadQueues -> RunRules.run() (server/coordinator/duty/RunRules.java:71)
       -> per used segment: matching Rule.run() e.g. LoadRule.run() (server/coordinator/rules/LoadRule.java:67)
          -> StrategicSegmentAssigner.replicateSegment() (server/coordinator/loading/StrategicSegmentAssigner.java:202)
             -> SegmentLoadQueueManager.loadSegment() (server/coordinator/loading/SegmentLoadQueueManager.java:52)
                -> LoadQueuePeon(HttpLoadQueuePeon).loadSegment() (server/coordinator/loading/HttpLoadQueuePeon.java:474)
                   -> HTTP POST to Historical's /druid-internal/v1/segments change-request endpoint
                      -> SegmentLoadDropHandler.addSegment() (server/coordination/SegmentLoadDropHandler.java:146)
                         -> SegmentManager.loadSegment() (server/SegmentManager.java:199) [pulls file from S3 via
                            SegmentCacheManager/SegmentLocalCacheManager into local disk cache]
                         -> DataSegmentAnnouncer.announceSegment() (BatchDataSegmentAnnouncer.java:146)
                            [announces via Curator/HTTP inventory so Coordinator + Broker learn the segment is served]


QUERY PATH (realtime queries disabled -> Historicals only serve pushed/handed-off segments)
=============================================================================================

 Router (services/ module: AsyncQueryForwardingServlet, QueryHostFinder, TieredBrokerHostSelector)
        │ HTTP proxy, chooses a Broker
        ▼
 Broker: QueryResource / QueryLifecycle (server/QueryResource.java, server/QueryLifecycle.java)
        ▼
 ClientQuerySegmentWalker -> CachingClusteredClient.getQueryRunnerForIntervals/-Segments()
   (client/CachingClusteredClient.java:181,220)
        │ looks up segment->server mapping in the timeline maintained by
        ▼
 BrokerServerView (client/BrokerServerView.java:62, implements TimelineServerView)
   fed by HttpServerInventoryView (client/HttpServerInventoryView.java:86) polling each Historical's
   GET /druid-internal/v1/segments via ChangeRequestHttpSyncer
        │ per segment, builds a QueryRunner against the owning Historical via
        ▼
 DirectDruidClient (client/DirectDruidClient.java:90) -- HTTP query fan-out to Historical
        ▼
 Historical: QueryResource/QueryLifecycle -> ServerManager.getQueryRunnerForSegments()
   (server/coordination/ServerManager.java:86,170) -- looks up local VersionedIntervalTimeline built by SegmentManager
        │
        ▼
 Results merged back up through CachingClusteredClient -> Broker -> Router -> client
```

## Key classes by responsibility

### Task-side segment creation & push (`org.apache.druid.segment.realtime.*`, `org.apache.druid.segment.handoff.*`)

- `Appenderator` — `server/src/main/java/org/apache/druid/segment/realtime/appenderator/Appenderator.java` — interface for
  add/persist/push/close/drop of in-flight segments for a task.
- `StreamAppenderator` — `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:117` — the realtime/streaming implementation used by
  Kafka indexing. Holds `dataSegmentPusher` (`:136`). Key methods: `persistAll()` (`:631`), `push()` (`:773`),
  `mergeAndPush()` (`:874`, calls `dataSegmentPusher.push()` at `:977`).
- `BatchAppenderator` — `.../appenderator/BatchAppenderator.java` — batch/native ingestion counterpart (not on the
  Kafka path but shares the `Appenderator` interface).
- `BaseAppenderatorDriver` — `server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:85` — shared driver logic: `pushInBackground()`
  (`:493`), `publishInBackground()` (`:584`, invokes the injected `TransactionalSegmentPublisher`), `dropInBackground()` (`:548`).
- `StreamAppenderatorDriver` — `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:78` — streaming driver, holds a
  `SegmentHandoffNotifier` (`:84`). Key methods: `persistAsync()` (`:261`), `publish()` (`:277`, push-then-publish),
  `registerHandoff()` (`:321`, registers a callback with the handoff notifier per segment and drops the local copy once
  fired), `publishAndRegisterHandoff()` (`:411`).
- `SegmentsAndCommitMetadata` — `.../appenderator/SegmentsAndCommitMetadata.java` — the value object threaded through
  push -> publish -> handoff, carrying the pushed `DataSegment`s and caller commit metadata (e.g. Kafka offsets).
- `TransactionalSegmentPublisher` — `.../appenderator/TransactionalSegmentPublisher.java` — interface the driver calls
  to atomically insert segments + commit metadata; concrete impl (`ActionBasedSegmentPublisher`) lives in
  `indexing-service` and issues a `SegmentTransactionalInsertAction` task action, which server-side resolves to
  `IndexerSQLMetadataStorageCoordinator`.
- `Sink` / `FireHydrant` — `segment/realtime/sink/Sink.java`, `segment/realtime/FireHydrant.java` — an in-memory Sink
  wraps a sequence of Hydrants (each hydrant a persisted or in-memory sub-segment) that get merged in `mergeAndPush()`.
- `AppenderatorsManager` (interface), `PeonAppenderatorsManager`, `UnifiedIndexerAppenderatorsManager` —
  `.../appenderator/*.java` — manage per-task or per-JVM Appenderator instances depending on task execution mode.

**Handoff (the exact hinge between "segment in S3" and "offsets committed")**:

- `SegmentHandoffNotifier` — `segment/handoff/SegmentHandoffNotifier.java` — `registerSegmentHandoffCallback(descriptor, executor, runnable)`.
- `SegmentHandoffNotifierFactory` — `segment/handoff/SegmentHandoffNotifierFactory.java`; `NoopSegmentHandoffNotifierFactory`
  for batch tasks that don't need handoff waiting.
- `CoordinatorBasedSegmentHandoffNotifierFactory` — `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifierFactory.java:25`
  — the realtime/Kafka-task factory; injects `CoordinatorClient` + `CoordinatorBasedSegmentHandoffNotifierConfig`.
- `CoordinatorBasedSegmentHandoffNotifier` — `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifier.java:38` — **this is
  the class the task uses to wait for handoff**. `start()` (`:74`) schedules `checkForSegmentHandoffs()` (`:85`) at a
  fixed rate (`CoordinatorBasedSegmentHandoffNotifierConfig.pollDuration`, default **`PT1S`** —
  `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifierConfig.java:29`).
  For every registered
  `SegmentDescriptor` it calls `coordinatorClient.isHandoffComplete(dataSource, descriptor)` (`:95`) and, on `true`,
  runs the registered callback (which triggers `Appenderator.drop()` and eventually offset commit in the task runner)
  and removes the descriptor from the wait set.
- `CoordinatorClient` / `CoordinatorClientImpl` — `client/coordinator/CoordinatorClient.java`,
  `server/src/main/java/org/apache/druid/client/coordinator/CoordinatorClientImpl.java:57` — `isHandoffComplete()` (`:72`) issues
  `GET /druid/coordinator/v1/datasources/{dataSource}/handoffComplete?interval=...&partitionNumber=...&version=...`
  against the Coordinator leader via `ServiceClient`.
- **Exact Coordinator endpoint hit**: `DataSourcesResource.isHandOffComplete()` —
  `server/src/main/java/org/apache/druid/server/http/DataSourcesResource.java:909`, `@Path("/{dataSourceName}/handoffComplete")`
  (`:906`). Logic: if no `LoadRule` applies / `shouldMatchingSegmentBeLoaded()` is false, returns `true` immediately
  (segment will never be handed off). Otherwise it looks up the segment in `CoordinatorServerView.getTimeline()`
  (`:934`, field `serverInventoryView` at `:115`) and returns `true` only if a server holding it is a
  `DruidServerMetadata.isSegmentReplicationTarget()` (`isSegmentLoaded()` at `:964`). Note this uses the
  **Coordinator's own view of what Historicals are announcing**, not a live RPC to Historicals — see Coordinator section.
- `DataSegmentServerAnnouncer` — `server/coordination/DataSegmentServerAnnouncer.java` (interface),
  `CuratorDataSegmentServerAnnouncer` — used by a server process to announce "I exist and serve segments" (distinct
  from per-segment announcement, see Historical section).

### Metadata store

- `IndexerSQLMetadataStorageCoordinator` — `server/src/main/java/org/apache/druid/metadata/IndexerSQLMetadataStorageCoordinator.java:101`
  — implements `IndexerMetadataStorageCoordinator`. `commitSegmentsAndMetadata()` (`:428`) runs the atomic
  transaction: `updateDataSourceMetadataInTransaction()` (checks/updates the datasource's committed offsets,
  `:2234`) then `insertSegments()` (`:1770`/`:1988`, `transaction.insertSegments()` at `:1817`/`:2031`) — segment
  rows and datasource-metadata row are written together so a segment is never visible without its offset commit
  being consistent. Also `commitAppendSegmentsAndMetadata()` (`:549`, used by streaming append ingestion),
  `commitReplaceSegments()` (`:472`), `commitAppendSegmentsAndMetadataInTransaction()` (`:1175`).
- `SqlSegmentsMetadataManager` — `server/src/main/java/org/apache/druid/metadata/SqlSegmentsMetadataManager.java` — the
  Coordinator-side cache of "used" segments, periodically polled from the segments table (`SegmentsMetadataManager`
  interface). Feeds `DataSourcesSnapshot` used by `RunRules`.
- `SqlSegmentsMetadataManagerV2` — `server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentsMetadataManagerV2.java`
  (unverified whether this is the active implementation vs V1 in this branch — both present).
- `MetadataRuleManager` — interface implemented elsewhere; used by `DataSourcesResource` (`metadataRuleManager.getRulesWithDefault()`
  at `:917`) and `RunRules` (`MetadataAction.GetDatasourceRules`) to fetch load/drop rules per datasource.

### Coordinator (`org.apache.druid.server.coordinator.*`)

- `DruidCoordinator` — `server/src/main/java/org/apache/druid/server/coordinator/DruidCoordinator.java:119` (860 lines).
  `start()` (`:366`) registers a leader-election listener and builds duty groups. `makeHistoricalManagementDuties()`
  (`:550`) is the ordered list run every Coordinator period: `PrepareBalancerAndLoadQueues` → `RunRules` (`:563`) →
  `UpdateReplicationStatus` → `CollectSegmentStats` → `UnloadUnusedSegments` → `MarkOvershadowedSegmentsAsUnused` →
  `MarkEternityTombstonesAsUnused` → `BalanceSegments` → `CloneHistoricals` → `CollectLoadQueueStats`. Each duty group
  runs on its own schedule inside a `DutiesRunnable` (`:693`, `run()` at `:712`).
- `CoordinatorDuty` — `server/coordinator/duty/CoordinatorDuty.java` — one pipeline stage,
  `DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams)`.
- `RunRules` — `server/src/main/java/org/apache/druid/server/coordinator/duty/RunRules.java:54` — `run()` (`:71`) iterates `usedSegments` (newest first,
  from `params.getUsedSegmentsNewestFirst()`), skips overshadowed segments, finds the first matching `Rule` per
  segment via `Rule.appliesTo()`, and delegates to it.
- `Rule` / `LoadRule` — `server/coordinator/rules/Rule.java`, `server/src/main/java/org/apache/druid/server/coordinator/rules/LoadRule.java:36` — `LoadRule.run()`
  (`:67`) calls `handler.replicateSegment(segment, getTieredReplicants())`; `shouldMatchingSegmentBeLoaded()` (`:79`)
  is exactly what `DataSourcesResource.isHandOffComplete()` consults. Concrete rules: `ForeverLoadRule`,
  `PeriodLoadRule`, `IntervalLoadRule` (`server/coordinator/rules/*.java`). Drop counterparts: `DropRule`,
  `ForeverDropRule`, `PeriodDropRule`, `PeriodDropBeforeRule`, `IntervalDropRule`. Broadcast:
  `BroadcastDistributionRule` and its `Forever`/`Interval`/`Period` variants.
- `SegmentActionHandler` — `server/coordinator/rules/SegmentActionHandler.java` — interface `LoadRule` calls
  (`replicateSegment`, `broadcastSegment`, `deleteSegment`, `moveSegment`).
- `StrategicSegmentAssigner` — `server/src/main/java/org/apache/druid/server/coordinator/loading/StrategicSegmentAssigner.java:56` — implements
  `SegmentActionHandler`. `replicateSegment()` (`:202`) computes required replica counts per tier
  (`SegmentReplicaCountMap`) and calls into per-tier assignment (`:536`, `:556` `replicateSegment(segment, server)`)
  which ultimately calls `SegmentLoadQueueManager`. Also `moveSegment()` (`:135`,`:177`), `broadcastSegment()` (`:327`),
  `deleteSegment()` (`:366`).
- `SegmentLoadQueueManager` — `server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentLoadQueueManager.java:32` — `loadSegment(segment, server, action)`
  (`:52`) calls `server.getPeon().loadSegment(segment, action, null)` (`:59`); `dropSegment()` (`:70`).
- `LoadQueuePeon` (interface) / `HttpLoadQueuePeon` — `server/coordinator/loading/LoadQueuePeon.java`,
  `server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:80` — `loadSegment()` (`:474`) and `dropSegment()` (`:508`) enqueue a `SegmentHolder` and
  schedule `doSegmentManagement()`, which batches queued `SegmentChangeRequestLoad`/`Drop` requests and POSTs them
  to the target Historical's segment-change endpoint, tracked via `ChangeRequestHttpSyncer`-style polling.
- `ReplicationThrottler` — `server/coordinator/loading/ReplicationThrottler.java` — caps in-flight replica loads per tier.
- `DruidCluster` / `ServerHolder` — `server/coordinator/DruidCluster.java`, `server/coordinator/ServerHolder.java` —
  in-memory view of historicals grouped by tier, with load-queue state (`startOperation`/`cancelOperation` used at
  `StrategicSegmentAssigner`/`SegmentLoadQueueManager`).
- `BalanceSegments` — `server/coordinator/duty/BalanceSegments.java` — cross-server rebalancing duty (uses balancer
  strategies under `server/coordinator/balancer/`, e.g. cost-based balancing — package present but not individually
  verified here `(unverified)` beyond its existence).
- Coordinator HTTP resource reporting load/serving status: `DataSourcesResource` —
  `server/src/main/java/org/apache/druid/server/http/DataSourcesResource.java:106` — besides `handoffComplete`
  (`:906`), exposes `getLoadInfoForAllSegments()`-backed endpoints (`:505`) and served-segments-in-interval listing
  (`prepareServedSegmentsInInterval()` at `:883`). It depends on `CoordinatorServerView` (`client/CoordinatorServerView.java`,
  field at `server/src/main/java/org/apache/druid/server/http/DataSourcesResource.java:115`) — the Coordinator's own `ServerInventoryView`-backed picture of what each
  Historical is announcing as served, kept live via the same server/segment announcement mechanism the Broker uses.

### Historical (`org.apache.druid.server.coordination.*`, `org.apache.druid.server.SegmentManager`, `segment/loading/*`)

- `SegmentLoadDropHandler` — `server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:61`
  — receives load/drop change requests (originally routed from the Coordinator's `HttpLoadQueuePeon` POST). `addSegment()`
  (`:146`) calls `segmentManager.loadSegment(segment)` (`:165`) then `announcer.announceSegment(segment)` (`:173`).
  `removeSegment()` (`:196`/`:202`) calls `announcer.unannounceSegment(segment)` (`:210`) then drops locally.
- `SegmentManager` — `server/src/main/java/org/apache/druid/server/SegmentManager.java:62` — builds/maintains the
  per-datasource `VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>` (`getTimeline()` at `:123`).
  `loadSegment()` (`:199`) fetches segment files via `SegmentCacheManager` and adds to the timeline;
  `loadSegmentOnBootstrap()` (`:166`) is the startup path (see `SegmentBootstrapper` below); `dropSegment()` (`:281`).
- `SegmentCacheManager` (interface) / `SegmentLocalCacheManager` — `segment/loading/SegmentCacheManager.java`,
  `server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:56` — `getSegmentFiles()` (`:357`) downloads/reads a segment from
  deep storage into a local `StorageLocation`, selecting among configured locations via `StorageLocationSelectorStrategy`.
- `StorageLocation` — `server/src/main/java/org/apache/druid/segment/loading/StorageLocation.java:42` — one configured local disk path with capacity
  tracking/reservation (`maybeReserve()`).
- `SegmentLoaderConfig` — `segment/loading/SegmentLoaderConfig.java` — configures local cache locations, max size,
  and (for tasks) temp storage used during push before/while segments are pushed to deep storage.
- `DataSegmentAnnouncer` (interface) / `BatchDataSegmentAnnouncer` — `server/coordination/DataSegmentAnnouncer.java`,
  `server/src/main/java/org/apache/druid/server/coordination/BatchDataSegmentAnnouncer.java:61` — `announceSegment()` (`:146`)/`unannounceSegment()` (`:222`),
  `announceSegments()` (`:252`, batched) — this is how a Historical publishes which segments it holds, consumed by
  `HttpServerInventoryView` polling (Broker/Coordinator side) rather than pushed directly.
- `DataSegmentServerAnnouncer` / `CuratorDataSegmentServerAnnouncer` — announces the server's existence (node-level,
  not per-segment) — see Service discovery section.
- `SegmentBootstrapper` — `server/src/main/java/org/apache/druid/server/coordination/SegmentBootstrapper.java:67` — on Historical startup,
  `loadSegmentsOnStartup()` (`:172`) loads previously-cached segments from local disk (`segmentManager.loadSegmentOnBootstrap()`
  at `:208`) and optionally fetches a bootstrap segment set from the Coordinator
  (`coordinatorClient.fetchBootstrapSegments()` at `:288`), then calls `serverAnnouncer.announce()` (`:126`).
- `ZkCoordinator` — `server/coordination/ZkCoordinator.java` — present in the source tree; the default/verified
  serving mechanism in this branch's `ServerViewModule` is HTTP-based (`druid.serverview.type=http`), so ZK-based
  coordination should be treated as legacy/optional `(unverified whether still wired by default)`.
- Query serving on a Historical: `ServerManager` — `server/src/main/java/org/apache/druid/server/coordination/ServerManager.java:86` — implements
  `QuerySegmentWalker`. `getQueryRunnerForIntervals()` (`:129`) resolves intervals to `SegmentDescriptor`s via the
  local timeline and delegates to `getQueryRunnerForSegments()` (`:170`), which builds per-segment query runners
  (`acquireAllSegments()` at ~`:186`, reference-counted via `ReferenceCountedSegmentProvider`).
- `QueryResource`, `QueryLifecycle`, `QueryScheduler` — `server/QueryResource.java`, `server/QueryLifecycle.java`,
  `server/QueryScheduler.java` — shared HTTP query entrypoint/lifecycle/laning used by both Broker and Historical
  processes (not Historical-specific, but this is where a Historical's incoming query from `DirectDruidClient` lands).

### Broker (`org.apache.druid.client.*`, `org.apache.druid.server.*`)

- `BrokerServerView` — `server/src/main/java/org/apache/druid/client/BrokerServerView.java:62` — implements
  `TimelineServerView`; the Broker's live segment→server timeline. Registers a `ServerView.SegmentCallback` (around
  `:132`) against the injected `ServerInventoryView`/`FilteredServerInventoryView`: `serverAddedSegment()`/`serverRemovedSegment()`
  (`:262`,`:307`) update `ServerSelector`s per segment, `serverAdded`/`serverRemoved` (`:164`,`:174`) track
  `QueryableDruidServer`s.
- `HttpServerInventoryView` — `server/src/main/java/org/apache/druid/client/HttpServerInventoryView.java:86` —
  **the discovery mechanism actually used in this version** (bound via `druid.serverview.type=http`, the class-doc
  at `:82` says it polls `GET /druid-internal/v1/segments` on each queryable server, via a per-server
  `ChangeRequestHttpSyncer` at `:542`/`:547`). `ServerViewModule` (`server/src/main/java/org/apache/druid/guice/ServerViewModule.java:36`) binds
  `ServerInventoryViewProvider`/`FilteredServerInventoryViewProvider` generically by `druid.serverview.type`
  (`SERVERVIEW_TYPE_HTTP="http"` at `:41` vs `SERVERVIEW_TYPE_BATCH="batch"`/`BatchServerInventoryView` at `:42`,
  ZK-based, legacy). `getInventory()` (`:319`) returns the live `DruidServer` collection.
- `ServerSelector` — `client/ServerSelector.java` (not separately verified beyond usage above) — per-segment set of
  candidate servers plus a tier-selection strategy, feeding `SegmentServerSelector`.
- `SegmentServerSelector`, `DruidServerHolder`(`QueryableDruidServer`) — `client/SegmentServerSelector.java`,
  `client/QueryableDruidServer.java` — pair a `SegmentDescriptor` with the chosen `ServerSelector`/server.
- `TimelineServerView` — `client/TimelineServerView.java` — interface `BrokerServerView` implements and
  `CachingClusteredClient` consumes (`getTimeline()`, `getQueryRunner()`).
- `CachingClusteredClient` — `server/src/main/java/org/apache/druid/client/CachingClusteredClient.java:117` —
  implements `QuerySegmentWalker`. `getQueryRunnerForIntervals()` (`:181`) / `getQueryRunnerForSegments()` (`:220`)
  resolve the query's datasource against `serverView.getTimeline()` (`:336`) — optionally transformed by a
  `timelineConverter` (`:200`) — then per matched segment call `serverView.getQueryRunner(server)` (`:685`) to get a
  `DirectDruidClient`-backed runner, fanning the query out and merging results.
- `DirectDruidClient` — `server/src/main/java/org/apache/druid/client/DirectDruidClient.java:90` — `QueryRunner` implementation that issues the actual HTTP
  query request to a specific Historical (`run()` at `:155`).
- `CacheUtil` — `client/CacheUtil.java` — per-segment/result-level cache key helpers used by `CachingClusteredClient`/`CachingQueryRunner`.
- `ClientQuerySegmentWalker` — referenced by `CachingClusteredClient` class docs as the Broker-level walker that wraps
  it with subquery/union handling `(location not independently verified in this pass, expected under server/src/main/java/org/apache/druid/server)`.
- `QueryResource` / `BrokerQueryResource` — `server/QueryResource.java`, `server/BrokerQueryResource.java` — Broker's
  HTTP query entrypoint and Broker-specific query-status endpoints.
- Schema caching: no `SegmentMetadataCache`/`BrokerSegmentMetadataCache` class was found directly under `server/`;
  such classes for SQL schema live in `sql/` `(unverified — out of scope for this doc, only flagging the gap)`.

### Router (`org.apache.druid.server.router.*`)

Important finding: in this checkout, **the actual Router query-forwarding classes are NOT in `server/`** — they live
in the `services/` module: `services/src/main/java/org/apache/druid/server/AsyncQueryForwardingServlet.java`,
`services/src/main/java/org/apache/druid/server/router/QueryHostFinder.java`,
`services/src/main/java/org/apache/druid/server/router/TieredBrokerHostSelector.java`,
`services/src/main/java/org/apache/druid/server/router/TieredBrokerConfig.java`.

What actually lives under `server/src/main/java/org/apache/druid/server/router/` is a small support set:

- `Router` — `server/src/main/java/org/apache/druid/server/router/Router.java:34` — a Guice `@BindingAnnotation`
  marker only, no logic.
- `ManagementProxyConfig` — `server/src/main/java/org/apache/druid/server/router/ManagementProxyConfig.java`.
- `AvaticaConnectionBalancer` (interface), `ConsistentHashAvaticaConnectionBalancer`,
  `RendezvousHashAvaticaConnectionBalancer`, plus `ConsistentHasher`/`RendezvousHasher` — JDBC/Avatica connection
  balancing strategies for the Router.
- `AsyncManagementForwardingServlet` — `server/src/main/java/org/apache/druid/server/AsyncManagementForwardingServlet.java`
  — forwards non-query management API calls (this one IS in `server/`, distinct from the query-forwarding servlet in `services/`).

### Service discovery / announcement plumbing

- `DruidNodeAnnouncer` (interface) / `CuratorDruidNodeAnnouncer` — `discovery/DruidNodeAnnouncer.java`,
  `curator/discovery/CuratorDruidNodeAnnouncer.java` — announces a process's `DiscoveryDruidNode` (role, host, port,
  services) to ZooKeeper on startup.
- `DruidNodeDiscoveryProvider` — `server/src/main/java/org/apache/druid/discovery/DruidNodeDiscoveryProvider.java:42` — abstract; `getForNodeRole(NodeRole)`
  (`:59`) returns a `DruidNodeDiscovery` for a role (e.g. `NodeRole.HISTORICAL`, `BROKER`, `COORDINATOR`); used by
  Brokers/Coordinators/Routers to find live peers of a given role. `CuratorDruidNodeDiscoveryProvider` is the
  ZK-backed implementation.
- `NodeRole` — `discovery/NodeRole.java` — enum of process roles (COORDINATOR, HISTORICAL, BROKER, ROUTER, etc.).
- `DiscoveryDruidNode`, `DataNodeService`, `LookupNodeService`, `WorkerNodeService` — `discovery/*.java` — the
  announced payload describing what a node serves.
- `org.apache.druid.curator.discovery.*` — `ServerDiscoverySelector`, `ServerDiscoveryFactory`, `CuratorServiceAnnouncer`
  — older Curator-based service discovery/selection, still present alongside `DruidNodeDiscoveryProvider`.
- Note: this is distinct from the **per-segment** announcement mechanism (`DataSegmentAnnouncer`/`BatchDataSegmentAnnouncer`)
  used by Historicals and consumed by `HttpServerInventoryView`/`CoordinatorServerView` — node discovery says "a
  Historical process exists at host:port"; segment announcement says "that Historical currently serves segment X".

## Critical flows

1. **Appenderator persist → push to S3 → metadata insert**
   `StreamAppenderator#persistAll` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:631`) →
   `StreamAppenderator#push` (`:773`) → `StreamAppenderator#mergeAndPush` (`:874`) →
   `DataSegmentPusher#push` (`:977`, impl `S3DataSegmentPusher` in `extensions-core/s3-extensions`) →
   `BaseAppenderatorDriver#pushInBackground` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:493`) →
   `StreamAppenderatorDriver#publish` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:277`) →
   `BaseAppenderatorDriver#publishInBackground` (`:584`) →
   `TransactionalSegmentPublisher#publish` (impl in `indexing-service`, issues `SegmentTransactionalInsertAction`) →
   `IndexerSQLMetadataStorageCoordinator#commitSegmentsAndMetadata` (`server/src/main/java/org/apache/druid/metadata/IndexerSQLMetadataStorageCoordinator.java:428`)
   → `updateDataSourceMetadataInTransaction` (`:2234`) + `insertSegments` (`:1770`) inside one DB transaction.

2. **Coordinator poll → RunRules → LoadRule → LoadQueuePeon → Historical load → announce**
   `DruidCoordinator.DutiesRunnable#run` (`server/src/main/java/org/apache/druid/server/coordinator/DruidCoordinator.java:712`) →
   `RunRules#run` (`server/src/main/java/org/apache/druid/server/coordinator/duty/RunRules.java:71`, iterates `usedSegmentsNewestFirst`) →
   `LoadRule#run` (`server/src/main/java/org/apache/druid/server/coordinator/rules/LoadRule.java:67`) → `StrategicSegmentAssigner#replicateSegment` (`server/src/main/java/org/apache/druid/server/coordinator/loading/StrategicSegmentAssigner.java:202`) →
   per-tier `StrategicSegmentAssigner#replicateSegment(segment, server)` (`:556`) →
   `SegmentLoadQueueManager#loadSegment` (`server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentLoadQueueManager.java:52`) →
   `HttpLoadQueuePeon#loadSegment` (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:474`, enqueues + HTTP POST to Historical) →
   `SegmentLoadDropHandler#addSegment` (`server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:146`) →
   `SegmentManager#loadSegment` (`server/src/main/java/org/apache/druid/server/SegmentManager.java:199`, via `SegmentLocalCacheManager#getSegmentFiles`) →
   `BatchDataSegmentAnnouncer#announceSegment` (`server/src/main/java/org/apache/druid/server/coordination/BatchDataSegmentAnnouncer.java:146`).

3. **Task's handoff wait → CoordinatorBasedSegmentHandoffNotifier → task exits** (offsets were already
   committed at flow 1's publish step; this flow only releases the task)
   `StreamAppenderatorDriver#registerHandoff` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:321`) →
   `SegmentHandoffNotifier#registerSegmentHandoffCallback` (impl `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifier.java:63`) →
   scheduled `CoordinatorBasedSegmentHandoffNotifier#checkForSegmentHandoffs` (`:85`, poll period from
   `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifierConfig.java:29`, default `PT1S`) →
   `CoordinatorClient#isHandoffComplete` (`server/src/main/java/org/apache/druid/client/coordinator/CoordinatorClientImpl.java:72`, `GET .../handoffComplete`) →
   `DataSourcesResource#isHandOffComplete` (`server/src/main/java/org/apache/druid/server/http/DataSourcesResource.java:909`, consults `LoadRule.shouldMatchingSegmentBeLoaded`
   and `CoordinatorServerView` timeline) → on `true`, the registered callback runs
   (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:362`) → `Appenderator#drop` the local copy →
   the task's `handOffWaitList` future completes and `SeekableStreamIndexTaskRunner` can return
   `TaskStatus.success` (in `indexing-service`, outside this module).

4. **Segment placement propagating to the Broker's timeline**
   Historical's `BatchDataSegmentAnnouncer#announceSegment` (`server/src/main/java/org/apache/druid/server/coordination/BatchDataSegmentAnnouncer.java:146`) makes the segment
   visible on the Historical's inventory node → Broker's `HttpServerInventoryView` (`server/src/main/java/org/apache/druid/client/HttpServerInventoryView.java:86`)
   polls `GET /druid-internal/v1/segments` on that Historical via `ChangeRequestHttpSyncer` (`:542`) and receives the
   change → fires segment-added callback → `BrokerServerView#serverAddedSegment` (`server/src/main/java/org/apache/druid/client/BrokerServerView.java:262`) updates
   the `ServerSelector` for that segment in the Broker's timeline (also consumed identically by the Coordinator's own
   `CoordinatorServerView`, which is what `DataSourcesResource#isHandOffComplete` reads).

5. **A query: Router → Broker (CachingClusteredClient) → Historical (ServerManager) → merged response**
   Router `AsyncQueryForwardingServlet`/`QueryHostFinder`/`TieredBrokerHostSelector` (`services/` module, not `server/`)
   pick a Broker → Broker `QueryResource`/`QueryLifecycle` (`server/QueryResource.java`) →
   `CachingClusteredClient#getQueryRunnerForIntervals` (`server/src/main/java/org/apache/druid/client/CachingClusteredClient.java:181`) →
   `CachingClusteredClient#run` (timeline lookup via `serverView.getTimeline`, `:336`) →
   `serverView.getQueryRunner(server)` (`:685`) → `DirectDruidClient#run` (`server/src/main/java/org/apache/druid/client/DirectDruidClient.java:155`, HTTP call to
   Historical) → Historical `QueryResource`/`QueryLifecycle` → `ServerManager#getQueryRunnerForSegments`
   (`server/src/main/java/org/apache/druid/server/coordination/ServerManager.java:170`) → per-segment `ReferenceCountedSegmentProvider` lookups and query execution → results
   stream back and are merged by `CachingClusteredClient` before returning through Broker → Router.

## Cross-module pointers

- `extensions-core/kafka-indexing-service/` — `KafkaIndexTask` (`.../indexing/kafka/KafkaIndexTask.java`) drives
  ingestion; built on `SeekableStreamIndexTaskRunner` (`indexing-service/.../seekablestream/SeekableStreamIndexTaskRunner.java`),
  which owns the `StreamAppenderator`/`StreamAppenderatorDriver` instances documented above and supplies the
  Kafka offsets (as `KafkaDataSourceMetadata`) that ride along in the publish transaction.
- `indexing-service/` — task action framework (`SegmentTransactionalInsertAction` et al.) that turns
  `TransactionalSegmentPublisher#publish` calls into RPCs against `IndexerSQLMetadataStorageCoordinator`;
  `SeekableStreamIndexTaskRunner` as above.
- `processing/` — `DataSegmentPusher` interface (`processing/.../segment/loading/DataSegmentPusher.java`) that
  `StreamAppenderator` depends on; also owns core segment/query execution types (`Segment`, `QueryRunner`,
  `VersionedIntervalTimeline`) that `server/`'s `SegmentManager`/`ServerManager`/`CachingClusteredClient` build on.
- `extensions-core/s3-extensions/` — `S3DataSegmentPusher` (`.../storage/s3/S3DataSegmentPusher.java`) is the concrete
  `DataSegmentPusher` used for the S3 deep-storage push step in flow (1).
- `services/` — hosts the actual Router runtime logic (`AsyncQueryForwardingServlet`, `QueryHostFinder`,
  `TieredBrokerHostSelector`, `TieredBrokerConfig`) that `server/`'s `router` package only supports with small helper
  classes (see Router section above).
- `sql/` — Broker/Coordinator SQL planning and (likely) segment-metadata-for-SQL caching; not covered here.

## Gotchas

- **Coordinator poll period vs. handoff latency**: handoff detection is fully poll-driven, not event-driven, on both
  ends — the task polls `handoffComplete` on its own `CoordinatorBasedSegmentHandoffNotifierConfig` period, and that
  endpoint's answer depends on the Coordinator's `CoordinatorServerView` having already observed the Historical's
  segment announcement via `HttpServerInventoryView`'s own poll cycle. Expect end-to-end handoff latency to be the
  sum of: load-queue processing time + Historical's inventory poll interval + task's handoff poll interval — not
  instantaneous even after the segment is fully loaded on a Historical.
- **Load queue size/throttling**: `HttpLoadQueuePeon` batches queued load/drop requests per server and
  `ReplicationThrottler` caps how many replica loads can be in flight per tier at once; a burst of new segments (e.g.
  a large backfill) can visibly delay handoff for unrelated concurrently-running realtime tasks because they share
  the same per-server load queue capacity.
- **Rule eligibility short-circuits handoff**: if no `LoadRule` applies to a segment's interval (or the matching rule
  has zero total replicas via `shouldMatchingSegmentBeLoaded()` returning false), `DataSourcesResource.isHandOffComplete`
  returns `true` immediately — i.e. the task believes handoff is "complete" without the segment ever landing on a
  Historical. This is intentional (data is still safely in deep storage and in the metadata store) but means a
  successful task tells you nothing about queryability: **committed offsets never imply the data is queryable.**
- **Segment reference counting**: `ServerManager`/`SegmentManager` use `ReferenceCountedSegmentProvider` so that
  in-flight queries hold a segment open even if a concurrent drop request arrives; a segment physically stays on disk
  until all references are released.
- **Tiering/replication**: `LoadRule.tieredReplicants` and `StrategicSegmentAssigner.replicateSegment` treat each tier
  independently — a segment can be "loaded" on one tier and still pending on another; `handoffComplete` only checks
  for any single `isSegmentReplicationTarget()` server, not full replication-factor satisfaction across all tiers.
- **Broker view staleness**: `BrokerServerView` (and the Coordinator's own `CoordinatorServerView`) reflect whatever
  `HttpServerInventoryView` last synced from each Historical; a Historical that is slow/unresponsive can leave stale
  segment-to-server mappings until its `ChangeRequestHttpSyncer` connection times out and resyncs
  (`config.getServerTimeout()` in `HttpServerInventoryView`), which can cause transient query failures or
  over-optimistic handoff answers if not accounted for.
- **`druid.serverview.type`**: switching between `http` (default, verified path documented above) and `batch`
  (ZK-based `BatchServerInventoryView`, legacy) changes both the Broker's and Coordinator's discovery mechanism
  simultaneously — this is a single cluster-wide config, not settable per-process independently `(unverified beyond
  the binding code in ServerViewModule)`.
