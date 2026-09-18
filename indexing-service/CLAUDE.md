# druid-indexing-service

`indexing-service` implements Druid's task-based indexing framework: the Overlord's task
lifecycle (queue, locking, task actions, metadata coordination), the generic streaming-ingestion
engine and supervisor framework that Kafka/Kinesis extend, and the worker-side execution model
used by Indexer/MiddleManager+Peon processes. It does not itself know about Kafka — the Kafka
specifics (`KafkaIndexTask`, `KafkaSupervisor`, `KafkaRecordSupplier`) live in
`extensions-core/kafka-indexing-service` and extend the abstract classes defined here.

> **Execution mechanics live in [`Code-deepdive-Claude.MD`](Code-deepdive-Claude.MD)** —
> task lifecycle, the seekablestream read loop, the publish/handoff future graph, task-action retry math, traced at method-body granularity with thread ownership, failure modes and config knobs.
> This file is the class map; that one is the runtime behaviour.

Loaded by (via Guice modules registered on each process's classpath):
- **Overlord** — task queue/lockbox/storage, `OverlordResource`, `SupervisorResource`, task actions.
- **Indexer** (or classic **MiddleManager**) — `WorkerTaskManager`/`WorkerTaskMonitor`, `ForkingTaskRunner`/`ThreadingTaskRunner`, `WorkerResource`.
- **Peon** (forked task JVM, or in-process for Indexer) — `TaskToolbox`, `SeekableStreamIndexTaskRunner`, runs the actual task.

## Position in the ingestion path

> **Ordering fact worth internalizing (verified in code):** the Kafka offsets are committed in the
> **same metadata transaction as the segment publish**, which happens **before** the segment is
> loaded onto any Historical — *not* after. In
> `SeekableStreamIndexTaskRunner.publishAndRegisterHandoff` (`:1013`) the `publishFuture`
> (segments + offsets, one txn) is created first, and `driver.registerHandoff(...)` (`:1070`) is
> only chained onto its **success callback**. Handoff therefore gates *task completion*
> (`TaskStatus.success`), not offset durability. If the Coordinator never loads the segment, the
> offsets are already committed and the segment is already in deep storage + the metadata store —
> the task just hangs in handoff-wait until `handoffConditionTimeout` fires. See
> `server/CLAUDE.md` for the Coordinator/handoff side.

```
Kafka topic
   │  (RecordSupplier — defined in kafka-indexing-service, consumed via seekablestream.common.RecordSupplier)
   ▼
[Peon JVM] SeekableStreamIndexTaskRunner.run()/runInternal()   <-- indexing-service
   read record -> StreamAppenderatorDriver.add() -> incremental persist (local disk)
   -> driver.push() (segment build) -> deep storage (S3) push
   -> driver.publish() = SegmentTransactionalAppendAction/InsertAction (ONE metadata txn:
      segment set + datasource metadata/offsets) -> submitted via TaskActionClient to Overlord
   -> driver.registerHandoff() -> waits for Coordinator to load segment onto a Historical
   │
   ▼
[Overlord] TaskQueue / TaskLockbox / TaskActionToolbox -> IndexerMetadataStorageCoordinator (server/)
   -> metadata store (segments table + datasource metadata table, single transaction)
   │
   ▼
Coordinator (server/, not in this module) loads segment onto Historical, handoff notified back
   -> only then can the SeekableStreamIndexTaskRunner report TaskStatus.success and exit
   │
   ▼
Broker (server/) learns new segment location via ServerView/BrokerServerView (not in this module)
   -> Router -> Broker -> Historicals serve queries
```

## Key classes by responsibility

### Task lifecycle & Overlord
- `indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskQueue.java:103` — in-memory queue of active tasks; `manage()` (`:358`), `manageQueuedTasks()` (`:376`), `add()` (`:498`), `shutdown()` (`:617`), `notifyStatus()` (`:682`) persists status to `TaskStorage` before telling `TaskRunner` to shut the task down.
- `indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskLockbox.java:88` — grants/revokes time-chunk and segment locks; `lock()` (`:352`/`:378`), `tryLock()` (`:406`), `revokeLock()` (`:804`); every operation runs under this instance's `giant` `ReentrantLock` (`:101`). Note there is **one `TaskLockbox` per datasource**, created and reference-counted by `indexing-service/src/main/java/org/apache/druid/indexing/overlord/GlobalTaskLockbox.java:65` (`datasourceToLockbox` map, instantiated at `:488`) — so lock operations serialize *within* a datasource, not across the whole Overlord.
- `indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskStorage.java`, `MetadataTaskStorage.java`, `HeapMemoryTaskStorage.java` — task metadata persistence (DB-backed vs in-memory).
- `indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskMaster.java` — Overlord leadership election wrapper; owns the active `TaskQueue`/`TaskRunner` instances.
- `TaskRunner` (`overlord/TaskRunner.java`) implementations: `RemoteTaskRunner.java` (ZK-based, legacy MiddleManager), `ForkingTaskRunner.java` (forks Peon JVMs, Indexer/MM), `ThreadingTaskRunner.java`/`SingleTaskBackgroundRunner.java` (in-process, Indexer).
- `indexing-service/src/main/java/org/apache/druid/indexing/common/TaskToolboxFactory.java:79` — `build(Task)` (`:219`) constructs the per-task `TaskToolbox`.
- `indexing-service/src/main/java/org/apache/druid/indexing/common/TaskToolbox.java:83` — the bag of dependencies handed to every task: `getTaskActionClient()` (`:254`), `getDataSegmentServerAnnouncer()` (`:294`), `getSegmentHandoffNotifierFactory()` (`:299`), segment pushers, metadata coordinator access, etc.
- `indexing-service/src/main/java/org/apache/druid/indexing/common/task/TaskResource.java:28` — NOT an HTTP resource; declares a task's required worker capacity/CPU (`availabilityGroup`, required capacity).

### Streaming ingestion engine (shared base for Kafka/Kinesis)
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTask.java:61` — abstract Task; `createTaskRunner()` (`:273`) is the template-method seam Kafka implements to return a `KafkaIndexTaskRunner`.
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:146` (2158 lines) — the read→index→persist→publish→handoff loop:
  - `run(TaskToolbox)` (`:292`) → `runInternal(TaskToolbox)` (`:396`): creates `StreamAppenderatorDriver` (`:463`), calls `driver.startJob()` (`:466`), loops reading records and calling `driver.add()` (`:690`), triggers `driver.persistAsync`/`persist` (`:723`, `:824`), and on stream end calls `publishAndRegisterHandoff()` (`:862`/`:1013`) per sequence, then waits on `handOffWaitList` (`:878-901`).
  - `publishAndRegisterHandoff(SequenceMetadata)` (`:1013`) — builds `driver.publish(publisher, committer, sequenceNames)` (`:1018`), then on success calls `driver.registerHandoff()` (`:1070`).
  - Template-method abstract seams Kafka must fill (`:2036-2122`): `isEndOfShard`, `getCheckPointsFromContext`, `getNextStartOffset`, `deserializePartitionsFromMetadata`, `getRecords`, `createDataSourceMetadata`, `createSequenceNumber`, `possiblyResetDataSourceMetadata`, `isEndOffsetExclusive`, `getSequenceMetadataTypeReference`.
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SequenceMetadata.java:54` — one ingestion "sequence" (a contiguous span of offsets sharing a segment-publish unit); `createPublisher()` (`:320`) builds a `SequenceMetadataTransactionalSegmentPublisher` (`:333`) that picks `SegmentTransactionalAppendAction` or `SegmentTransactionalInsertAction` (`:404-428`) depending on whether concurrent append/replace mode is on.
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/common/RecordSupplier.java`, `OrderedPartitionableRecord.java`, `OrderedSequenceNumber.java`, `StreamPartition.java` — generic stream-reading abstractions Kafka's `KafkaRecordSupplier` implements.
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/StreamChunkParser.java`, `SettableByteEntityReader.java` — turn raw stream bytes (`ByteEntity`) into `InputRow`s.

### Supervisor framework
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:152` (4748 lines) — owns task-group lifecycle for a datasource's streaming ingestion.
  - `runInternal()` (`:1710`) drives one supervisor tick: `discoverTasks()` (`:2061`), `updatePartitionLagFromStream()`/`checkTaskDuration()` (`:1725`, `:1823`).
  - `createNewTasks()` (`:3809`) → `createIndexTasks(...)` (`:4122`, abstract at `:4393`) — builds and submits new `index_kafka` tasks per task group.
  - `generateSequenceName(...)` (`:2755`) — deterministic sequence-name generation from partition offsets + `DataSchema` + tuning config; used both when building new tasks (`:231`) and when validating existing task checkpoints (`:2724`).
  - `checkpoint(int taskGroupId, DataSourceMetadata)` (`:4289`) — public entry point (called via `CheckpointNotice`, `:738`) that verifies the reported checkpoint matches expected in-memory state (`:758-784`) then calls `checkpointTaskGroup()`.
  - `TaskGroup` inner class (`:189`) tracks `checkpointSequences` (TreeMap of sequence-id → per-partition offsets, `:209`, `addNewCheckpoint` `:267`) — the in-memory view of what a task group has committed so far.
  - Template-method abstract seams Kafka fills (`:2792-4747`): `baseTaskName`, `createTaskIoConfig`, `createIndexTasks`, `getTaskGroupIdForPartition`, `checkSourceMetadataMatch`, `doesTaskMatchSupervisor`, `createDataSourceMetaDataForReset`, `makeSequenceNumber`, `setupRecordSupplier`, `createReportPayload`, `getNotSetMarker`/`getEndOfPartitionMarker`/`isEndOfShard`.
- `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/CheckPointDataSourceMetadataAction.java:32` — the `TaskAction` a task submits to the Overlord to report a new checkpoint; Overlord side calls back into `SeekableStreamSupervisor.checkpoint()`.

### Segment allocation, publish & offset commit
- `indexing-service/src/main/java/org/apache/druid/indexing/appenderator/ActionBasedSegmentAllocator.java:31` — `SegmentAllocator` impl used by streaming tasks; `allocate()` (`:50`) submits a `SegmentAllocateAction` via `SegmentAllocateActionGenerator`.
- `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentAllocateAction.java` — Overlord-side task action that allocates a new segment ID (talks to `IndexerMetadataStorageCoordinator` in `server/` under the Overlord's segment-allocation lock queue, `SegmentAllocationQueue.java`).
- `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentTransactionalInsertAction.java:56` — `perform()` (`:198`) calls `IndexerMetadataStorageCoordinator.commitMetadataOnly()` (`:206`, offsets only, no new segments) or `commitSegmentsAndMetadata()` (`:240`, segments + `startMetadata`/`endMetadata` i.e. Kafka offsets in one call). This is **the exactly-once commit**: segment publish and datasource-metadata (offset) update happen inside one metadata-store transaction in `IndexerMetadataStorageCoordinator` (`server/src/main/java/org/apache/druid/indexing/overlord/IndexerMetadataStorageCoordinator.java`).
- `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentTransactionalAppendAction.java:62` — `perform()` (`:154`) calls `commitAppendSegments()` (`:183`, no metadata check) or `commitAppendSegmentsAndMetadata()` (`:190`, segments + offsets together) — used when concurrent-append mode is enabled (multiple concurrently-published segment sets to the same interval).
- Both actions call the static `IndexerMetadataStorageCoordinator.validateDataSourceMetadata(supervisorId, startMetadata, endMetadata)` (`:110`/`:137`) before publishing, to catch stale/conflicting offset ranges.
- `indexing-service/src/main/java/org/apache/druid/indexing/appenderator/ActionBasedPublishedSegmentRetriever.java` — resolves already-published segment IDs for a task action (used to look up segments across replicas/concurrent tasks).

### Handoff
- `driver.publish(...)` / `driver.registerHandoff(...)` calls are made from `SeekableStreamIndexTaskRunner.publishAndRegisterHandoff` (`:1013`, `:1070`) — but `StreamAppenderatorDriver`, `SegmentsAndCommitMetadata`, and the actual `SegmentHandoffNotifier` implementation that polls/announces for a Historical to load the segment live in `server/src/main/java/org/apache/druid/segment/realtime/appenderator/` and `server/.../handoff/` — **not in this module**. `indexing-service` only wires the task to a `SegmentHandoffNotifierFactory` via `TaskToolbox.getSegmentHandoffNotifierFactory()` (`indexing-service/src/main/java/org/apache/druid/indexing/common/TaskToolbox.java:299`) and blocks on the resulting future (`handOffWaitList`, runner `:878-901`).

### Indexer worker execution
- `indexing-service/src/main/java/org/apache/druid/indexing/worker/WorkerTaskManager.java:84` — base class managing the set of tasks assigned to a worker process (implements `IndexerTaskCountStatsProvider`); tracks running/completed tasks, exposes them to `WorkerResource`.
- `indexing-service/src/main/java/org/apache/druid/indexing/worker/WorkerTaskMonitor.java:49` — `extends WorkerTaskManager`; legacy ZK-based announce/assign path (`RemoteTaskRunner` counterpart).
- `indexing-service/src/main/java/org/apache/druid/indexing/worker/TaskAnnouncement.java:36` — serialized status a worker publishes (to ZK or HTTP) so the Overlord's `RemoteTaskRunner`/`HttpRemoteTaskRunner` knows task state/location.
- `indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java` — bootstraps a forked Peon JVM (reads task JSON from stdin/file, builds `TaskToolbox`, runs the task, reports status back to parent).
- `indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java` — Indexer/MM-side: forks the Peon JVM process per task, wires up java opts/classpath.

### HTTP endpoints
- `indexing-service/src/main/java/org/apache/druid/indexing/overlord/http/OverlordResource.java:109` (`@Path("/druid/indexer/v1")`) — `POST /task` (`:161`, submit task), `GET /task/{taskid}/status` (`:288`), `GET /task/{taskid}/segments` (`:360`), `POST /task/{taskid}/shutdown` (`:374`), `POST /datasources/{dataSource}/shutdownAllTasks` (`:389`), `GET /leader`/`isLeader` (`:216`/`:228`).
- `indexing-service/src/main/java/org/apache/druid/indexing/overlord/supervisor/SupervisorResource.java:80` (`@Path(".../supervisor")`) — `POST /supervisor` (create/update spec, `:119`), `GET /{id}/status` (`:282`), `GET /{id}/stats` (`:327`), `POST /{id}/suspend`/`resume` (`:381`/`:390`), `POST /{id}/shutdown` (`:400`).
- `indexing-service/src/main/java/org/apache/druid/indexing/worker/http/WorkerResource.java:59` (`@Path("/druid/worker/v1")`) — `GET /tasks` (`:154`, tasks on this worker), `POST /task/{taskid}/shutdown` (`:181`), `POST /disable`/`enable` (`:96`/`:121`).

## Critical flows

### (a) Supervisor creates & manages index_kafka tasks
1. `SeekableStreamSupervisor.runInternal()` (`:1710`) runs on a notice-queue thread each tick.
2. `discoverTasks()` (`:2061`) reconciles running tasks from `TaskStorage`/`TaskRunner` against expected `TaskGroup`s; calls `getCheckpointsFromContext` per task and `verifyAndMergeCheckpoints`-style logic (`:2425+`) to rebuild `checkpointSequences`.
3. `checkTaskDuration()` (`:1823`) — for task groups whose tasks have exceeded `taskDuration`, issues a checkpoint/stop.
4. `createNewTasks()` (`:3809`) computes starting offsets per group, calls `generateSequenceName()` (`:2755`), then abstract `createIndexTasks()` (`:4393`, Kafka-specific impl in `KafkaSupervisor`) to build `SeekableStreamIndexTask` instances, and submits them through the injected `TaskQueue`/`TaskMaster`.

### (b) Task read→persist→push→publish→handoff cycle
1. `TaskToolboxFactory.build(Task)` (`:219`) assembles the `TaskToolbox` for the forked Peon.
2. `SeekableStreamIndexTaskRunner.run()` (`:292`) → `runInternal()` (`:396`): builds `StreamAppenderatorDriver` (`:463`), `driver.startJob()` (`:466`) restores any prior committed metadata.
3. Main loop calls `getRecords()` (abstract, Kafka impl) then per-row `driver.add(row, sequenceName, committerSupplier, ...)` (`:690`); when row/size thresholds are hit, `isPushRequired` triggers a segment handoff-eligible boundary (`:702-708`).
4. `driver.persistAsync`/`persist` (`:723`, `:824`) flushes the in-memory index to local disk incrementally.
5. On stream end or checkpoint boundary, `publishAndRegisterHandoff(sequenceMetadata)` (`:1013`) calls `driver.publish(publisher, committer, sequenceNames)` (`:1018`) — this internally pushes segments to deep storage and then submits the transactional task action.
6. On publish success, `driver.registerHandoff(...)` (`:1070`) is called and the task blocks (`handOffWaitList`, `:878-901`) until the Coordinator has loaded the segment onto a Historical before the task reports `TaskStatus.success`.

### (c) Exactly-once offset commit transaction
1. `SequenceMetadataTransactionalSegmentPublisher` (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SequenceMetadata.java:333`) builds either a `SegmentTransactionalInsertAction.appendAction(...)`/`overwriteAction` or `SegmentTransactionalAppendAction.forSegments(...)` (`:404-428`), passing `startMetadata`/`endMetadata` = the previous vs. new `SeekableStreamDataSourceMetadata` (Kafka offsets).
2. The action is submitted through `TaskActionClient` (`TaskToolbox.getTaskActionClient()`) to the Overlord.
3. `SegmentTransactionalInsertAction.perform()` (`:198`) or `SegmentTransactionalAppendAction.perform()` (`:154`) first calls `IndexerMetadataStorageCoordinator.validateDataSourceMetadata()` to reject stale offset ranges, then calls `commitSegmentsAndMetadata()`/`commitAppendSegmentsAndMetadata()` — a single metadata-store transaction that inserts the new segment rows AND updates the datasource's stored offsets. If either half would fail (e.g. offset mismatch from a concurrent task), the whole transaction fails and `SegmentPublishResult` reports failure — no segments are published without offsets being committed, and vice versa.

### (d) Checkpointing / incremental publish
1. As the task appends rows, `SeekableStreamIndexTaskRunner` tracks a `SequenceMetadata` per active sequence; when `AppenderatorDriverAddResult.isPushRequired()` is true and the sequence isn't yet checkpointed, it flags `sequenceToCheckpoint` (`:702-708`).
2. The task calls `CheckPointDataSourceMetadataAction` (submitted as a `TaskAction`) to tell the Overlord/Supervisor about the new checkpoint offsets.
3. `SeekableStreamSupervisor.checkpoint(taskGroupId, checkpointMetadata)` (`:4289`) validates the reported checkpoint against `TaskGroup.checkpointSequences` (`CheckpointNotice`, `:738-786`) and calls `checkpointTaskGroup()` to persist it, then instructs the task (via `SeekableStreamIndexTaskClient`) to publish the just-finished sequence early (incremental publish) while continuing to read the next sequence — this is what lets a single long-running task publish multiple segments over its lifetime without waiting for full task completion.

## Cross-module pointers
- `extensions-core/kafka-indexing-service` — `KafkaIndexTask`/`KafkaIndexTaskRunner` (extends `SeekableStreamIndexTask`/`Runner`), `KafkaSupervisor`/`KafkaSupervisorSpec` (extends `SeekableStreamSupervisor`/`Spec`), `KafkaRecordSupplier` (implements `RecordSupplier`), `KafkaIndexTaskIOConfig`.
- `server/` — `IndexerMetadataStorageCoordinator` (actual metadata-store transaction implementation for `commitSegmentsAndMetadata`/`commitAppendSegmentsAndMetadata`), `StreamAppenderatorDriver`/`BaseAppenderatorDriver`/`SegmentsAndCommitMetadata` (`org.apache.druid.segment.realtime.appenderator`), `SegmentHandoffNotifier` implementations, Coordinator's segment-loading/balancing duties, `BrokerServerView`/segment announcement to Broker.
- `processing/` — `Appenderator` core indexing/incremental-index machinery that `StreamAppenderatorDriver` wraps, `DataSchema`, row-level ingestion types (`InputRow`, `ByteEntity`).

## Gotchas
- The `TaskLockbox` "giant" lock is **per-datasource**, not process-wide: `GlobalTaskLockbox` keeps a `ConcurrentHashMap<String, DatasourceLockboxResource>` (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/GlobalTaskLockbox.java:65`) and builds one `TaskLockbox` per datasource (`:488`), each with its own `giant` `ReentrantLock` (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskLockbox.java:101`), reference-counted so an idle lockbox can be dropped (`:598-619`). High allocation churn on one datasource therefore does **not** serialize unrelated datasources — but every lock/unlock does go through a `ConcurrentHashMap.compute` (`:482`).
- `IndexerMetadataStorageCoordinator.validateDataSourceMetadata()` is called on every transactional publish — a stale `startMetadata` (e.g. from a lagging/duplicate task after a supervisor reset) causes the whole segment-publish transaction to fail, not just the offset update; segments and offsets are truly atomic (one succeeds, or neither does).
- Sequence names (`SeekableStreamSupervisor.generateSequenceName()`) are deterministic from partition offsets + `DataSchema` + tuning config — changing tuning config (e.g. `maxRowsPerSegment`) mid-flight can change generated sequence names and confuse checkpoint matching across supervisor restarts.
- `taskDuration` (tuning config) triggers task rollover via `checkTaskDuration()`, but the *old* task doesn't die until it finishes publishing+handoff for all its sequences — so at any time there can be more running tasks per partition than `taskCount` would suggest (overlap during rollover).
- Handoff wait (`handOffWaitList`) blocks task completion but does NOT block the read loop from starting new sequences — incremental/early publish (checkpointing) lets a task keep reading while a previous sequence is still waiting on Coordinator handoff.
- In `SegmentTransactionalAppendAction.perform()` (`indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentTransactionalAppendAction.java:154`) the choice between `commitAppendSegments` (`:183`) and `commitAppendSegmentsAndMetadata` (`:190`) keys off **`startMetadata == null`**, not off concurrent-append mode: with no `startMetadata` the segments are committed *without* any offset/metadata update. Streaming tasks pass non-null metadata whenever `ioConfig.isUseTransaction()` is set (see `SequenceMetadata.createPublisher`), so exactly-once holds — but a `useTransaction: false` supervisor silently takes the metadata-less branch. The action also requires the task to hold `TaskLockType.APPEND` locks (`:166`) and to implement `PendingSegmentAllocatingTask` (`:156`), or it throws.
- `WorkerTaskMonitor` (ZK-based) vs `WorkerTaskManager`/HTTP path are two different task-assignment mechanisms coexisting in the code — verify which `TaskRunner` (`RemoteTaskRunner` vs `HttpRemoteTaskRunner` in `overlord/hrtr`) the cluster actually uses before assuming task-status propagation semantics.

## Confluent fork deltas
Diff vs upstream tag `druid-34.0.0` touches `indexing-service/` in a handful of places (see `git diff --stat druid-34.0.0..HEAD -- indexing-service/`), notably: `WorkerCategorySpec.java`/`WorkerSelectUtils.java` (worker selection strategy changes, related to `#389` "task distribution strategy that assigns tasks to workers based on supervisor affinity"), `SeekableStreamIndexTaskRunner.java`/`StreamChunkParser.java`/`OrderedPartitionableRecord.java` (Kafka-header-based pre-ingestion filtering, `OBSDATA-11562`, `#398`), and `IndexTaskUtils.java` (lookup-enrichment try-catch wrapping, `#399`). These are Confluent-specific additions layered on top of stock Druid 34.0.0 streaming ingestion.
