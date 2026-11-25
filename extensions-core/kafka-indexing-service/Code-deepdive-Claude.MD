# Code deep dive — druid-kafka-indexing-service

Scope: execution mechanics of the Kafka streaming path in this fork — what runs on which thread,
what state each call mutates, and where each failure is actually caught. Covers the supervisor tick,
the task poll→filter→parse→index→publish loop, and the Confluent header-filter path. Class inventory
and "what does X do" live in `extensions-core/kafka-indexing-service/CLAUDE.md`; read that first —
this document deliberately does not repeat it. Realtime queries are disabled in this deployment, so
`KafkaIndexTask#supportsQueries()`
(`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTask.java:175`)
is not exercised. All paths are repo-root-relative; `KIS/` below abbreviates
`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/` and `IS/` abbreviates
`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/`.

## Happy path, traced

### A. Supervisor start (Overlord, `SupervisorManager` thread)

1. `SeekableStreamSupervisor#tryInit` builds the supervisor's single consumer:
   `recordSupplier = setupRecordSupplier()` (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:1266`) →
   `KafkaSupervisor#setupRecordSupplier` (`KIS/indexing/kafka/supervisor/KafkaSupervisor.java:128-137`) →
   `new KafkaRecordSupplier(consumerProperties, sortingMapper, configOverrides, isMultiTopic, headerBasedFilterConfig)`.
   Inside `KafkaRecordSupplier#getKafkaConsumer` (`KIS/indexing/kafka/KafkaRecordSupplier.java:404-438`):
   user props are copied (`:418-422`), `isolation.level=read_committed` and a random
   `group.id=kafka-supervisor-<id>` are applied with `putIfAbsent` (`:423-424`), then
   `props.putAll(KafkaConsumerConfigs.getConsumerProperties())` **last** (`:425`) so
   `metadata.max.age.ms=10000`, `auto.offset.reset=none`, `enable.auto.commit=false`
   (`KIS/indexing/kafka/KafkaConsumerConfigs.java:34-36`) cannot be overridden. The TCCL is swapped to
   the extension classloader for consumer construction (`:427-436`). State mutated: `this.consumer`,
   `this.monitor`, `this.headerFilterEvaluator` (`:129-133`). Returns a `KafkaRecordSupplier`.
2. Two executors are armed. `exec` (single thread, `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:870`)
   runs the notice loop (`:1268-1304`): `notices.poll(pollTimeout)` where
   `pollTimeout = max(ioConfig.period, 1000ms)` (`:1271`), then `notice.handle()` (`:1280`) inside a
   `catch (Throwable)` that only alerts (`:1291-1296`) — a failing notice never kills the loop.
   `scheduledExec` (`:876`) enqueues a `RunNotice` every `max(period, 1000ms)` starting after
   `startDelay` (`:1306-1311`).
3. `scheduleReporting` (`:4457-4484`) arms `reportingExec` (`:881`):
   `updateCurrentAndLatestOffsets` with **fixed delay** `max(offsetFetchPeriod, 5000ms)` after
   `startDelay + 15000ms` (`:4463-4470`), plus `emitLag` and `emitNoticesQueueSize` at fixed rate =
   `druid.monitoring.emissionPeriod` (default `PT1M`,
   `processing/src/main/java/org/apache/druid/java/util/metrics/DruidMonitorSchedulerConfig.java:34`).

### B. One supervisor tick (notice thread, `runInternal`)

4. `RunNotice#handle` (`:396-405`) throttles: if `now - lastRunTime < 1000ms` it returns without
   running (`:398-402`); otherwise sets `lastRunTime` and calls `runInternal()` (`:404`).
   `RunNotice.hashCode()==0` / `equals`-by-class (`:414-424`) makes the `NoticesQueue` de-duplicate
   pending run notices, so a slow tick cannot cause a backlog of redundant runs.
5. `runInternal` (`:1710-1766`) → `updatePartitionDataFromStream()` (`:2804`). It takes
   `recordSupplierLock` (`:2808`) and calls `recordSupplier.getPartitionIds(ioConfig.getStream())`.
   In `KafkaRecordSupplier#getPartitionIds` (`KIS/indexing/kafka/KafkaRecordSupplier.java:282-313`): multi-topic
   compiles the stream string as a `Pattern` and filters `consumer.listTopics()` (`:287-293`,
   throwing `INVALID_INPUT` if nothing matches, `:294-299`); single-topic calls
   `consumer.partitionsFor(stream)` and throws if null (`:301-307`). Each `PartitionInfo` becomes
   `new KafkaTopicPartition(multiTopic, p.topic(), p.partition())` (`:310`). A thrown exception is
   swallowed into `return false` (`:2812-2817`), which on the first-ever run aborts the tick
   (`:1716-1718`).
6. Still in `updatePartitionDataFromStream`: stored offsets are read via `getOffsetsFromMetadataStorage()`
   (`:2838`) — the Kafka override (`KIS/indexing/kafka/supervisor/KafkaSupervisor.java:576-615`) reconciles
   single-topic ↔ multi-topic keys through `getMatchingKafkaTopicPartition` (`:618-630`). Newly seen
   partitions are appended to `partitionIds` and, if not the first discovery, to
   `subsequentlyDiscoveredPartitions` (`:2895-2901`). Each partition is then mapped to a group via
   `KafkaSupervisor#getTaskGroupIdForPartition` (`KIS/indexing/kafka/supervisor/KafkaSupervisor.java:140-148`):
   `partition % taskCount`, or `Math.abs(31 * topic.hashCode() + partition) % taskCount` in
   multi-topic mode. State mutated: `partitionIds`, `partitionGroups`, `subsequentlyDiscoveredPartitions`.
7. `discoverTasks()` (`:2061`) adopts already-running `index_kafka` tasks
   (`doesTaskMatchSupervisor` compares `supervisorId`, `KafkaSupervisor.java:157-165`), then
   `verifyAndMergeCheckpoints` (`:2424+`) compares each replica's checkpoint map (fetched over the
   chat API) against `retrieveDataSourceMetadata()` (`:2457`). `checkSourceMetadataMatch` is a bare
   `instanceof KafkaDataSourceMetadata` (`KafkaSupervisor.java:151-154`) and a mismatch throws
   `IAE` (`:2460-2465`). Replicas whose earliest checkpoint does not equal the stored offsets are
   added to `tasksToKill` (`:2521-2529`); if every task in a group is killed, the group is dropped and
   its partitions set to `getNotSetMarker()` = `-1L` (`:2549-2558`, marker at
   `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:91`) so the next run re-reads the metadata store.
8. `checkTaskDuration()` (`:3280-3386`) sorts active groups by earliest task start time (`:3296-3300`)
   and, when `earliestTaskStart + taskDuration` is in the past (`:3318`) and
   `numPendingCompletionTaskGroups + numStoppedTasks < maxAllowedStops` (`:3323`), calls
   `checkpointTaskGroup(group, true)` — an HTTP `pause` + `setEndOffsets(finish=true)` against every
   replica. Returned end offsets become the next start offsets: `partitionOffsets.putAll(endOffsets)`
   (`:3362`); the group moves to `pendingCompletionTaskGroups` with
   `completionTimeout = now + ioConfig.completionTimeout` (`:3343-3344`) and is removed from
   `activelyReadingTaskGroups` (`:3384`). If any returned offset is the end-of-partition marker
   (`Long.MAX_VALUE`, `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:92`) the whole set is discarded and replaced with
   `-1L` (`:3347-3367`) — for Kafka this is defensive only, since `isEndOfShard` is hardwired `false`
   (`KafkaSupervisor.java:363-366`).
9. `createNewTasks()` (`:3809-3930`) creates a `TaskGroup` for any group id lacking one (`:3888-3898`),
   computing start offsets via `generateStartingSequencesForPartitionGroup` →
   `getOffsetFromStorageForPartition` (`:3988-4040`): a stored offset is availability-checked with
   `checkOffsetAvailability` → `KafkaRecordSupplier#isOffsetAvailable`
   (`KIS/indexing/kafka/KafkaRecordSupplier.java:253-258`) unless `skipSequenceNumberAvailabilityCheck`; if it is
   gone, `resetOffsetAutomatically` triggers `resetInternal` + `StreamException` (`:3998-4009`),
   otherwise a `StreamException` telling the operator to call the reset API (`:4011-4018`). With no
   stored offset it falls back to `getOffsetFromStreamForPartition` (`:4033`) →
   `getEarliestSequenceNumber`/`getLatestSequenceNumber` under `recordSupplierLock` (`:4076-4093`).
   Note `useEarliestOffset` is overridden to `true` for newly discovered partitions (`:4025-4031`).
10. `createTasksForGroup(groupId, replicas - existing)` (`:4095-4146`) sets every end offset to
    `getEndOfPartitionMarker()` = `Long.MAX_VALUE` (`:4101-4103`) — Kafka tasks are open-ended and are
    stopped by `checkTaskDuration`, not by a precomputed end — then calls
    `KafkaSupervisor#createTaskIoConfig` (`KafkaSupervisor.java:196-227`). That builds
    `KafkaIndexTaskIOConfig` with `SeekableStreamStartSequenceNumbers` (empty exclusive set, `:214`),
    `SeekableStreamEndSequenceNumbers` (`:215`), `useTransaction = true` (`:218`),
    `refreshRejectionPeriodsInMinutes = taskDuration.getStandardMinutes()` (`:224`) and
    `headerBasedFilterConfig` (`:225`).
11. `KafkaSupervisor#createIndexTasks` (`:230-259`) serialises the group's checkpoint map once into the
    task context under `CHECKPOINTS_CTX_KEY` (`:240-242`) and constructs exactly `replicas`
    `KafkaIndexTask`s (`:245-257`), all sharing the **same** context map instance and the **same**
    `KafkaIndexTaskIOConfig` instance, each with `new TaskResource(baseSequenceName, 1)` (`:250`) so
    replicas share an availability group. `createTasksForGroup` then submits each to
    `taskQueue.add(indexTask)` (`:4136`), and if any task was created a follow-up `RunNotice` is
    scheduled 5s later (`:3925-3929`).

### C. Task startup (Peon, task run thread)

12. `KafkaIndexTask#createTaskRunner` (`KIS/indexing/kafka/KafkaIndexTask.java:99-107`) returns a
    `KafkaIndexTaskRunner`. `SeekableStreamIndexTaskRunner#runInternal`
    (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:396`) sets `status = STARTING` (`:399`), builds the
    `RowIngestionMeters` + `ParseExceptionHandler` (`:404-410`), and constructs the **single**
    `StreamChunkParser` for the task (`:413-422`) — inside it, `SettableByteEntityReader` builds one
    `KafkaInputReader` for the whole task life
    (`IS/SettableByteEntityReader.java:55-59` → `KIS/data/input/kafkainput/KafkaInputFormat.java:88-133`).
    `initializeSequences()` (`:424`) restores `sequences`/`endOffsets`; `runThread = Thread.currentThread()`
    (`:429`).
13. The consumer is created inside a try-with-resources (`:456-457`) →
    `KafkaIndexTask#newTaskRecordSupplier` (`KIS/indexing/kafka/KafkaIndexTask.java:110-137`): TCCL swap, copy of
    `consumerProperties` with a redundant `auto.offset.reset=none` (`:118`), then a new
    `KafkaRecordSupplier` carrying `configOverrides`, `multiTopic` and `headerBasedFilterConfig`
    (`:120-127`), and `toolbox.getMonitorScheduler().addMonitor(recordSupplier.monitor())` (`:128-131`).
14. Offsets are restored: with no prior persist, `currOffsets.putAll(sequences.get(0).startOffsets)`
    after asserting they are ≥ the ioConfig start offsets (`:502-510`); otherwise from the persisted
    `METADATA_NEXT_PARTITIONS` via `KafkaIndexTaskRunner#deserializePartitionsFromMetadata`
    (`KIS/indexing/kafka/KafkaIndexTaskRunner.java:108-119`) with stream/partition-set sanity checks
    (`:520-537`). Because `KafkaIndexTaskRunner#isEndOffsetExclusive()` returns `true`
    (`KIS/indexing/kafka/KafkaIndexTaskRunner.java:204-207`), the `lastReadOffsets` pre-seeding block at
    `IS/SeekableStreamIndexTaskRunner.java:565-575` is **skipped** — Kafka never skips a start record.
15. `assignPartitions(recordSupplier)` (`:1234-1260`) filters `currOffsets` by
    `isMoreToReadBeforeReadingRecord` and calls `recordSupplier.assign(assignment)` (`:1258`) →
    `KafkaRecordSupplier#assign` (`KIS/indexing/kafka/KafkaRecordSupplier.java:137-153`): rejects >1 distinct
    stream with `InvalidInput` (`:145-147`), stores `this.stream` (`:148`), then
    `consumer.assign(...)` — **`assign()`, never `subscribe()`**, so there is no consumer-group
    rebalance, no `ConsumerRebalanceListener`, and no coordinator-driven partition movement.
    `possiblyResetDataSourceMetadata` is a **no-op for Kafka**
    (`KIS/indexing/kafka/KafkaIndexTaskRunner.java:194-201`).
    `seekToStartingSequence` (`:1353-1363`) then issues one `consumer.seek(tp, currOffsets[p])` per
    partition (`KIS/indexing/kafka/KafkaRecordSupplier.java:156-162`). `ingestionState = BUILD_SEGMENTS`,
    `status = READING` (`:606-611`).

### D. Read loop (task run thread, one iteration)

16. `possiblyPause()` (`:616`) — on resume, partitions are re-assigned without re-seeking (`:620-621`);
    if the assignment is now empty, `publishOnStop` and `stopRequested` are set (`:623-627`).
17. `checkPublishAndHandoffFailure()` (`:643`) rethrows any completed-but-failed publish/handoff future
    (`:1000-1010`); `maybePersistAndPublishSequences(committerSupplier)` (`:645`) persists and publishes
    any closed sequence (`:1208-1232`).
18. `getRecords(recordSupplier, toolbox)` (`:649-652`) → `KafkaIndexTaskRunner#getRecords`
    (`KIS/indexing/kafka/KafkaIndexTaskRunner.java:87-105`) → `recordSupplier.poll(ioConfig.getPollTimeout())`.
19. `KafkaRecordSupplier#poll` (`KIS/indexing/kafka/KafkaRecordSupplier.java:198-230`):
    - `consumer.poll(Duration.ofMillis(timeout))` (`:202`) with `timeout` = `pollTimeout`
      (default 100ms, `KIS/indexing/kafka/supervisor/KafkaSupervisorIOConfig.java:47,110`). An empty poll returns
      an **empty `ArrayList`** — no sentinel, no exception — so the run loop simply iterates zero
      records and comes back around. Note `poll` is deliberately **not** wrapped in `wrapExceptions`
      (contrast `:158`, `:263`), so a raw `OffsetOutOfRangeException` escapes to step 24.
    - per record: `new KafkaTopicPartition(multiTopic, record.topic(), record.partition())` (`:203`) —
      one allocation per record even in single-topic mode, where the topic is then dropped
      (`KIS/data/input/kafka/KafkaTopicPartition.java:70-77`).
    - **the header filter runs here, before any record wrapping**: `headerFilterEvaluator != null &&
      !headerFilterEvaluator.shouldIncludeRecord(record)` (`:206`). Excluded records become an
      `OrderedPartitionableRecord` with `Collections.emptyList()` data and `filtered=true` (`:208-215`);
      no `KafkaRecordEntity` is ever constructed for them, so the value payload is never touched.
    - accepted records become `record.value() == null ? null : ImmutableList.of(new KafkaRecordEntity(record))`
      with `filtered=false` (`:218-225`). `KafkaRecordEntity` is `super(record.value())`
      (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafka/KafkaRecordEntity.java:44`) → `ByteBuffer.wrap(byte[])` with **no copy**
      (`processing/src/main/java/org/apache/druid/data/input/impl/ByteEntity.java:38-46`). The array is
      the one `ByteArrayDeserializer` produced for this record, not a shared/reused fetch buffer, so
      there is no aliasing hazard across polls; the entity does retain the entire `ConsumerRecord`
      (`:48-51`), keeping key and headers reachable until the batch is parsed.
20. Filter internals — `KafkaHeaderBasedFilterEvaluator#shouldIncludeRecord`
    (`KIS/indexing/kafka/KafkaHeaderBasedFilterEvaluator.java:78-93`) delegates to
    `evaluateInclusion(record.headers())` (`:105-126`) and catches **every** exception, logging a warn
    and returning `true` (`:83-92`). `evaluateInclusion` fails open on: `headers == null` (`:108-110`),
    `headers.lastHeader(headerName) == null` or `header.value() == null` (`:112-117`), and a null
    decode result (`:119-123`). Only a successfully decoded value reaches
    `filterHandler.shouldInclude(headerValue)` (`:125`) →
    `InDimFilterHandler#shouldInclude` = `filterValues.contains(headerValue)`
    (`KIS/indexing/kafka/InDimFilterHandler.java:64-67`), backed by a `HashSet` copy of the filter's
    `SortedSet` taken once in the constructor (`:54`). `headerName` is
    `InDimFilter#getDimension()` (`:58-61`) resolved once in the evaluator constructor (`:61-63`).
    Decoding: `getDecodedHeaderValue` (`:134-144`) wraps the header bytes in a `ByteBuffer` and calls
    `stringDecodingCache.get(key, k -> new String(headerBytes, encoding))`.
21. Back in the run loop, per record: `verifyRecordInRange(partitionId, sequenceNumber)` (`:660`) →
    (`:1999-2020`) throws `ISE` if the offset is below `currOffsets[p]`, returns false if it is at or
    past `endOffsets[p]`. Then `parser.parse(record.getData(), isEndOfShard(...) /* always false for
    Kafka */, record.isFiltered())` (`:671`) →
    `IS/StreamChunkParser.java:114-133`: an empty/null chunk with `isFiltered=true` increments
    **`filtered`** (`:117-119`); with `isFiltered=false` (Kafka tombstone, since `poll` passed `null`
    data and `OrderedPartitionableRecord` normalises null → `ImmutableList.of()`,
    `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/common/OrderedPartitionableRecord.java:72`)
    it increments **`thrownAway`** (`:120-124`).
    Non-empty chunks go to `parseWithInputFormat` (`:167-189`): `incrementProcessedBytes(remaining())`
    (`:174`), `byteEntityReader.setEntity(valueBytes)` (`:175`) — the reader object is reused, only the
    entity pointer swaps — then a `FilteringCloseableInputRowIterator` drains rows, with `ParseException`
    routed to `parseExceptionHandler` (`:184-186`).
22. Row construction — `KafkaInputReader#read` (`KIS/data/input/kafkainput/KafkaInputReader.java:90-102`):
    `extractHeaderAndKeys(record)` (`:138-158`) builds a fresh `HashMap` per record from the header
    reader (`:117-126`), then `putIfAbsent`s the Kafka record timestamp and topic (`:130`, `:133`) —
    a header column literally named `kafka.timestamp`/`kafka.topic` wins over the record's own values
    — then `computeIfAbsent`s the key column from the **first dimension of the first key row only**
    (`:145-149`, `getFirstValue` at `:347-351`). If the value is non-null, `buildBlendedRows`
    (`:160-185`) maps each value row through `buildBlendedEventMap` (`:283-340`), which prefers the
    value row's field and falls back to the header/key map (`:298-302`) — **value wins on collision**;
    header/key keys are unioned into the dimension list (`:169`) and the synthetic
    `__kif_auto_timestamp` is removed from dimensions (`:171`). The real timestamp is parsed here
    (`:173`) from the blended map, not by the value sub-parser, because
    `KafkaInputFormat` handed the value reader a `dummyTimestampSpec`
    (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputFormat.java:52`, `:97-102`).
23. `driver.add(row, sequenceToUse.getSequenceName(), committerSupplier, true, false)` (`:690-698`) —
    incremental persists are suppressed until the whole poll batch is indexed (`:695-697`). On
    `isPushRequired` the sequence is marked for checkpoint (`:703-711`); on `isPersistRequired` an async
    persist is fired whose failure sets `backgroundThreadException` (`:721-741`). After the rows,
    `lastReadOffsets[p] = record.getSequenceNumber()` and
    `currOffsets[p] = getNextStartOffset(sequenceNumber)` = `sequenceNumber + 1`
    (`:746-747`, `KIS/indexing/kafka/KafkaIndexTaskRunner.java:80-83`). **Filtered records reach `:746-747` too** —
    `rows` is empty so nothing is indexed, but the offset advances, which is why filtering does not
    stall lag or checkpointing. Finally `isMoreToReadAfterReadingRecord` (`:752-755`, `:1341-1351`)
    drops finished partitions from the assignment and re-`assign`s the consumer (`:757-761`).
24. Rollover trigger: if `System.currentTimeMillis() > nextCheckpointTime` (`:770`, set from
    `tuningConfig.getIntermediateHandoffPeriod()` at `:1846`) or the row limit was hit, the runner
    `requestPause()`s and submits a `CheckPointDataSourceMetadataAction` (`:786-810`); a `false`
    result is fatal (`:809`).

### E. Publish and offset commit (task run thread + publish executor)

25. When `stillReading` goes false (or stop is requested), the loop's `finally` persists pending data
    (`:820-833`), then `status = PUBLISHING` under `statusLock` (`:835-841`).
26. For each not-yet-publishing sequence, the last one's end offsets are shortened to `currOffsets`
    (`:851-855`) and `publishAndRegisterHandoff(sequenceMetadata)` is called (`:862`).
27. `publishAndRegisterHandoff` (`:1013-1039`): `driver.publish(sequenceMetadata.createPublisher(this,
    toolbox, ioConfig.isUseTransaction()), committer, [sequenceName])` at `:1018-1022`. The publisher
    (`IS/SequenceMetadata.java:352-432`) sanity-checks that the committed metadata equals the
    sequence's end offsets (`:373-379`) and, with `useTransaction=true` (set by
    `KafkaSupervisor#createTaskIoConfig:218`), submits **one** `SegmentTransactionalInsertAction.appendAction(
    segmentsToPush, supervisorId, dataSource, startMetadata, endMetadata, schemaMapping)` (`:411-424`)
    — segment rows and `KafkaDataSourceMetadata` offsets in a single metadata-store transaction. If the
    task produced no segments but did advance, it degrades to `commitMetadataOnlyAction` (`:383-410`),
    and if neither changed it returns OK without touching the store (`:392-398`).
28. **Ordering, verified:** the offsets are durable the moment that action commits. `registerHandoff`
    is only chained onto the publish future's success callback (`:1069-1092`), and the run thread
    blocks on `handOffWaitList` afterwards (`:878-899`) with `handoffConditionTimeout` (default 15min,
    `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskTuningConfig.java:44`) — a handoff timeout is downgraded to an alert,
    not a failure (`:888-895`). Handoff therefore gates only *when the task may exit*, never offset
    durability, and never runs before the commit.
29. The supervisor observes the advance on its next `getOffsetsFromMetadataStorage()`
    (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:576`) or via `checkPendingCompletionTasks`, and `checkTaskDuration`
    (step 8) has already recorded the end offsets as the next start offsets.

## Nuances

- **End offsets are exclusive.** `isEndOffsetExclusive()==true`
  (`KIS/indexing/kafka/KafkaIndexTaskRunner.java:204-207`) has three consequences: the runner reports no exclusive
  start partitions (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:315`), `lastReadOffsets` is not pre-seeded at
  restore time (`:565-575`), and `setEndOffsets` creates the next sequence with an empty
  exclusive-start set (`:1744-1747`). `getNextStartOffset` is plain `+1`
  (`KIS/indexing/kafka/KafkaIndexTaskRunner.java:80-83`); `KafkaSequenceNumber#compareTo` is plain `Long.compareTo`
  (`KIS/indexing/kafka/KafkaSequenceNumber.java:41-46`) constructed with `isExclusive=false` (`:32`).
- **Kafka consumer-group offsets are never used.** `enable.auto.commit=false` is forced and applied
  last (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaConsumerConfigs.java:36` + `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaRecordSupplier.java:425`); no code calls `commitSync`
  or `commitAsync` anywhere in the module. `group.id` is a random per-supplier value (`:424`) and
  exists only to satisfy the client. The only durable offset state is `KafkaDataSourceMetadata`.
- **`auto.offset.reset=none` is deliberate.** Every position is set explicitly by `seek`
  (`:156-162`), `seekToBeginning` (`:167-170`) or `seekToEnd` (`:176-179`). `useEarliestOffset`
  influences only the supervisor's first-ever choice (`IS/supervisor/SeekableStreamSupervisor.java:4024-4038`).
- **What survives a task restart:** the persisted sequence list plus `METADATA_NEXT_PARTITIONS`
  (`:514-520`) — i.e. `currOffsets`. `lastReadOffsets` and the whole in-memory
  `IncrementalIndex` above the last persist do not survive, but re-reading from `currOffsets` is
  idempotent because the publish is transactional on `startMetadata` (`IS/SequenceMetadata.java:412-424`):
  a replayed publish whose start metadata no longer matches the store fails the transaction rather
  than double-appending.
- **Idempotency across replicas** comes from the same mechanism: both replicas of a group compute
  identical start/end metadata, so exactly one transaction wins and the other's publish returns a
  failed `SegmentPublishResult`, surfacing as the `ISE` at `:1024-1028`.
- **Cached vs recomputed.** Cached: `latestSequenceFromStream` and `partitionToTimeLag` (volatile
  fields, `KafkaSupervisor.java:95-96`), refreshed only by `reportingExec`; the
  `HeaderFilterHandler` and its `HashSet` (built once per `KafkaRecordSupplier`); the decoded-header
  Caffeine cache; the `KafkaInputReader` (once per task). Recomputed per record: the
  `KafkaTopicPartition` (`:203`), the `mergedHeaderMap` (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputReader.java:119`), the key
  sub-reader (`KafkaInputFormat.java:109-123` creates a new reader per record when `keyFormat` is set),
  and the `ByteBuffer` cache key (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaHeaderBasedFilterEvaluator.java:137`).
- **Silent fallbacks.** Header filter exceptions → include (`:83-92`). Metadata-store topic mismatch →
  stored offsets ignored with a warn (`KafkaSupervisor.java:598-605`). `updatePartitionDataFromStream`
  failure → `return false`, tick skipped (`:2812-2817`). Timestamp fetch failure in the time-lag path →
  `log.info` and partial map (`KafkaSupervisor.java:484-486`). Handoff timeout → alert only (`:888-895`).
- **Units.** `pollTimeout`, `pollRetryMs`, `handoffConditionTimeout`, `httpTimeout` are milliseconds;
  `taskDuration`/`period`/`startDelay`/`completionTimeout`/`offsetFetchPeriod`/`intermediateHandoffPeriod`
  are Joda `Period`s; `refreshRejectionPeriodsInMinutes` is minutes
  (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:224`); record lag is in offsets, time lag in milliseconds
  (`KafkaSupervisor.java:443-450`).

## Failure modes

| Failure | Where it surfaces (`path:LINE`) | Detection | Blast radius | Recovery / knob |
| --- | --- | --- | --- | --- |
| Offset out of range (retention/compaction ate the offset) | `KIS/indexing/kafka/KafkaIndexTaskRunner.java:95-104` | `OffsetOutOfRangeException` from `consumer.poll` | one task, its partitions | `resetOffsetAutomatically`; else retry every `pollRetryMs` (30s) |
| Stored offset unavailable at task-creation time | `IS/supervisor/SeekableStreamSupervisor.java:3997-4019` | `isOffsetAvailable` false | whole task group; supervisor UNHEALTHY | `resetOffsetAutomatically`, or `POST /reset`; `skipSequenceNumberAvailabilityCheck` |
| Brokers unreachable (supervisor) | `IS/supervisor/SeekableStreamSupervisor.java:2812-2817` | exception from `getPartitionIds` | tick skipped; no new tasks | retries next `period`; first run aborts (`:1716-1718`) |
| Brokers unreachable (task) | `IS/SeekableStreamIndexTaskRunner.java:814-819` | `StreamException` out of `poll`/`assign` | task fails → supervisor recreates | `consumerProperties` timeouts |
| Empty poll / no data | `KIS/indexing/kafka/KafkaRecordSupplier.java:200,229` | empty list, no signal | none; loop spins at `pollTimeout` | `pollTimeout` |
| Value payload unparseable | `IS/StreamChunkParser.java:176-186` | `ParseException` → `parseExceptionHandler` | row dropped, `unparseable` counter | `maxParseExceptions`, `logParseExceptions` |
| Key format not map-based | `KIS/data/input/kafkainput/KafkaInputReader.java:151-155` | `ClassCastException` → `IOException` | task fails | fix `keyFormat` |
| Header filter misconfigured (non-`in` filter) | `KIS/indexing/kafka/supervisor/KafkaHeaderBasedFilterConfig.java:76-82` | `InvalidInput` at spec submit | supervisor never starts | use an `in` filter |
| Header filter unsupported at runtime | `KIS/indexing/kafka/HeaderFilterHandlerFactory.java:52-56` | `IllegalArgumentException` in evaluator ctor | `KafkaRecordSupplier` construction fails | same |
| Header evaluation throws per record | `KIS/indexing/kafka/KafkaHeaderBasedFilterEvaluator.java:83-92` | warn log, returns `true` | over-ingestion, silent | none — fail-open by design |
| Partition added mid-run | `IS/supervisor/SeekableStreamSupervisor.java:2895-2901` | `partitionIds` grows | new group/task; running tasks unaffected | `metadata.max.age.ms` (forced 10s) |
| Partition removed / topic deleted | `IS/supervisor/SeekableStreamSupervisor.java:2878-2886` | no active partitions → `return false` | tick skipped, UNHEALTHY | recreate topic or reset |
| Replica checkpoints disagree with metadata store | `IS/supervisor/SeekableStreamSupervisor.java:2521-2529,2549-2558` | offset comparison | offending replicas killed; group may reset to `-1L` | automatic |
| Publish transaction lost (concurrent replica / stale start metadata) | `IS/SeekableStreamIndexTaskRunner.java:1024-1028` | null `SegmentsAndCommitMetadata` → `ISE` | that task fails; no duplicate data | automatic re-run |
| Handoff timeout | `IS/SeekableStreamIndexTaskRunner.java:888-895` | `TimeoutException` | alert only; task still succeeds | `handoffConditionTimeout` |
| Async persist failure | `IS/SeekableStreamIndexTaskRunner.java:733-737,639-641` | `backgroundThreadException` | task dies next iteration | none |
| Task-group completion timeout | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:3343` | `completionTimeout` elapsed | group's tasks killed, offsets re-read | `completionTimeout` |
| Time-lag path seeks to negative offset | `KafkaSupervisor.java:438-440` | `IllegalArgumentException` → `StreamException` | lag refresh fails, warn only | `emitTimeLagMetrics=false` |

**Offset out of range.** `getRecords` catches only `OffsetOutOfRangeException`
(`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskRunner.java:95`) — this works only because `KafkaRecordSupplier#poll` is the one
method not wrapped in `wrapExceptions` (`:198-230` vs `:158`, `:263`). It logs and calls
`possiblyResetOffsetsOrWait(e.offsetOutOfRangePartitions(), ...)` (`:102`), returning an empty list so
the loop continues. In `possiblyResetOffsetsOrWait` (`:121-177`): if
`tuningConfig.isResetOffsetAutomatically()` is false, nothing is inspected at all — it falls straight
to the else branch and waits up to `task.getPollRetryMs()` (30s, `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTask.java:61`) on
`pollRetryLock`/`isAwaitingRetry`, waking early on pause or stop (`:166-175`). This is the "reading
ahead of the log end" case and is harmless. If auto-reset is on, for each out-of-range partition it
calls `getEarliestSequenceNumber` (three consumer round-trips: `position`, `seekToBeginning`,
`position`, then `seek` back — `KafkaRecordSupplier.java:243-250`), re-seeks to the wanted offset
(`:148`), and only if `leastAvailableOffset > nextOffset` marks `doReset` (`:152-155`). Then
`sendResetRequestAndWait` (`IS/SeekableStreamIndexTaskRunner.java:1420-1460`) submits
`ResetDataSourceMetadataAction` carrying the *unavailable* offsets, emits the alert
"Offsets were reset automatically, potential data duplication or loss" (`:1446-1450`) and pauses the
task. **Data loss is real here** — everything between the wanted offset and the earliest surviving
offset is gone. Duplication is possible if the subsequent reset lands the supervisor earlier than data
already published. `KafkaIndexTaskRunner#possiblyResetDataSourceMetadata` is intentionally empty
(`:194-201`): unlike Kinesis, Kafka handles staleness reactively on poll rather than proactively at
assignment.

**Broker unavailability.** Two very different behaviours. On the supervisor, everything goes through
`recordSupplierLock` and returns `false`/warns, so the supervisor degrades but survives
(`IS/supervisor/SeekableStreamSupervisor.java:2812-2817`, `KafkaSupervisor.java:406-409`,
`:509-512`). On a task, `assign`/`seek`/`getPosition` wrap the client exception in a `StreamException`
(`KafkaRecordSupplier.java:440-448`) which propagates out of the read loop to the catch at
`IS/SeekableStreamIndexTaskRunner.java:814-819`, cancels the publish/handoff futures
(`:937-950`) and fails the task; the supervisor then recreates it from the last committed offsets.
No offsets are lost because nothing was committed.

**Poll timeout.** `pollTimeout` (default 100ms) is only `consumer.poll`'s max block. An expired poll is
indistinguishable from a genuinely empty topic: an empty `ArrayList` (`:200`, `:229`). The loop then
re-runs `checkPublishAndHandoffFailure` and `maybePersistAndPublishSequences` and polls again, so a
low `pollTimeout` on an idle topic costs CPU in those two calls, while a high one delays reaction to
`pause`/`setEndOffsets` by up to `pollTimeout`.

**Deserialization failure.** Note the module only ever deserialises to `byte[]`:
`getKafkaDeserializer` rejects any configured deserializer whose `deserialize` does not return
`byte[]` (`KafkaRecordSupplier.java:383-389`), and a missing class becomes a `StreamException`
(`:391-393`) at consumer construction. Real payload decoding happens later in the `InputFormat`, where
a `ParseException` is funnelled into `parseExceptionHandler` (`IS/StreamChunkParser.java:184-186`) and
counted as `unparseable`; the task fails only once `maxParseExceptions` is exceeded. A tombstone
(`value()==null`) is not a parse failure — `poll` passes `null` data (`:222`), the record normalises to
an empty list (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/common/OrderedPartitionableRecord.java:72`) and `StreamChunkParser:120-124`
increments `thrownAway`.

**Header-filter misconfiguration.** Caught at three depths. (1) Spec submit: `validateSupportedFilter`
uses exact `getClass()` membership in `SUPPORTED_FILTER_TYPES`
(`KafkaHeaderBasedFilterConfig.java:41-43`, `:76-82`), so anything but a literal `InDimFilter`
(including `not(in(...))`) is rejected with `InvalidInput`; the charset is validated the same way
(`:62-66`). (2) Supplier construction: `HeaderFilterHandlerFactory.forFilter` throws
`IllegalArgumentException` for a non-`InDimFilter` (`HeaderFilterHandlerFactory.java:48-56`) —
unreachable given (1) but it means the failure lands in `setupRecordSupplier`/`newTaskRecordSupplier`
rather than in the read loop. (3) Per record: any exception is swallowed and the record is *included*
(`KafkaHeaderBasedFilterEvaluator.java:83-92`). The dangerous misconfiguration is therefore not a
crash but a semantic one — a wrong `dimension` (header name) means `lastHeader` always returns null
and **nothing is ever filtered** (`:112-117`), silently, with no metric distinguishing it from a topic
where every record legitimately matches.

**Partition added or removed mid-task.** Tasks use `assign()` with a fixed partition set
(`KafkaRecordSupplier.java:149-152`), so a running task never sees a new partition — it finishes its
own partitions and exits. The supervisor picks the new partition up on the next tick
(`IS/supervisor/SeekableStreamSupervisor.java:2895-2901`), adds it to
`subsequentlyDiscoveredPartitions` so it starts from earliest regardless of `useEarliestOffset`
(`:4025-4031`), and assigns it a group via `getTaskGroupIdForPartition`. Because that mapping is
`partition % taskCount`, adding partitions does not move existing ones, but changing `taskCount`
reshuffles every partition. Removal is worse: if *all* partitions vanish, `updatePartitionDataFromStream`
returns false (`:2878-2886`) and the supervisor stalls; a partial removal leaves stale entries in
`partitionOffsets`/`partitionGroups` (Kafka's `supportsPartitionExpiration()` is false, so the cleanup
at `:2907-2915` never runs).

**Supervisor/task offset mismatch.** `verifyAndMergeCheckpoints` walks the replicas newest-checkpoint
first (`:2483`) and looks for the earliest checkpoint whose every partition offset exactly equals the
metadata store's (`:2495-2513`). That replica's tail of checkpoints becomes the group's
`checkpointSequences` (`:2514-2520`); replicas that disagree with the store, or later with the chosen
group state (`:2532-2544`), go to `tasksToKill`. If that empties the group, the group is dropped and
every partition set to `-1L` so start offsets are re-read from the store (`:2549-2558`) — the
supervisor's ultimate answer to "I don't know where I am" is always "ask the metadata store".

## Optimizations in the current structure

- **Filter before wrapping and before parsing.** The evaluator runs at
  `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaRecordSupplier.java:206`, i.e. before `new KafkaRecordEntity(record)` at `:222` and long before
  `StreamChunkParser`. Cost: filtering is invisible to `RowIngestionMeters` except as the `filtered`
  counter (`IS/StreamChunkParser.java:117-119`), so there is no `processedBytes` accounting for
  dropped data and no way to tell a mis-typed header name from a fully-matching topic.
- **Caffeine header-decode cache.** Key `ByteBuffer.wrap(headerBytes)`, value the decoded `String`,
  bounded by `maximumSize(stringDecodingCacheSize)` (default 10 000) with Caffeine's default
  Window-TinyLFU size eviction and **no** expiry (`KafkaHeaderBasedFilterEvaluator.java:57-59`,
  `:137-138`). It eliminates the charset decode and `String` allocation for repeated header values.
  Costs: `ByteBuffer.hashCode()` still walks all remaining bytes on every lookup, so per-record cost
  stays O(header length) even on a hit; each retained key pins the header `byte[]` taken from a
  `ConsumerRecord`, so the cache holds up to `stringDecodingCacheSize` such arrays alive; and a
  high-cardinality header (a request id, say) turns the cache into pure overhead — every lookup is a
  miss plus an insert plus an eviction.
- **`HashSet` instead of the filter's `TreeSet`.** `InDimFilterHandler:54` copies
  `InDimFilter#getValues()` (a `SortedSet`,
  `processing/src/main/java/org/apache/druid/query/filter/InDimFilter.java:206`) into a `HashSet`
  once, trading O(log n) comparisons for O(1) hashing. Cost: a second full copy of the value set per
  supplier, and the copy silently drops the filter's `extractionFn` (`processing/src/main/java/org/apache/druid/query/filter/InDimFilter.java:95`),
  which the handler never consults.
- **One `InputEntityReader` per task, entity swapped per record.**
  `IS/SettableByteEntityReader.java:55-59` builds the reader once and `setEntity` just re-points it
  (`:62-65`, called from `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/StreamChunkParser.java:175`). Cost: the reader is stateful and single-
  threaded by construction, and `KafkaInputFormat` still allocates a fresh key sub-reader per record
  when `keyFormat` is set (`KafkaInputFormat.java:109-123`), so the saving applies to the value path only.
- **Lazy blended event map.** `buildBlendedEventMap` returns an `AbstractMap` view rather than copying
  (`KafkaInputReader.java:283-340`), explicitly to keep flattener laziness intact (`:275-282`). Cost:
  `entrySet()` materialises a `LinkedHashSet` of synthetic entries on every call (`:314-337`), so any
  consumer that iterates the map pays more than a plain `HashMap` would have.
- **Zero-copy value entity.** `KafkaRecordEntity` wraps `record.value()` without copying
  (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafka/KafkaRecordEntity.java:44` → `ByteEntity.java:38-46`). Cost: the entity retains the whole
  `ConsumerRecord` (`:48-51`), so an entire poll batch's keys and headers stay reachable until the
  batch finishes indexing.
- **Lag computed lazily from a cached snapshot.** `updatePartitionLagFromStream` only refreshes
  `latestSequenceFromStream` (`KafkaSupervisor.java:519-522`); the subtraction happens in
  `getRecordLagPerPartitionInLatestSequences` (`:291-308`) at report time. Cost: staleness (see below)
  and the negative-lag suppression hack in `emitLag` (`:4637-4649`).
- **Batched incremental persists.** `driver.add(..., false)` defers persists until the poll batch is
  fully indexed (`IS/SeekableStreamIndexTaskRunner.java:690-698`) and persists asynchronously
  (`:721-741`). Cost: a persist failure is only noticed on the next loop iteration via
  `backgroundThreadException` (`:639-641`).
- **RunNotice de-duplication + 1s floor.** `hashCode()==0`/`equals`-by-class (`:414-424`) plus the
  `MAX_RUN_FREQUENCY_MILLIS` guard (`:398-402`) bound supervisor work regardless of how many notices
  are queued. Cost: an explicit "run now" request can be silently coalesced away.

## Concurrency & thread model

Kafka's `KafkaConsumer` is not thread-safe, and the code respects that in two different ways.

**Supervisor (Overlord).** One `KafkaRecordSupplier` is shared by two threads: the single-threaded
notice executor `exec` (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:870`, loop at `:1268-1304`) and
`reportingExec` (`:881`). Every touch of the consumer is bracketed by the `ReentrantLock`
`recordSupplierLock` (`:890`, exposed via `getRecordSupplierLock()` at `:1247`):
`updatePartitionDataFromStream` (`:2808-2820`), `assignRecordSupplierToPartitionIds` (`:2984-3004`),
`getOffsetFromStreamForPartition` (`:4078-4092`), and on the Kafka side
`updatePartitionLagFromStream` (`KafkaSupervisor.java:503-529`) and
`updatePartitionTimeAndRecordLagFromStream` (`:400-457`). The latter holds the lock across up to ten
`poll` calls plus per-partition seeks, so a slow time-lag refresh directly blocks partition discovery
on the notice thread. `workerExec` (`:886`, `ListeningScheduledExecutorService` sized by
`tuningConfig.workerThreads`) runs only chat-client callbacks and never touches the consumer.
`latestSequenceFromStream`/`partitionToTimeLag` are `volatile` (`KafkaSupervisor.java:95-96`) — the
publication mechanism between the reporting thread and the report/emit threads.

**Task (Peon).** The consumer is created and closed inside a single try-with-resources on the run
thread (`IS/SeekableStreamIndexTaskRunner.java:456-457`, `runThread` captured at `:429`) and every
`poll`/`seek`/`assign` happens on that thread — so no lock is needed and none exists in
`KafkaRecordSupplier`. Two exceptions: (a) `KafkaConsumerMonitor#doMonitor` reads
`consumer.metrics()` (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaConsumerMonitor.java:141`) from the `MonitorScheduler`
thread every `emissionPeriod`; `KafkaConsumer.metrics()` is one of the few client methods that does not
acquire the consumer's single-access guard (unverified — kafka-clients 3.9.1 is a binary dependency,
`extensions-core/kafka-indexing-service/pom.xml:78`); (b) `close()` calls `monitor.stopAfterNextEmit()`
before `consumer.close()` (`KafkaRecordSupplier.java:331-332`), which only prevents the *next* cycle —
a monitor cycle already in flight can read a closing consumer (inferred).

**Task locks.** `pauseLock` + `hasPaused`/`shouldResume` (`:189-191`) synchronise the Jetty chat
threads (`pause`, `resume`, `setEndOffsets` at `:1736-1836`) with the run thread's `possiblyPause`
(`:1383-1403`). `statusLock` (`:203`) guards the `stopGracefully` ↔ publish transition (`:835-841`,
`:1505-1524`). `pollRetryLock` + `isAwaitingRetry` (`:205-206`) is the interruptible sleep used by
Kafka's `possiblyResetOffsetsOrWait` (`KafkaIndexTaskRunner.java:166-175`) and signalled by
`stopGracefully` (`:1529-1537`) and `resume` (`:1910-1915`). `currOffsets` is a `ConcurrentHashMap`
(`:171`) because the chat API reads it (`:1581`) while the run thread writes it (`:747`);
`lastReadOffsets` is a plain `HashMap` (`:167`) touched only by the run thread. Publish and handoff
callbacks run on `MoreExecutors.directExecutor()` (`:1033`, `:1091`), i.e. on whichever thread
completed the driver future — which is why `sequences.remove` and `persistSequences` inside
`onSuccess` (`:1056-1067`) are off the run thread and why the run thread snapshots `sequences` before
iterating (`:843-846`).

## Config knobs that change execution

| Property | Class field (`path:LINE`) | Default | What it changes in the code path |
| --- | --- | --- | --- |
| `ioConfig.topic` / `topicPattern` | `KIS/indexing/kafka/supervisor/KafkaSupervisorIOConfig.java:54-55`, validated `:207-220` | one required | `topicPattern` sets `isMultiTopic()` (`:159-162`) → regex branch in `getPartitionIds` (`KafkaRecordSupplier.java:286-299`) and topic-aware `KafkaTopicPartition` keys |
| `ioConfig.consumerProperties` | `KafkaSupervisorIOConfig.java:51,105-109` | `bootstrap.servers` required | copied into the client props (`KafkaRecordSupplier.java:418-422`); always loses to `KafkaConsumerConfigs` (`:425`) |
| `ioConfig.pollTimeout` | `KafkaSupervisorIOConfig.java:47,110` | 100 ms | `consumer.poll(Duration.ofMillis(...))` (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaRecordSupplier.java:202`); also the per-poll budget in the time-lag path (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:468`) |
| `ioConfig.useEarliestOffset` | `KafkaSupervisorIOConfig.java:147-151` | false | only `getOffsetFromStorageForPartition`'s no-stored-offset branch (`SeekableStreamSupervisor.java:4024-4033`) |
| `ioConfig.emitTimeLagMetrics` | `KafkaSupervisorIOConfig.java:56,114,167-171` | false | switches `updatePartitionLagFromStream` to the seek+poll variant (`KafkaSupervisor.java:498-501` → `:396-458`), holding `recordSupplierLock` far longer |
| `ioConfig.configOverrides` | `KafkaSupervisorIOConfig.java:53,111` | null | `configOverrides.overrideConfigs(consumerProperties)` before defaults (`KafkaRecordSupplier.java:413-417`) |
| `ioConfig.headerBasedFilterConfig` | `KafkaSupervisorIOConfig.java:57,104,173-178` | null | null ⇒ `headerFilterEvaluator == null` ⇒ the `:206` branch is never taken and filtering costs nothing |
| `…filter` (must be `in`) | `KafkaHeaderBasedFilterConfig.java:41-43,76-82` | required | `getDimension()` becomes the header name (`InDimFilterHandler.java:58-61`); `getValues()` becomes the `HashSet` (`:54`); `extractionFn` ignored |
| `…encoding` | `KafkaHeaderBasedFilterConfig.java:57,62-66` | `UTF-8` | `Charset` used by `new String(headerBytes, encoding)` (`KafkaHeaderBasedFilterEvaluator.java:56,138`) |
| `…stringDecodingCacheSize` | `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaHeaderBasedFilterConfig.java:58` | 10 000 | `Caffeine.maximumSize(...)` (`KafkaHeaderBasedFilterEvaluator.java:57-59`) |
| `ioConfig.taskCount` | base, used at `KafkaSupervisor.java:142-147` | 1 | divisor in `getTaskGroupIdForPartition`; changing it re-maps every partition |
| `ioConfig.replicas` | base, used at `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:245` | 1 | loop bound in `createIndexTasks`; N tasks share one ioConfig/context |
| `ioConfig.taskDuration` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisorIOConfig.java:97` | `PT1H` | rollover test at `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:3318`; also `refreshRejectionPeriodsInMinutes` (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:224`) |
| `ioConfig.period` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisorIOConfig.java:99` | `PT30S` | `RunNotice` schedule and notice poll timeout (`SeekableStreamSupervisor.java:1271,1309`), floored at 1s |
| `ioConfig.startDelay` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisorIOConfig.java:98` | `PT5S` | first tick and both reporting schedules (`:1305-1311`, `:4463-4483`) |
| `ioConfig.completionTimeout` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisorIOConfig.java:101` | `PT30M` | `group.completionTimeout` (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:3343`) |
| `ioConfig.stopTaskCount` | base → `getMaxAllowedStops()` | = `taskCount` | how many groups `checkTaskDuration` may roll per tick (`:3323`) |
| `tuningConfig.resetOffsetAutomatically` | `IS/SeekableStreamIndexTaskTuningConfig.java:39,119` | false | whether `possiblyResetOffsetsOrWait` resets or waits (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskRunner.java:131`); also the supervisor auto-reset at `:3998` |
| `tuningConfig.intermediateHandoffPeriod` | `KIS/indexing/kafka/KafkaIndexTaskTuningConfig.java:51,101` | `P2147483647D` | `nextCheckpointTime` (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:1846`, tested `:770`) |
| `tuningConfig.handoffConditionTimeout` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskTuningConfig.java:44` | 15 min | timed vs unbounded wait on `handOffWaitList` (`:880-899`) |
| `tuningConfig.offsetFetchPeriod` | `KIS/indexing/kafka/supervisor/KafkaSupervisorTuningConfig.java:40,131-133`; default `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisorTuningConfig.java:29` | `PT30S` | `updateCurrentAndLatestOffsets` fixed delay, floored at 5s (`:4463-4470`); also the staleness window in `emitLag` (`:4637-4640`) |
| `tuningConfig.workerThreads` | `KafkaSupervisorTuningConfig.java:36,124` | null | `workerExec` size for chat callbacks (`:969-975`) |
| `tuningConfig.chatRetries` / `httpTimeout` / `shutdownTimeout` | `KafkaSupervisorTuningConfig.java:37-39,125-130` | 8 / `PT10S` / `PT80S` | supervisor→task HTTP client behaviour |
| `tuningConfig.skipSequenceNumberAvailabilityCheck` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskTuningConfig.java:40` | false | skips `isOffsetAvailable` at task creation (`:3996`) |
| `maxParseExceptions` / `logParseExceptions` | `SeekableStreamIndexTaskTuningConfig.java:134-141` | unlimited / false | `ParseExceptionHandler` built at `SeekableStreamIndexTaskRunner.java:405-410` |
| `druid.monitoring.emissionPeriod` | `processing/src/main/java/org/apache/druid/java/util/metrics/DruidMonitorSchedulerConfig.java:34` | `PT1M` | `KafkaConsumerMonitor#doMonitor` cadence and `emitLag`/`emitNoticesQueueSize` rate (`:4472-4483`) |

`KafkaConsumerMonitor` emits, once per cycle: `kafka/consumer/bytesConsumed`, `recordsConsumed`,
`fetch`, `outgoingBytes`, `incomingBytes` as **deltas** against a per-`MetricName` `AtomicLong`
(`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaConsumerMonitor.java:131`, `:156-162`), and `fetchRate`, `fetchLatencyAvg`, `fetchLatencyMax`,
`fetchSizeAvg`, `fetchSizeMax`, `recordsLag`, `recordsPerRequestAvg` as raw gauges (`:54-127`,
`:151-153`). Only metrics whose Kafka tag set exactly equals the declared dimension set are emitted, so
grand totals are skipped (`:146-149`); `client-id` is dropped from the emitted dimensions (`:167-169`);
`NaN` values are suppressed (`:165`).

## Sharp edges

- **A mistyped header name silently disables filtering.** `headers.lastHeader(headerName)` returning
  null is a fail-open path (`KafkaHeaderBasedFilterEvaluator.java:112-117`) indistinguishable from
  "every record matched". There is no metric or log for it.
- **Fail-open is not uniform.** A *missing* header includes the record (`:115`), but a *present,
  empty* header value (`byte[0]`, which is not null) decodes to `""` and is then tested against the
  filter (`:119-125`) — so it is normally **excluded**. Missing and empty behave oppositely.
- **The "undecodable bytes" fail-open branch is effectively dead.**
  `new String(bytes, charset)` replaces malformed input with U+FFFD rather than throwing, so
  `getDecodedHeaderValue` returning null (`:121-123`, `:140-143`) is essentially unreachable for
  standard charsets — undecodable bytes are compared as replacement characters and therefore *excluded*,
  not included (inferred from `String(byte[], Charset)` semantics; the code path itself is verified).
- **Duplicate header keys: last one wins.** `lastHeader` (`:112`) silently ignores earlier headers with
  the same key.
- **`in` filters with an `extractionFn` are accepted and the `extractionFn` is ignored.**
  `KafkaHeaderBasedFilterConfig` only checks the class (`:78`); `InDimFilterHandler` reads just
  `getDimension()` and `getValues()` (`:58-61`, `:54`).
- **Kafka tombstones never reach `KafkaInputReader`'s tombstone branch.** `poll` sets `data = null`
  for `value() == null` (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaRecordSupplier.java:222`), which becomes an empty list
  (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/common/OrderedPartitionableRecord.java:72`), which `StreamChunkParser` short-circuits as `thrownAway`
  (`:120-124`). The header/key-only row built at `KafkaInputReader.java:99-101` is therefore
  unreachable from the streaming path — matching `KafkaRecordEntity`'s own javadoc
  (`KafkaRecordEntity.java:35-36`) but contradicting the reader's apparent intent.
- **`assign(emptySet)` leaves `this.stream` stale.** The early return at
  `KafkaRecordSupplier.java:139-142` skips the `this.stream = ...` assignment at `:148`, so a later
  `getAssignment()` (`:183-194`) builds `StreamPartition`s from whatever stream was assigned before
  (or `null` if never).
- **The time-lag path can seek to a negative offset.** `recordSupplier.seek(partition, endOffset - 2)`
  (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:439`) is guarded only by `endOffset != 0` (`:438`), so a partition holding
  exactly one record (`endOffset == 1`) seeks to `-1`, which `KafkaConsumer.seek` rejects; the
  `StreamException` aborts the whole lag refresh (`:452-454`).
- **`recordSupplier.assign(...)` is called inside the per-record loop** in
  `getTimestampPerPartitionAtCurrentOffset` (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:476`) rather than after it — one
  re-assign per polled record, each of which discards fetch buffers for de-assigned partitions.
- **Changing `taskCount` re-maps every partition.** `partition % taskCount`
  (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:146`) has no stability property; every partition can move groups, so all
  groups roll over. In multi-topic mode `Math.abs(31 * topic.hashCode() + partition)`
  (`:144`) is additionally negative when the inner sum is `Integer.MIN_VALUE`, yielding a negative
  group id (inferred; astronomically unlikely but unguarded).
- **Replicas share mutable-looking state.** `createIndexTasks` passes the *same* context `Map` and the
  *same* `KafkaIndexTaskIOConfig` object to all N tasks (`KafkaSupervisor.java:241-256`); correctness
  relies on nothing mutating them before serialisation.
- **Filtered records are invisible to lag but visible to `filtered`.** Offsets advance for filtered
  records (`SeekableStreamIndexTaskRunner.java:746-747`) so lag looks healthy, while
  `ingest/events/filtered` is the only signal that data is being dropped — and it shares that counter
  with `transformSpec` filtering.
- **`metadata.max.age.ms` is forced to 10s and cannot be raised** (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaConsumerConfigs.java:34` +
  `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaRecordSupplier.java:425`). With a broad `topicPattern` and many topics this is a recurring
  full metadata refresh on every consumer in the cluster.
- **The OAuthBearer static block runs on class load and mutates a JVM-wide system property**
  (`KafkaRecordSupplier.java:72-78`), so the first touch of this class anywhere in the Peon/Overlord
  JVM sets `org.apache.kafka.sasl.oauthbearer.allowed.urls=notallowed` for every Kafka client in the
  process unless it was already set.
