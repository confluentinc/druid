# druid-kafka-indexing-service

`extensions-core/kafka-indexing-service` implements Kafka as a seekable-stream source for Druid's streaming
ingestion framework (`indexing-service`'s `org.apache.druid.indexing.seekablestream.*`). It is loaded by:
- **Overlord**: runs `KafkaSupervisor` (one per datasource/topic), which manages `index_kafka` task lifecycle.
- **Indexer / MiddleManager+Peon**: executes `KafkaIndexTask` (type `index_kafka`), which reads Kafka, builds
  incremental segments via `StreamAppenderatorDriver`, and publishes them.
- **Broker/Router are not part of this module** — realtime queries are disabled in this deployment, so
  `KafkaIndexTask#supportsQueries()` returning `true` is not exercised in the query path documented here.

> **Execution mechanics live in [`Code-deepdive-Claude.MD`](Code-deepdive-Claude.MD)** —
> the supervisor tick, the poll→filter→parse→index loop, and the Confluent header-filter internals, traced at method-body granularity with thread ownership, failure modes and config knobs.
> This file is the class map; that one is the runtime behaviour.

## Position in the ingestion path

1. Overlord's `KafkaSupervisor` (extends `SeekableStreamSupervisor`) discovers partitions via
   `KafkaRecordSupplier#getPartitionIds`, computes lag, and launches `KafkaIndexTask`s per task group.
2. Each `KafkaIndexTask` (Indexer/Peon) runs `KafkaIndexTaskRunner` (extends `SeekableStreamIndexTaskRunner`):
   `KafkaRecordSupplier#poll` → optional `KafkaHeaderBasedFilterEvaluator` filter → `StreamChunkParser#parse`
   (using `KafkaInputFormat`/`KafkaInputReader`) → rows added to `StreamAppenderatorDriver`.
3. On reaching configured end offsets (or time-based rollover), the runner calls
   `SeekableStreamIndexTaskRunner#publishAndRegisterHandoff`, which drives `StreamAppenderatorDriver#publish`
   using a `SequenceMetadata`-created `TransactionalSegmentPublisher`. This issues a
   `SegmentTransactionalInsertAction` that **atomically** (a) inserts segment metadata / pushes segments to deep
   storage (S3) and (b) commits the new `KafkaDataSourceMetadata` (offsets) to the metadata store, in one
   transaction driven from `indexing-service`.
4. Coordinator later loads the pushed segments onto Historicals (outside this module); only after the
   transactional commit above do the offsets show as "processed" — Druid does **not** use Kafka consumer-group
   commits for this.
5. Placement propagates to Broker via the Coordinator/segment-metadata mechanism (outside this module).

## Key classes by responsibility

### Supervisor
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisor.java:85` — extends
  `SeekableStreamSupervisor<KafkaTopicPartition, Long, KafkaRecordEntity>`. Implements all abstract hooks:
  - `:128 setupRecordSupplier()` — builds `KafkaRecordSupplier`, passing `getheaderBasedFilterConfig()`.
  - `:140 getTaskGroupIdForPartition()` — `partition % taskCount`, or for multi-topic,
    `abs(31*topic.hashCode()+partition) % taskCount`.
  - `:196 createTaskIoConfig()` — builds `KafkaIndexTaskIOConfig` with `SeekableStreamStartSequenceNumbers`/
    `SeekableStreamEndSequenceNumbers`, propagating `headerBasedFilterConfig`.
  - `:229 createIndexTasks()` — instantiates N (`replicas`) `KafkaIndexTask`s per task group, stamping
    `CHECKPOINTS_CTX_KEY` (`:87 CHECKPOINTS_TYPE_REF`) into task context.
  - `:339 createDataSourceMetaDataForReset()`, `:345 makeSequenceNumber()` (→ `KafkaSequenceNumber`),
    `:351/357 getNotSetMarker()/getEndOfPartitionMarker()` (`-1L` / `Long.MAX_VALUE`).
  - `:396 updatePartitionTimeAndRecordLagFromStream()` / `:496 updatePartitionLagFromStream()` — lag computation;
    dispatches to time+record lag variant if `ioConfig.isEmitTimeLagMetrics()`.
  - `:575 getOffsetsFromMetadataStorage()` — Confluent/upstream logic to reconcile stored offsets across
    single-topic ↔ multi-topic (regex) supervisor-config changes via `:618 getMatchingKafkaTopicPartition()`.
- `src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisorSpec.java` — top-level supervisor
  spec (`type: "kafka"`), wraps `KafkaSupervisorIOConfig` + `KafkaSupervisorTuningConfig`.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisorIOConfig.java:40` — extends
  `SeekableStreamSupervisorIOConfig`; holds `topic`/`topicPattern` (`:207 checkTopicArguments` enforces exactly
  one), `consumerProperties` (must include `bootstrap.servers`, `:107`), `pollTimeout` (default 100ms, `:47`),
  `emitTimeLagMetrics` (`:56/168`), and `:57/175 headerBasedFilterConfig`.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisorTuningConfig.java:33` — extends
  `KafkaIndexTaskTuningConfig` and implements `SeekableStreamSupervisorTuningConfig`; adds `workerThreads`,
  `chatRetries`, `httpTimeout`, `shutdownTimeout`, `offsetFetchPeriod`; `:212 convertToTaskTuningConfig()`
  produces the per-task tuning config.
- `src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisorReportPayload.java` — `/status`
  report DTO (lag, latest offsets, etc.), built by `KafkaSupervisor#createReportPayload`.
- `src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisorIngestionSpec.java` — combines
  data schema + IO/tuning config used inside `KafkaSupervisorSpec`.

### Task & runner
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTask.java:46` — extends
  `SeekableStreamIndexTask<KafkaTopicPartition, Long, KafkaRecordEntity>`, `type = "index_kafka"` (`:48`).
  - `:99 createTaskRunner()` → `KafkaIndexTaskRunner`.
  - `:110 newTaskRecordSupplier()` — builds task-local `KafkaRecordSupplier`, force-overrides
    `auto.offset.reset=none` (`:118`), passes `headerBasedFilterConfig` (`:126`).
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskRunner.java:60` — extends
  `SeekableStreamIndexTaskRunner<KafkaTopicPartition, Long, KafkaRecordEntity>`.
  - `:80 getNextStartOffset()` → `sequenceNumber + 1` (Kafka offsets are exclusive-of-consumed on restart).
  - `:87 getRecords()` — calls `recordSupplier.poll(pollTimeout)`; on `OffsetOutOfRangeException` calls
    `:121 possiblyResetOffsetsOrWait()` (auto-reset via `tuningConfig.isResetOffsetAutomatically()`, else
    retries after `task.getPollRetryMs()` = 30000ms default).
  - `:108 deserializePartitionsFromMetadata()` → `SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long>`.
  - `:180 createDataSourceMetadata()` → `new KafkaDataSourceMetadata(partitions)`.
  - `:188 createSequenceNumber()` → `KafkaSequenceNumber.of(...)`.
  - `:204 isEndOffsetExclusive()` returns `true` (Kafka end offsets are exclusive, unlike Kinesis).
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskIOConfig.java:38` — extends
  `SeekableStreamIndexTaskIOConfig`; carries `consumerProperties`, `pollTimeout`, `configOverrides`,
  `multiTopic` and `:43/196 headerBasedFilterConfig` down to the task.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskTuningConfig.java:33` — extends
  `SeekableStreamIndexTaskTuningConfig`; `resetOffsetAutomatically` and `intermediateHandoffPeriod` (rollover)
  fields at `:49/51` (constructor) drive gotchas below.

### Kafka consumer plumbing
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaRecordSupplier.java:67` — implements
  `RecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>`, wraps a `KafkaConsumer<byte[], byte[]>`.
  - `:72` static block force-sets `org.apache.kafka.sasl.oauthbearer.allowed.urls=notallowed` unless already set
    (CVE-2025-27817 mitigation).
  - `:137 assign()` — only a single logical `stream` (topic or topic-pattern) may be assigned at once
    (`InvalidInput` otherwise); tracks `stream` for later `getAssignment()`.
  - `:198 poll()` — the poll loop; for each `ConsumerRecord`, if `headerFilterEvaluator != null` and
    `!shouldIncludeRecord(record)`, emits an `OrderedPartitionableRecord` with **empty data list and
    `filtered=true`** (offset still advances, no row emitted) instead of a `KafkaRecordEntity`-wrapped record.
  - `:282 getPartitionIds()` — multi-topic mode compiles the configured stream string as a `Pattern` and matches
    against `consumer.listTopics()`; single-topic mode uses `consumer.partitionsFor(stream)`.
  - `:404 getKafkaConsumer()` — merges `KafkaConsumerConfigs.getConsumerProperties()` (forced) over
    user-supplied `consumerProperties` (via `configOverrides.overrideConfigs`), then sets
    `isolation.level=read_committed` and `group.id=kafka-supervisor-<random>` only `putIfAbsent` (defaults, can
    be overridden by user config), and finally `props.putAll(consumerConfigs)` (the *forced* configs always win
    last).
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaConsumerConfigs.java:31` — the exact forced settings:
  `metadata.max.age.ms=10000`, `auto.offset.reset=none`, **`enable.auto.commit=false`**. These are applied last
  in `getKafkaConsumer()` so they cannot be overridden by user `consumerProperties`.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafka/KafkaTopicPartition.java:50` — partition id type used
  throughout; carries optional `topic` (needed because multi-topic mode can have the same partition number
  across different topics) and an `isMultiTopicPartition()`/backward-compat serialization flag.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafka/KafkaRecordEntity.java:38` — `extends ByteEntity implements
  KafkaEntity`; wraps a Kafka `ConsumerRecord` as the row-data unit consumed by `KafkaInputFormat`.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaConsumerMonitor.java:39` — `AbstractMonitor` registered
  via `toolbox.getMonitorScheduler().addMonitor(recordSupplier.monitor())` in
  `KafkaIndexTask#newTaskRecordSupplier`; emits per-consumer-metric-name events (`:54` static `METRICS` map)
  each `doMonitor()` cycle (`:139`), diffing cumulative counters (`kafka/consumer/*`).
- `src/main/java/org/apache/druid/indexing/kafka/KafkaConsumerMetric.java` — declares which raw Kafka client
  metrics map to which Druid metric names/dimensions, consumed by `KafkaConsumerMonitor`.

### Offsets & exactly-once state
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaDataSourceMetadata.java:38` — the datasource metadata
  persisted to Druid's metadata store as the durable "how far have we ingested" state; extends
  `SeekableStreamDataSourceMetadata<KafkaTopicPartition, Long>`. Constructor normalizes any
  `SeekableStreamSequenceNumbers` into `KafkaSeekableStreamStartSequenceNumbers` or
  `KafkaSeekableStreamEndSequenceNumbers`. `:102 matches()` implements the reconciliation logic used when
  checking that a task's committed offsets are consistent with previously stored metadata (including
  multi-topic merges). **This, not Kafka consumer-group offsets, is the source of truth for resumption.**
- `src/main/java/org/apache/druid/indexing/kafka/KafkaSeekableStreamStartSequenceNumbers.java` /
  `KafkaSeekableStreamEndSequenceNumbers.java` — Kafka-specific subclasses of the generic
  `SeekableStreamStartSequenceNumbers`/`SeekableStreamEndSequenceNumbers` adding topic-set bookkeeping needed
  for multi-topic mode.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaSequenceNumber.java:28` — `extends
  OrderedSequenceNumber<Long>`; plain numeric offset comparison (no special end-of-shard semantics, unlike
  Kinesis sequence numbers).
- Atomic publish+commit itself lives in `indexing-service` (see Cross-module pointers) — this module only
  supplies the Kafka-specific `DataSourceMetadata`/sequence-number types plugged into that machinery.

### Input format & parsing (`org.apache.druid.data.input.kafkainput`)
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputFormat.java:41` — blends **key + value + headers + Kafka record timestamp/topic** into one row.
  Uses a `dummyTimestampSpec` (`:52`, `__kif_auto_timestamp`) so value/key/header sub-parsers don't choke on a
  missing timestamp column; `createReader()` (`:88`) builds a `KafkaInputReader`, wiring optional header/key
  sub-readers. Default column names: `kafka.header.*` prefix, `kafka.key`, `kafka.timestamp`, `kafka.topic`.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/data/input/kafkainput/KafkaInputReader.java:49` — actual blending logic:
  - `:117 extractHeader()` — reads headers via the supplied `KafkaHeaderReader`, then `putIfAbsent`s the Kafka
    record timestamp/topic columns (header/value fields win if same key exists).
  - `:138 extractHeaderAndKeys()` — additionally parses the record key with the key `InputFormat` and takes
    only the **first row's first dimension value** as the key column (`:347 getFirstValue`).
  - `:90 read()` — tombstone handling: if `record.getRecord().value() == null`, emits a synthetic row from
    header/key data only (no value payload) instead of calling the value parser.
  - `:160 buildBlendedRows()` — merges value-parsed row fields with header/key map, value fields take priority
    (`buildBlendedEventMap`, `:283`).
- `KafkaHeaderFormat.java` / `KafkaHeaderReader.java` — pluggable interfaces for header decoding.
- `KafkaStringHeaderFormat.java` / `KafkaStringHeaderReader.java` — concrete implementation decoding all header
  values as strings (with configurable encoding), the only header format shipped.

### Confluent header filtering (fork-specific)
Pre-ingestion filtering of Kafka records **by message header value**, evaluated in the consumer poll loop
before any deserialization/row-building work, so filtered-out records never reach `KafkaInputFormat`.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaHeaderBasedFilterConfig.java:39` — the
  supervisor/task-spec-facing config: `filter` (a Druid `DimFilter`, currently only `InDimFilter` allowed —
  `:41 SUPPORTED_FILTER_TYPES` / `:76 validateSupportedFilter`), `encoding` (default UTF-8), and
  `stringDecodingCacheSize` (default `10_000`). Registered as Jackson subtype `"kafka"` via
  `KafkaIndexTaskModule` (`:56`).
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/HeaderFilterHandler.java:28` — interface: `getHeaderName()`,
  `shouldInclude(String headerValue)`, `getDescription()`.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/HeaderFilterHandlerFactory.java:46` — `forFilter(Filter)`
  factory; today only dispatches to `InDimFilterHandler`, throws `IllegalArgumentException` otherwise.
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/InDimFilterHandler.java:37` — copies `InDimFilter`'s values
  into a `HashSet` (`:54`) for O(1) membership checks (filter's own `TreeSet` is O(log n)).
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaHeaderBasedFilterEvaluator.java:39` — the evaluator
  instantiated per `KafkaRecordSupplier`:
  - `:54` ctor builds the `HeaderFilterHandler` from `config.getFilter().toFilter()`, plus a Caffeine
    `stringDecodingCache` (`ByteBuffer` → decoded `String`, bounded by `stringDecodingCacheSize`) to avoid
    repeated charset decoding of repeated header byte values.
  - `:78 shouldIncludeRecord(ConsumerRecord)` — top-level entry point called from `KafkaRecordSupplier.poll()`;
    any exception during evaluation is caught and defaults to **including** the record (fail-open, `:91`).
  - `:105 evaluateInclusion(Headers)` — **permissive/fail-open semantics throughout**: null `Headers`, missing
    header, null header value, or failed decode all result in inclusion; only a *successfully decoded* header
    value that fails `filterHandler.shouldInclude()` is excluded.
- Config threading: `KafkaHeaderBasedFilterConfig` flows
  `KafkaSupervisorIOConfig.headerBasedFilterConfig` (`:57/175`) →
  `KafkaSupervisor#createTaskIoConfig` (`:225`) → `KafkaIndexTaskIOConfig.headerBasedFilterConfig` (`:43/196`)
  → `KafkaIndexTask#newTaskRecordSupplier` (`:126`) → `KafkaRecordSupplier` ctor (`:105-134`) →
  `KafkaHeaderBasedFilterEvaluator`. Also flows directly `KafkaSupervisorIOConfig` → `KafkaSupervisor#setupRecordSupplier` (`:135`) for the supervisor's own metadata-fetching consumer.
- Offset-accounting for filtered records: `KafkaRecordSupplier.poll()` (`:206-215`) still emits an
  `OrderedPartitionableRecord` for a filtered message (empty data, `filtered=true`), so the base
  `SeekableStreamIndexTaskRunner` still advances/commits the offset for it (see
  `indexing-service/.../common/OrderedPartitionableRecord.java:45/123 filtered/isFiltered`, and
  `indexing-service/.../StreamChunkParser.java:114-134` which returns zero rows for `isFiltered=true` while
  tracking it as "filtered" rather than a parse failure/"thrownAway").

### Module registration & sampling
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskModule.java:36` — `DruidModule` registering
  Jackson subtypes: `index_kafka` → `KafkaIndexTask` (`:46`), `"kafka"` → `KafkaDataSourceMetadata`,
  `KafkaIndexTaskIOConfig`, `KafkaSupervisorTuningConfig`, `KafkaSupervisorSpec`, `KafkaSamplerSpec`,
  `KafkaInputFormat`, `KafkaHeaderBasedFilterConfig`; `KafkaTuningConfig` (legacy name) →
  `KafkaIndexTaskTuningConfig` (`:51`, kept for backward compat). Also registers a custom key serializer for
  `KafkaTopicPartition` (`:58`).
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaSamplerSpec.java:41` — `extends
  SeekableStreamSamplerSpec`; used by the web-console/API "sample data" endpoint to preview parsed rows for a
  given `KafkaSupervisorSpec` without launching real tasks (spins up a `KafkaRecordSupplier` directly).
- `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskClientFactory.java:32` — `extends
  SeekableStreamIndexTaskClientFactory<KafkaTopicPartition, Long>`; builds the HTTP client the Overlord's
  `KafkaSupervisor` uses to talk to running `KafkaIndexTask`s (pause/resume/status/checkpoint chat API, in
  `indexing-service`).

## Critical flows

1. **Supervisor → task creation & partition assignment**
   `KafkaSupervisor` (inherited `SeekableStreamSupervisor#runInternal`, `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:1710`)
   → `#discoverTasks` (`:2061`) to reconcile already-running tasks
   → `#checkTaskDuration` (`:3280`) to roll tasks needing new sequences
   → `#createNewTasks` (`:3809`), which for each new task group calls
   `KafkaSupervisor#createTaskIoConfig` then `KafkaSupervisor#createIndexTasks`
   (partition → task-group mapping via `KafkaSupervisor#getTaskGroupIdForPartition`), submitting resulting
   `KafkaIndexTask`s to the `TaskQueue`.

2. **Runner poll → parse → filter → index loop** (per task, `SeekableStreamIndexTaskRunner#runInternal`,
   `indexing-service/.../SeekableStreamIndexTaskRunner.java:~650` loop)
   `KafkaIndexTaskRunner#getRecords` → `KafkaRecordSupplier#poll` (consumer.poll, applies
   `KafkaHeaderBasedFilterEvaluator#shouldIncludeRecord` per record) → for each
   `OrderedPartitionableRecord`, `verifyRecordInRange` → `StreamChunkParser#parse(data, isEndOfShard,
   record.isFiltered())` (empty rows if filtered) → for each `InputRow`,
   `StreamAppenderatorDriver#add(row, sequenceName, committerSupplier, ...)`.

3. **Reaching end offsets → publish → offset commit**
   Runner detects a sequence's partitions have all hit their configured end offset →
   `SeekableStreamIndexTaskRunner#publishAndRegisterHandoff` → `SequenceMetadata#createPublisher` builds a
   `TransactionalSegmentPublisher` whose `publishAnnotatedSegments`
   (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SequenceMetadata.java:352`) issues
   `SegmentTransactionalInsertAction` (`indexing-service/.../actions/SegmentTransactionalInsertAction.java`) —
   this single metadata-store transaction both records the new segments (already pushed to S3 by the
   `StreamAppenderator`) and updates `KafkaDataSourceMetadata` (the committed offsets). Only after that
   transaction succeeds does `driver.registerHandoff` request the Coordinator/Historical handoff; the
   supervisor's next `getOffsetsFromMetadataStorage` call sees the advanced offsets.

4. **Header-based filtering decision path**
   `KafkaRecordSupplier#poll` calls `KafkaHeaderBasedFilterEvaluator#shouldIncludeRecord` →
   `#evaluateInclusion(record.headers())` → `headers.lastHeader(headerName)` (permissive default `true` if
   headers/header/value null) → `#getDecodedHeaderValue` (Caffeine-cached charset decode, permissive `true` on
   decode failure) → `InDimFilterHandler#shouldInclude` (HashSet membership check against the `in`-filter's
   values). `false` ⇒ `KafkaRecordSupplier.poll` emits a `filtered=true`, dataless
   `OrderedPartitionableRecord` (offset still advances; no row reaches `KafkaInputFormat`/`RowIngestionMeters`
   as a normal processed row).

## Configuration reference

Supervisor spec `ioConfig` (`KafkaSupervisorIOConfig`):
- `topic` / `topicPattern` — exactly one required (`:60-83`, `checkTopicArguments`); `topicPattern` enables
  multi-topic mode (`isMultiTopic()`, regex matched against `consumer.listTopics()`).
- `consumerProperties` — must include `bootstrap.servers`; merged with forced settings, see
  `KafkaConsumerConfigs`/`KafkaRecordSupplier.getKafkaConsumer`.
- `useEarliestOffset` — maps to base class `isUseEarliestSequenceNumber()`/`isUseEarliestOffset()` (`:148`).
- `pollTimeout` — `KafkaRecordSupplier.poll(timeout)`, default 100ms (`DEFAULT_POLL_TIMEOUT_MILLIS`, `:47`).
- `emitTimeLagMetrics` — toggles `KafkaSupervisor#updatePartitionTimeAndRecordLagFromStream` vs the cheaper
  offset-only lag path.
- `configOverrides` — `KafkaConfigOverrides`, applied before forced/default consumer configs.
- `headerBasedFilterConfig` — Confluent-fork field; see `KafkaHeaderBasedFilterConfig` above. Nested fields:
  `filter` (must be `in`), `encoding` (default UTF-8), `stringDecodingCacheSize` (default 10000).
- `idleConfig`, `stopTaskCount`, `autoScalerConfig`, `lagAggregator` — inherited
  `SeekableStreamSupervisorIOConfig` autoscaling/idle knobs (indexing-service).

Tuning config (`KafkaSupervisorTuningConfig` extends `KafkaIndexTaskTuningConfig`):
- `resetOffsetAutomatically` — drives `KafkaIndexTaskRunner#possiblyResetOffsetsOrWait` behavior on
  `OffsetOutOfRangeException`.
- `intermediateHandoffPeriod` — task-duration-independent forced segment rollover/handoff interval (inherited
  `SeekableStreamIndexTaskTuningConfig`).
- `workerThreads`, `chatRetries`, `httpTimeout`, `shutdownTimeout`, `offsetFetchPeriod` — supervisor↔task chat
  client tuning (`KafkaIndexTaskClientFactory`).
- `taskDuration`, `taskCount`, `replicas` — live on `KafkaSupervisorIOConfig` (inherited), consumed by
  `KafkaSupervisor#createReportPayload`/`#createNewTasks`.

## Cross-module pointers

- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java`
  — base supervisor run loop (`runInternal`, `discoverTasks`, `checkTaskDuration`, `createNewTasks`) and the
  full abstract-method contract `KafkaSupervisor` implements (`:4200-4747`).
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTask.java` —
  base task class; owns `newDriver`, `getInputSourceResources`, task lock granularity.
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java`
  — base runner: `runInternal` main loop, `publishAndRegisterHandoff`, `initializeSequences`,
  `maybePersistAndPublishSequences`.
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SequenceMetadata.java` — builds the
  `TransactionalSegmentPublisher` and issues `SegmentTransactionalInsertAction`.
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/StreamChunkParser.java` — turns raw
  `ByteEntity` records into `InputRow`s via `InputFormat`/`InputRowParser`, honors the `isFiltered` flag.
- `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentTransactionalInsertAction.java`
  — the atomic segment-metadata-insert + datasource-metadata-commit action.
- `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java` and
  `StreamAppenderatorDriver.java` — in-task incremental segment building, persistence, and deep-storage push
  (S3) prior to the transactional publish above.

## Confluent fork deltas

Per `git log --oneline -30 -- extensions-core/kafka-indexing-service/` and
`git diff --stat upstream/34.0.0 -- extensions-core/kafka-indexing-service/` (upstream 34.0.0 tag exists as
`upstream/34.0.0`), on top of stock Apache Druid 34.0.0 this fork adds:
- **Kafka header-based pre-ingestion filtering** (commits `96fbe44a4f`, `700fe0e6f3`, `25b1b50679`,
  OBSDATA-11562): new files `HeaderFilterHandler`, `HeaderFilterHandlerFactory`, `InDimFilterHandler`,
  `KafkaHeaderBasedFilterEvaluator`, `supervisor/KafkaHeaderBasedFilterConfig`, plus threading of
  `headerBasedFilterConfig` through `KafkaSupervisorIOConfig`, `KafkaIndexTaskIOConfig`,
  `KafkaRecordSupplier`, `KafkaIndexTaskModule`, and the `filtered`/`isFiltered` flag added to
  `OrderedPartitionableRecord` (indexing-service) and honored in `StreamChunkParser`.
- Config renames (`5c8cfa6bee`, `62bd4c0d16`): `headerBasedFilterConfig` was briefly renamed to
  `headerBasedInclusionConfig` then reverted back.
- `KafkaRecordEntity.java` modified (+9/-? lines) and `pom.xml` modified — part of `685a7df248` "Druid-34
  Confluent patches" bulk-apply commit, not further isolated here.
- All other upstream 34.0.0 history (embedded-cluster tests, Kafka client 3.9.1 bump/CVE-2025-27817 mitigation,
  multi-supervisor-same-datasource support, time-lag metrics, etc.) is unmodified stock Druid — those commits
  predate/are shared with `upstream/34.0.0`.

## Gotchas

- **`enable.auto.commit=false` is forced** (`KafkaConsumerConfigs:36`) and applied *last* in
  `KafkaRecordSupplier.getKafkaConsumer` — user `consumerProperties` cannot re-enable Kafka auto-commit.
  Druid never relies on the Kafka consumer group's committed offsets; `KafkaDataSourceMetadata` in the
  metadata store is the only durable offset state.
- `auto.offset.reset=none` is forced both as a default (`KafkaConsumerConfigs:35`) and explicitly re-set in
  `KafkaIndexTask#newTaskRecordSupplier` (`:118`) — Druid always seeks explicitly rather than relying on
  Kafka's reset policy; `useEarliestOffset` only affects the *first-ever* offset chosen by the supervisor, not
  ongoing consumer behavior.
- `KafkaIndexTaskRunner#isEndOffsetExclusive()` returns `true` for Kafka (unlike some other seekable streams),
  and `getNextStartOffset` is simply `sequenceNumber + 1`.
- `resetOffsetAutomatically` only kicks in on `OffsetOutOfRangeException` (data expired/compacted out from
  under a lagging task); otherwise the runner just retries every `pollRetryMs` (30s default,
  `KafkaIndexTask.pollRetryMs`) waiting for data to appear.
- Multi-topic ingestion (`topicPattern`) changes `KafkaTopicPartition` semantics (topic becomes part of the
  partition key) and affects task-group assignment hashing (`KafkaSupervisor#getTaskGroupIdForPartition`); a
  supervisor spec transition from single-topic to multi-topic (or back) is explicitly handled by
  `KafkaSupervisor#getOffsetsFromMetadataStorage`/`getMatchingKafkaTopicPartition` to avoid losing/duplicating
  previously stored offsets.
- Header-filtering is **fail-open**: any missing header, undecodable bytes, or evaluator exception results in
  the record being *included*, not dropped — filtering only excludes records with a successfully decoded
  header value that doesn't match the `in`-filter.
- Filtered records still advance the committed offset (they're not silently skipped/retried) — see
  `OrderedPartitionableRecord.filtered` handling above; this is intentional so filtering doesn't stall
  supervisor lag/checkpointing.
- Time-lag metrics (`emitTimeLagMetrics`) are comparatively expensive: `updatePartitionTimeAndRecordLagFromStream`
  performs extra seeks/polls per refresh cycle (`KafkaSupervisor:396-458`) versus the plain offset-lag path.
- Task/segment rollover is controlled by both `taskDuration` (supervisor IO config) and
  `intermediateHandoffPeriod` (tuning config); either can trigger a new task generation independent of end
  offsets being reached.
