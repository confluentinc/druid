# druid-processing

`druid-processing` (Maven artifact `org.apache.druid:druid-processing`) provides Druid's segment storage format (write + read) and its query execution engine. It has no server/HTTP layer of its own — it is a library consumed by `indexing-service` (task execution) and `server` (Historical/Broker query serving).

> **Execution mechanics live in [`Code-deepdive-Claude.MD`](Code-deepdive-Claude.MD)** —
> row→IncrementalIndex→persist→merge mechanics and the cursor/filter/vectorization decision path, traced at method-body granularity with thread ownership, failure modes and config knobs.
> This file is the class map; that one is the runtime behaviour.

**Scope of this doc**: this module is large (600+ classes: aggregators, expressions, joins, nested-column/JSON support, sketches glue, etc. are NOT covered here). This document maps *only* the two paths relevant to the Kafka-ingest pipeline:

1. Segment build path — how ingested rows become on-disk/deep-storage segments (used by `index_kafka` tasks).
2. Query execution path — how a query against a `Segment` is executed on a Historical, and how per-segment results are merged (also used by the Broker for merging Historical responses).

Everything else in `processing/` is out of scope and may be undocumented here.

## Where this module is used

**Ingestion (index_kafka path)**:
- `indexing-service`'s `SeekableStreamIndexTask` (Kafka ingest task) and `BatchAppenderators` drive row ingestion via `Appenderator` implementations that live in `server` (`org.apache.druid.segment.realtime.appenderator.StreamAppenderator`, `org.apache.druid.segment.realtime.sink.Sink`), which directly construct and add rows to `OnheapIncrementalIndex` (from this module) and call `IndexMergerV9.persist`/`mergeQueryableIndex` (also this module) to flush and merge segments to local disk before push to deep storage.
- `indexing-service`'s `TaskToolbox`/`TaskToolboxFactory` wire up `IndexIO`, `IndexMergerV9`, and `IndexSpec` for tasks.

**Querying (Router → Broker → Historical path)**:
- `server`'s `ServerManager` (Historical) uses `QueryRunnerFactory`/`QueryToolChest`/`QueryRunner` from this module to build the per-segment execution pipeline over `Segment`/`QueryableIndex` objects it obtains from `SegmentManager`.
- `server`'s `CachingClusteredClient` (Broker) uses `QueryToolChest.mergeResults` / `ResultMergeQueryRunner` and per-query-type merge logic (this module) to combine per-Historical results.
- `server`'s `QueryLifecycle` and `QueryResource` sit above `QuerySegmentWalker` (interface defined here) to route queries.

## Segment build path

### In-memory rows
- `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:111` (abstract) — in-memory row store during ingestion. Key method `add(InputRow)` (`:466`) delegates to abstract `addToFacts(...)` (`:385`).
- `processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:83` — the on-heap implementation actually used by streaming ingestion; overrides `addToFacts` (`:228`). Enforces `maxRowCount` (`:95`, checked at `:413`, rejection reason set at `:425` — "Maximum number of rows [%d] reached") to signal backpressure/persist triggers back to the Appenderator.
- `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndexSchema.java:37` — dimensions/metrics/granularity/**rollup** (`isRollup()` at `:105`) config used to build an `IncrementalIndex`; rollup is decided here, at index-creation time, not at persist time.
- `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndexAddResult.java:26` — return value of `add()`; carries `isRowAdded()` (`:96`), `hasParseException()` (`:85`) — this is the row-limit/parse-exception signalling path the Appenderator inspects to decide when to trigger a persist.
- `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndexAdapter.java:46` (implements `IndexableAdapter`) — adapter that lets `IndexMerger` read rows back out of an `IncrementalIndex` for persisting.

### Persist & merge to disk
- `processing/src/main/java/org/apache/druid/segment/IndexMerger.java:48` (interface) — declares `persist(...)` (`:258`, plus convenience overloads at `:208`, `:229`) and `mergeQueryableIndex(...)` (`:301`, convenience at `:274`).
- `processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:89` — the only production implementation. `persist(IncrementalIndex, Interval, File, IndexSpec, ProgressIndicator, SegmentWriteOutMediumFactory)` (`:1042`) turns an `IncrementalIndex` into an on-disk v9 segment (validates the interval contains min/max timestamps, throws on empty index). `mergeQueryableIndex(...)` (`:1094`) merges multiple `QueryableIndex`es (multiple persisted segments for the same interval) into one, via private `merge(...)` (`:1299`), doing multi-phase column writing (dictionary merge → column value writing → index/bitmap writing).
- `processing/src/main/java/org/apache/druid/segment/IndexIO.java:93` — `CURRENT_VERSION_ID = V9_VERSION` (`:97`); `loadIndex(File)` (`:195`, `:200` for lazy/failcallback variant) memory-maps a persisted segment directory back into a `QueryableIndex`, validating segment version.
- `processing/src/main/java/org/apache/druid/segment/IndexSpec.java:41` — per-segment tuning: bitmap type (Roaring/Concise), compression strategy for dimensions/metrics, long-encoding — set by ingestion task tuningConfig and baked into the persisted files.

### Segment format & identity
- `processing/src/main/java/org/apache/druid/timeline/DataSegment.java:57` (with `Builder` at `:528`) — the deep-storage segment descriptor (interval, version, shardSpec, loadSpec, size, dimensions/metrics) that is pushed to S3 and later loaded by Coordinator/Historicals. **Lives in `processing/`, not `core/`.**
- `processing/src/main/java/org/apache/druid/timeline/SegmentId.java:50` — canonical identity (`dataSource_interval_version_partitionNum`), also in `processing/`.
- `processing/src/main/java/org/apache/druid/timeline/partition/NumberedShardSpec.java:41` — `ShardSpec` implementation for partition-within-interval; streaming (Kafka) ingestion appends use numbered shard specs as new segments are cut per Kafka partition/sequence.
- `processing/src/main/java/org/apache/druid/segment/Segment.java:40` (interface extends `Closeable`) — the runtime handle to queryable segment data (either in-memory or memory-mapped). `processing/src/main/java/org/apache/druid/segment/QueryableIndexSegment.java:36` wraps a persisted/merged `QueryableIndex`; `processing/src/main/java/org/apache/druid/segment/IncrementalIndexSegment.java:32` wraps a live `IncrementalIndex` (realtime queries are disabled in this pipeline, so this is mostly build-time use inside the Appenderator before persist).
- `processing/src/main/java/org/apache/druid/segment/ReferenceCountedSegmentProvider.java:47` (extends `ReferenceCountingCloseableObject<Segment>`) — wraps a `Segment` with refcounting; `acquireReference()` (`:168`) returns an `Optional<Segment>` only if the segment hasn't already been closed (i.e., dropped). This is the mechanism `ServerManager` (in `server`) uses to safely hand out segments to concurrent queries while allowing background drop/swap.

### Timeline structures
- `processing/src/main/java/org/apache/druid/timeline/VersionedIntervalTimeline.java:74` — `VersionedIntervalTimeline<VersionType, ObjectType extends Overshadowable<ObjectType>>`, the interval→version→partition index. `add(Interval, VersionType, PartitionChunk)` (`:179`), `lookup(Interval)` (`:311`), `findChunk(Interval, VersionType, int partitionNum)` (`:281`), `findFullyOvershadowed()` (`:381`). Both the Broker's view of the cluster (via `server`'s `CachingClusteredClient`/`TimelineServerView`) and each Historical's local `SegmentManager` are built on this same class from `processing/`.
- `processing/src/main/java/org/apache/druid/timeline/partition/PartitionHolder.java:33` and `processing/src/main/java/org/apache/druid/timeline/partition/PartitionChunk.java:31` — the per-interval bucket of chunks the timeline returns from `lookup`/`findChunk`.

### Column/schema pieces relevant to reading (brief)
`processing/src/main/java/org/apache/druid/segment/column/ColumnHolder.java:30` exposes a single column's data/index/capabilities from a `QueryableIndex`; `processing/src/main/java/org/apache/druid/segment/column/ColumnCapabilities.java:36` describes type/multi-value/dictionary-encoded/has-bitmap-index flags used to pick a read path. `processing/src/main/java/org/apache/druid/segment/DimensionHandler.java:63` / `processing/src/main/java/org/apache/druid/segment/DimensionIndexer.java:112` abstract over dimension types (string/long/float/double) for both indexing (dictionary building during ingest) and merging. Low-level compressed column readers implement `processing/src/main/java/org/apache/druid/segment/data/ColumnarLongs.java:41` and `processing/src/main/java/org/apache/druid/segment/data/ColumnarInts.java:29`.

## Query execution path

### Query abstractions
- `processing/src/main/java/org/apache/druid/query/Query.java:74` — query spec interface (datasource, intervals, context, granularity).
- `processing/src/main/java/org/apache/druid/query/QueryRunner.java:28` — `run(QueryPlus, ResponseContext)`; the composable unit of execution (segment-level and merge-level runners implement this same interface).
- `processing/src/main/java/org/apache/druid/query/QueryRunnerFactory.java:31` — per-query-type factory producing a segment-level `QueryRunner` (`createRunner(Segment)`) and a merge runner.
- `processing/src/main/java/org/apache/druid/query/QueryToolChest.java:46` (abstract) — per-query-type behavior: result merging, caching strategy (`CacheStrategy`), metrics.
- `processing/src/main/java/org/apache/druid/query/QueryPlus.java:35` — immutable wrapper of a `Query` plus a `QueryMetrics`/identity, threaded through the runner chain.
- `processing/src/main/java/org/apache/druid/query/QuerySegmentWalker.java:27` — interface implemented by Historical/Broker-side classes in `server` (`ServerManager`, `CachingClusteredClient`) to resolve a query + intervals/segment-descriptors into a `QueryRunner`.
- `processing/src/main/java/org/apache/druid/query/QueryContexts.java:41` — typed accessors over the query context map (timeouts, vectorize flags, priority, lane).

### Runner decorator stack
Segment-level runners are wrapped in a fixed decorator order (built in `server`'s `ServerManager.buildQueryRunnerForSegment`, using classes from this module):
- `processing/src/main/java/org/apache/druid/query/MetricsEmittingQueryRunner.java:35` — wraps `factory.createRunner(segment)`, reports per-segment time via `QueryMetrics::reportSegmentTime`.
- `CachingQueryRunner` lives in `server`, not this module (`server/src/main/java/org/apache/druid/client/CachingQueryRunner.java`); it wraps the above using this module's `CacheStrategy`/`QueryToolChest`.
- `processing/src/main/java/org/apache/druid/query/BySegmentQueryRunner.java:41` — optionally returns results tagged by segment (used for internal debugging/`bySegment` context flag).
- `processing/src/main/java/org/apache/druid/query/FinalizeResultsQueryRunner.java:43` — applies `QueryToolChest` finalization (e.g. sketch finalization) at the top of the stack.
- `processing/src/main/java/org/apache/druid/query/ChainedExecutionQueryRunner.java:57` — fans out `run(...)` (`:78`) across multiple segment-level runners on the processing thread pool and merges via the toolchest's ordering; used for non-groupBy query types.
- `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/GroupByMergingQueryRunner.java` — groupBy-specific parallel merge runner (uses buffer grouper infrastructure instead of `ChainedExecutionQueryRunner`).
- `ReferenceCountingSegmentQueryRunner` does not exist as a standalone class in this Druid version; the equivalent safety mechanism is `ReferenceCountedSegmentProvider.acquireReference()` (see Segment format section) called by `server`'s `ServerManager.acquireAllSegments` before a `QueryRunner` is ever built.

### Per-query-type engines
- **Timeseries**: `processing/src/main/java/org/apache/druid/query/timeseries/TimeseriesQueryEngine.java:66` — `process(...)` (`:91`) dispatches to `processVectorized` (`:133`) or `processNonVectorized` (`:255`) depending on `CursorHolder.canVectorize()`.
- **TopN**: `processing/src/main/java/org/apache/druid/query/topn/TopNQueryEngine.java:63` — `query(...)` (`:79`); delegates to algorithm selection (`HeapBasedTopNAlgorithm`, pooled scanners) based on cardinality/aggregator count.
- **GroupBy**: `processing/src/main/java/org/apache/druid/query/groupby/GroupingEngine.java:112` is the top-level entry (`process(...)` at `:477`), which dispatches to `VectorGroupByEngine.process` (`:524`, in `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/vector/VectorGroupByEngine.java:81`) when vectorizable, or non-vectorized `GroupByQueryEngine.process` (`:536`, static method in `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/GroupByQueryEngine.java:98`) otherwise. Buffer-based hash grouping lives in `query/groupby/epinephelinae/` (`BufferHashGrouper`, `ConcurrentGrouper`, `SpillingGrouper` for disk spill under memory pressure).
- **Scan**: `processing/src/main/java/org/apache/druid/query/scan/ScanQueryEngine.java:64` — `process(...)` (`:66`); `ScanQueryRunnerFactory` wires per-segment cursor scanning with row/time limiting (`ScanQueryLimitRowIterator`).
- **TimeBoundary**: `TimeBoundaryQueryRunnerFactory`/`TimeBoundaryQueryQueryToolChest` in `processing/src/main/java/org/apache/druid/query/timeboundary/` — answers min/max time, typically served from segment metadata without a full scan.
- **SegmentMetadata**: `SegmentMetadataQueryRunnerFactory` + `SegmentAnalyzer` in `processing/src/main/java/org/apache/druid/query/metadata/` — introspects column types/cardinality/size per segment; this is what feeds the Broker's schema cache (`server`'s `BrokerSegmentMetadataCache`/coordinator schema sync consumes results of this query type).

### Cursor & filter read path
- `processing/src/main/java/org/apache/druid/segment/CursorFactory.java:27` (interface) — `makeCursorHolder(CursorBuildSpec)` (`:33`) is how every query engine obtains a `CursorHolder` from a `Segment`.
- `processing/src/main/java/org/apache/druid/segment/CursorHolder.java:50` — `asCursor()` (`:56`) for row-at-a-time; `canVectorize()` (`:73`, default) gates `asVectorCursor()` for the vectorized path.
- `processing/src/main/java/org/apache/druid/segment/CursorBuildSpec.java:60` — carries filter, virtual columns, requested ordering/interval, and vectorize preference into cursor construction.
- `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorFactory.java:37` (`makeCursorHolder` at `:61`) and `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:69` — the on-disk/mmap segment implementation; this is what a Historical uses for merged segments.
- `processing/src/main/java/org/apache/druid/segment/Cursors.java:29` — utility, e.g. `getTimeOrdering` (`:58`), used to decide whether a query's requested ordering matches segment storage order (affects whether a sort is needed).
- `processing/src/main/java/org/apache/druid/query/vector/VectorCursorGranularizer.java` and `processing/src/main/java/org/apache/druid/query/CursorGranularizer.java` — bucket a cursor's rows into query granularity buckets during vectorized/non-vectorized scans (used by Timeseries/GroupBy engines).
- Filter/bitmap: `processing/src/main/java/org/apache/druid/query/filter/Filter.java:37` — `getBitmapColumnIndex(ColumnIndexSelector)` (`:125`) is the entry point for index-based (pre-filtering) row selection; results flow through `processing/src/main/java/org/apache/druid/segment/column/ColumnIndexSupplier.java:36` and `BitmapColumnIndex` implementations in `processing/src/main/java/org/apache/druid/segment/index/` (there is no single legacy `BitmapIndex` interface in this version — it was split into `ColumnIndexSupplier`/`BitmapColumnIndex`).

### Processing resources
- `processing/src/main/java/org/apache/druid/query/DruidProcessingConfig.java:37` — `numThreads` (`:44`, defaults based on cores) and `numMergeBuffers` (`:46`, defaults to `max(2, numThreads/4)`, see `:76`); `intermediateComputeSizeBytes()` (`:161`) sizes each merge buffer.
- `processing/src/main/java/org/apache/druid/query/QueryProcessingPool.java:40` (extends `ListeningExecutorService`) — the thread pool `ChainedExecutionQueryRunner` submits per-segment work to.
- `org.apache.druid.collections.BlockingPool` / `DefaultBlockingPool` (in `processing/src/main/java/org/apache/druid/collections/`) — the off-heap merge-buffer pool (used by GroupBy for hash-table buffers); threads block/timeout if no buffer is free, bounding concurrent GroupBy memory use.
- `QueryScheduler` and lane/priority throttling live in `server` (`server/src/main/java/org/apache/druid/server/QueryScheduler.java`), **not** in this module.

## Critical flows

**(a) Row → IncrementalIndex → persist → merged segment on disk** (Kafka ingest, driven from `server`'s `StreamAppenderator`, using this module's classes):

1. `StreamAppenderator.add(...)` (in `server`) calls `OnheapIncrementalIndex#add` → `IncrementalIndex#add(InputRow)` → abstract `addToFacts(...)` → `OnheapIncrementalIndex#addToFacts` inserts/aggregates the row in the on-heap fact table, returning `IncrementalIndexAddResult`.
2. When `maxRowCount`/memory threshold is hit (signalled via `IncrementalIndexAddResult`/`isRowAdded()`), the Appenderator triggers a persist: `IndexMergerV9#persist(IncrementalIndex, Interval, File, IndexSpec, ...)` reads rows out via `IncrementalIndexAdapter`, applying rollup as configured in `IncrementalIndexSchema#isRollup()`, and writes a v9-format segment directory to local disk.
3. Multiple persisted (per-flush) segments for the same Kafka task/interval are combined by `IndexMergerV9#mergeQueryableIndex(...)` → private `merge(...)`, dictionary-merging and rewriting columns/bitmaps into one segment.
4. The task pushes the merged directory to deep storage and constructs a `DataSegment` (with `SegmentId`, `NumberedShardSpec`, load spec pointing at S3) which is announced to the metadata store for the Coordinator to load onto Historicals.

**(b) Query arriving at a Historical → segment-level QueryRunner → cursor scan → merged result**:

1. `server`'s `ServerManager#getQueryRunnerForSegments` resolves `SegmentDescriptor`s against its local `VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>`, calling `acquireAllSegments` → `PartitionHolder#findChunk`-derived `ReferenceCountedSegmentProvider#acquireReference()` to safely obtain live `Segment` handles.
2. For each segment, `ServerManager#buildQueryRunnerForSegment` builds the decorator stack: `factory.createRunner(segment)` → `MetricsEmittingQueryRunner` → `CachingQueryRunner` → `BySegmentQueryRunner`.
3. `QueryRunnerFactory#createRunner`'s runner calls into the per-query-type engine (e.g. `TimeseriesQueryEngine#process`), which calls `Segment#as(CursorFactory.class)` → `QueryableIndexCursorFactory#makeCursorHolder(CursorBuildSpec)` → `CursorHolder#asCursor()`/`asVectorCursor()`, applying `Filter#getBitmapColumnIndex` for pre-filtering.
4. Per-segment results from `ChainedExecutionQueryRunner` (or `GroupByMergingQueryRunner` for groupBy) are merged via `QueryToolChest`'s merge function, then `FinalizeResultsQueryRunner` finalizes before returning to the Historical's HTTP layer.
5. On the Broker side, `server`'s `CachingClusteredClient` repeats an analogous merge across per-Historical results using the same `QueryToolChest`/`CacheStrategy` classes from this module.

**(c) A segment file on disk becomes a queryable `Segment` object**:

1. `IndexIO#loadIndex(File)` memory-maps the persisted/downloaded segment directory, validates the version against `IndexIO.CURRENT_VERSION_ID`, and returns a `QueryableIndex` (backed by `SimpleQueryableIndex`).
2. The `QueryableIndex` is wrapped in a `QueryableIndexSegment implements Segment`.
3. `server`'s `SegmentManager`/`SegmentLoader` wraps that `Segment` in a `ReferenceCountedSegmentProvider` and inserts it into the local `VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>`, making it visible to `ServerManager` for query flow (b).

## Cross-module pointers

- `server/src/main/java/org/apache/druid/server/coordination/ServerManager.java` — Historical's top-level `QuerySegmentWalker` impl; owns `acquireAllSegments`/`buildQueryRunnerForSegment` (see flow b).
- `server/src/main/java/org/apache/druid/server/SegmentManager.java` — owns the per-datasource `VersionedIntervalTimeline` of loaded segments on a Historical; loads/drops via `IndexIO`.
- `server/src/main/java/org/apache/druid/client/CachingClusteredClient.java` — Broker-side fan-out/merge across Historicals, reuses `QueryToolChest`/`CacheStrategy` from this module.
- `server/src/main/java/org/apache/druid/client/CachingQueryRunner.java` — segment-level result cache wrapper inserted into the `ServerManager` decorator stack.
- `server/src/main/java/org/apache/druid/server/QueryLifecycle.java`, `server/src/main/java/org/apache/druid/server/QueryScheduler.java` — request-level lifecycle and lane/priority throttling above `QuerySegmentWalker`.
- `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java` (+ `BatchAppenderator`, `Appenderators`) and `server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java` — own the `OnheapIncrementalIndex` lifecycle and call `IndexMergerV9` directly during Kafka ingest (flow a).
- `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTask.java` — the `index_kafka` task implementation driving the Appenderator above.
- `indexing-service/src/main/java/org/apache/druid/indexing/common/TaskToolboxFactory.java` — injects `IndexIO`, `IndexMergerV9`, `IndexSpec` into tasks.
- `indexing-service/src/main/java/org/apache/druid/indexing/common/task/BatchAppenderators.java`, `indexing-service/src/main/java/org/apache/druid/indexing/common/task/batch/parallel/PartialSegmentMergeTask.java` — other callers of `IndexMergerV9`/`OnheapIncrementalIndex` for batch/parallel indexing.

## Gotchas

- **Reference counting is mandatory for correctness, not just an optimization.** `ReferenceCountedSegmentProvider#acquireReference()` returns `Optional.empty()` once a segment has been dropped/closed; `ServerManager` must check this on every query, otherwise a concurrently-dropped segment could be read after its backing mmap is unmapped.
- **On-heap vs off-heap.** `IncrementalIndex` (ingest-time buffer) is entirely on-heap (`OnheapIncrementalIndex`); persisted/merged `QueryableIndex` data is memory-mapped (off-heap) via `IndexIO#loadIndex`. GroupBy merge buffers (`DruidProcessingConfig#numMergeBuffers`/`BlockingPool`) are also off-heap `ByteBuffer`s, separate from segment mmaps — sizing both matters for Historical memory footprint.
- **Rollup and granularity are fixed at `IncrementalIndex` creation time** (`IncrementalIndexSchema#isRollup()`), not adjustable at persist/merge time; `IndexMergerV9#persist`/`mergeQueryableIndex` only re-serialize what the `IncrementalIndex`/prior segments already contain.
- **`IndexMergerV9#persist` throws on an empty index** (`IAE("Trying to persist an empty index!")`) and validates the given `Interval` fully contains `index.getMinTime()`/`getMaxTime()` — a mismatched segment granularity vs. actual row timestamps fails persist, not silently truncates.
- **Vectorization fallback is per-cursor, per-query-type, decided by `CursorHolder#canVectorize()`.** Engines like `TimeseriesQueryEngine`/`GroupingEngine` branch to non-vectorized processing (row-at-a-time `Cursor`) whenever a filter, virtual column, or aggregator isn't vectorizable — this is a silent perf fallback, not an error.
- **Row-limit signalling during ingest is advisory, not a hard exception path.** `OnheapIncrementalIndex` sets `outOfRowsReason` and returns it via `IncrementalIndexAddResult`; it's the caller's (Appenderator's) responsibility to check `isRowAdded()`/the reason and trigger a persist — the row is otherwise silently not added.
- **`NumberedShardSpec`** (streaming append shard spec) allows multiple concurrent segments per interval per Kafka task, which is why `VersionedIntervalTimeline#findChunk` takes an explicit `partitionNum` — the timeline is not just interval+version keyed.
