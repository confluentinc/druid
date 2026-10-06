# Code deep dive — druid-processing

Scope: this document traces *execution mechanics* along exactly two paths — (A) the ingest/segment-build
path used by `index_kafka` tasks (`OnheapIncrementalIndex` → `IndexMergerV9` → v9 segment dir → `IndexIO`),
and (B) the per-segment query-execution path used by Historicals (`QueryRunner` stack → `CursorHolder` →
filter split → aggregate → merged `Sequence`).
Aggregators, expressions, joins, nested/JSON columns, SQL planning, MSQ, and projections are out of scope
except where they change control flow on these two paths.
Class inventory and "who calls what across modules" already live in `processing/CLAUDE.md` — read that first.
This doc assumes it and goes to method-body granularity instead.

## Happy path, traced

### (A) Ingest: one Kafka row → on-disk v9 segment

All of A1–A6 runs on the **task's single ingest thread** (the `SeekableStreamIndexTask` runner thread).
`IncrementalIndex#addToFacts` is explicitly documented as *not* needing thread safety
(`processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:384`), and `add()`
itself says "Not thread-safe" (`:460`). The concurrent data structures underneath exist for *readers*
(realtime queries / persist iteration), not for concurrent writers.

**A1. `IncrementalIndex#add(InputRow)`** — `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:466`
Four steps, in order: `toIncrementalIndexRow(row)` (`:468`), `inputRowHolder.set(row)` (`:469`),
`addToFacts(...)` (`:470`), `updateMaxIngestedTime(row.getTimestamp())` (`:474`).
Returns `IncrementalIndexAddResult(rowCount, bytesInMemory, parseException)` (`:481`).
Note `updateMaxIngestedTime` is `private synchronized` (`:647`) — the only lock on the hot add path
besides `dimensionDescs`.

**A2. `toIncrementalIndexRow`** — `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:489`
- Applies `rowTransformers` via `formatRow` (`:491`, `:406`).
- Hard-rejects rows below the index's `minTimestamp` with `DruidException.defensive` (`:492-498`).
- Enters `synchronized (dimensionDescs)` (`:505`) — this is the real ingest-side lock. Inside it:
  - builds `dims = new Object[dimensionDescs.size()]` (`:510`);
  - for each dimension in the row, looks up `DimensionDesc`; if absent, **discovers a new dimension**
    (`addNewDimension`, `:532`) using `NestedCommonFormatColumnHandler` when `useSchemaDiscovery`, else
    forced-STRING legacy behavior (`:522-531`);
  - calls `indexer.processRowValsToUnsortedEncodedKeyComponent(value, true)` (`:538`) — **this is where the
    per-dimension dictionary gets its new entry**, and where `dimsKeySize` accumulates
    `encodedKeyComponent.getEffectiveSizeBytes()` (`:540`);
  - `ParseException` from a single dimension is *caught and collected*, not thrown (`:542-544`) — the row
    still proceeds with `dimsKey == null` for that column;
  - a dimension appearing twice in one row throws `ISE("Dimension[%s] occurred more than once in InputRow")`
    (`:564`);
  - dimensions known to the index but absent from this row get `indexer.setSparseIndexed()` (`:571-573`).
- Newly discovered dims land in an `overflow` list and are appended after the fixed-width prefix (`:576-584`).
- **Granularity truncation happens here**: `truncated = queryGranularity.bucketStart(row.getTimestampFromEpoch())`
  (`:588`), then the row key timestamp is `Math.max(truncated, minTimestamp)` (`:591`).
- Returns `IncrementalIndexRowResult(incrementalIndexRow, parseExceptionMessages)` (`:596`).

**A3. `OnheapIncrementalIndex#addToFacts`** — `processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:228`
State: `facts` (a `FactsHolder`, `:93`), `aggregators` = `ConcurrentHashMap<Integer, Aggregator[]>` (`:92`),
`indexIncrement` = `AtomicInteger` (`:94`), plus inherited `numEntries` and `bytesInMemory`
(`processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:256`).

There is **no `synchronized` block and no CAS retry loop in this method** in 34.0.0. Safety comes entirely
from the single-writer assumption; the code asserts it defensively (see below).

- Projections are updated *first*, deliberately, so a projection is never behind the base table (`:236-240`).
- `final int priorIndex = facts.getPriorIndex(key)` (`:242`) — the rollup lookup.
- **Existing key (rollup hit)**: `aggs = aggregators.get(priorIndex)` (`:248`), then
  `doAggregate(...)` returns the incremental byte delta which is added to `totalSizeInBytes` (`:249-250`).
  No new fact entry, `numEntries` unchanged.
- **New key**: allocate `Aggregator[metrics.length]` (`:255`, doubled when `preserveExistingMetrics`, `:253`),
  `factorizeAggs` (`:257` → `:292`) which calls `agg.factorizeWithSize(selectors.get(name))` and sums
  `getInitialSizeBytes() + Long.BYTES` per aggregator (`:305-311`); then `doAggregate` for this row (`:258`).
  Then:
  ```java
  final int rowIndex = indexIncrement.getAndIncrement();
  aggregators.put(rowIndex, aggs);
  final int prev = facts.putIfAbsent(key, rowIndex);
  if (IncrementalIndexRow.EMPTY_ROW_INDEX == prev) { numEntries.incrementAndGet(); }
  else { throw DruidException.defensive("Encountered existing fact entry for new key, possible concurrent add?"); }
  ```
  (`OnheapIncrementalIndex.java:260-268`) — the `else` branch is the single-writer assertion.
- Byte accounting for a new row (`:271-275`):
  `key.estimateBytesInMemory() + aggregator init bytes + ROUGH_OVERHEAD_PER_MAP_ENTRY`, where
  `ROUGH_OVERHEAD_PER_MAP_ENTRY = Long.BYTES * 5 + Integer.BYTES` = 44 (`:90`), and
  `estimateBytesInMemory()` = `8 + 4*dims.length + 8 + 8 + dimsKeySize`
  (`IncrementalIndexRow.java:127-132`).
- Returns `AddToFactsResult(numEntries.get(), totalSizeInBytes.get(), parseExceptionMessages)` (`:278`).

**What `sizeInBytes` counts / misses.** Counted: the row-key object estimate, per-aggregator
`getInitialSizeBytes()` plus one 8-byte reference each, per-`aggregate()` incremental growth
(`aggregateWithSize()`, `:349`), and the flat 44-byte map-entry constant. **Not** counted: the
`ConcurrentHashMap<Integer, Aggregator[]>` entry and boxed `Integer` key in `aggregators`, the
`Aggregator[]` array header, and — most significantly — the **per-dimension dictionaries inside each
`DimensionIndexer`**, which grow with cardinality and are never charged to `bytesInMemory`. On a high-
cardinality string dimension the real heap footprint can substantially exceed the tracked figure.

**Parse-exception contract.** `doAggregate` catches `ParseException` per aggregator: with
`preserveExistingMetrics` it logs `warn` and rethrows (fails ingestion, `:355-362`); otherwise it logs at
`debug` and appends to `parseExceptionsHolder`, skipping that one aggregator (`:363-366`). Dimension-level
and aggregator-level messages are then merged by `getCombinedParseException`
(`processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:600`) into a single `UnparseableColumnsParseException` (`:636`) — or `null` if
`numAdded == 0` (`:626-628`).

**A4. Row-limit signalling** — `OnheapIncrementalIndex#canAppendRow` at `:411`
`countCheck = numRows() < maxRowCount`; `sizeCheck = maxBytesInMemory <= 0 || getBytesInMemory().get() < maxBytesInMemory`.
Sets `outOfRowsReason` to one of three strings (`:416`, `:423`, `:425`) and returns the conjunction.
This is a **pre-add gate the caller must poll**, not something `add()` enforces.
`IncrementalIndexAddResult.isRowAdded()` is `reasonOfNotAdded == null && parseException == null`
(`IncrementalIndexAddResult.java:96-99`); the `reasonOfNotAdded` constructor (`:58`) is used by
`server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:70` (`NOT_WRITABLE`) and `:73`
("write after index swapped") — **not** by the row-limit path. The row-limit path surfaces via
`StreamAppenderator.add` calling `sink.canAppendRow()`
(`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:351`),
which sets `persist = true` with reason "No more rows can be appended to sink" (`:353`); it also
independently checks `rowsCurrentlyInMemory >= maxRowsInMemory` (`:363`) and
`bytesCurrentlyInMemory >= maxBytesTuningConfig` (`:371`). Note `maxBytesInMemory == 0` is remapped to
`Long.MAX_VALUE` in the constructor (`processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:123`), so "0" means *unlimited*, while
`-1` disables only the `sizeCheck` (`:413`).

**A5. `IndexMergerV9#persist`** — `processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1042` (persist thread from the Appenderator's
persist executor)
- `if (index.isEmpty()) throw new IAE("Trying to persist an empty index!")` (`:1051-1053`).
- Interval validation (`:1057-1064`):
  `dataInterval.contains(index.getMinTime()) && dataInterval.contains(index.getMaxTime())`, else
  `IAE("interval[%s] does not encapsulate the full range of timestamps[%s, %s]")`. Joda `contains(instant)`
  is `start <= t < end`, so **a row exactly on the interval end fails persist**.
- Wraps the index in a single `IncrementalIndexAdapter` (`:1071`) and calls
  `multiphaseMerge(..., rollup=false, ..., maxColumnsToMerge=-1)` (`:1069-1090`). `rollup=false` is
  deliberate: rollup already happened in the facts table, so re-rolling would be a no-op on a single
  iterable (comment at `:1077-1080`).
- **`IncrementalIndexAdapter`'s constructor eagerly walks every row** (`processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndexAdapter.java:64` →
  `processRows` at `:77`), filling per-dictionary-id `MutableBitmap[]` via
  `indexer.fillBitmapsFromUnsortedEncodedKeyComponent(...)` (`:101`) and touching `null` into every
  dictionary for absent dims (`:93`). So the first inverted indexes are built on the persist thread,
  in heap, before any file is written.

**A6. `multiphaseMerge` → `merge` → `makeIndexFiles`**
- `multiphaseMerge` (`processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1149`): `deleteDirectory(outDir)` then `mkdirp` (`:1162-1163`).
  With `maxColumnsToMerge == UNLIMITED_MAX_COLUMNS_TO_MERGE` it goes straight to `merge` (`:1167-1178`);
  otherwise it batches adapters into phases by column count (`getMergePhases`, `:1251`), merges each phase to
  a temp dir, **reloads the phase outputs with `indexIO.loadIndex(outputFile, true, NOOP)`** (`:1228`) and
  loops until one output remains. Temp dirs are cleaned in `finally` (`:1237-1248`).
- `merge` (`:1299`): computes `mergedDimensionsWithTime` and `mergedMetrics` (`:1310-1314`); builds
  `sortedMetricAggs` aligned to `mergedMetrics` and throws `IAE("Indices to merge contained metric[%s], but
  requested metrics did not")` on a gap (`:1334`) or `IAE("Metric mismatch, index[%d] ...")` on order
  mismatch (`:1340`). Chooses the row merger: `RowCombiningTimeAndDimsIterator` when `rollup`, else
  `MergingRowIterator` (`:1349-1354`). Merges `Metadata` across adapters (`:1361`) and requires `__time`
  in any explicit sort order (`:1363-1373`).
- `makeIndexFiles` (`:131`) is the phase machine. Ordered writes:
  1. `new FileSmoosher(outDir)` (`:154`), then `SegmentWriteOutMedium` chosen from the passed factory or
     `defaultSegmentWriteOutMediumFactory` (`:157-160`) and registered on the `Closer` (`:161`).
     Default impl per `processing/src/main/java/org/apache/druid/segment/writeout/SegmentWriteOutMediumFactory.java:30` is `TmpFileSegmentWriteOutMediumFactory`
     ("tmpFile"); alternatives are "offHeapMemory", "onHeapMemory", "legacyTmpFile" (`:32-35`).
     `TmpFileSegmentWriteOutMedium` creates `outDir/tmpOutputFiles` and hands out
     `LazilyAllocatingHeapWriteOutBytes` (`TmpFileSegmentWriteOutMedium.java:67-76`) — i.e. small columns
     stay on heap and only spill to a `filePeon*` temp file when they outgrow the heap buffer.
  2. `version.bin` = `Ints.toByteArray(IndexIO.V9_VERSION)` (`:163`).
  3. `factory.json` = `MMappedQueryableSegmentizerFactory` unless `indexSpec.getSegmentLoader()` overrides
     (`:168-175`).
  4. `mergeFormat` unions per-column `ColumnFormat`s across adapters (`:181` → `:1014`), then one
     `DimensionMergerV9` per dimension via `handler.makeMerger(...)` (`:186-199`).
  5. **Dictionary merge phase**: `writeDimValuesAndSetupDimConversion` (`:215` → `:998`) calls
     `merger.writeMergedValueDictionary(indexes)` per dimension.
     In `DictionaryEncodedColumnMerger.writeMergedValueDictionary` (`processing/src/main/java/org/apache/druid/segment/DictionaryEncodedColumnMerger.java:168`):
     collects each adapter's `getDimValueLookup` (`:187`), forces the null entry in when a dim is present in
     some indexes and absent in others (`:198-211`), and if `numMergeIndex > 1` builds a
     `DictionaryMergingIterator` (`:222`) that produces the merged sorted dictionary and per-adapter
     `conversions[i]` id-remap buffers (`:228-243`); `cardinality` comes from the iterator (`:244`) or the
     single lookup's size (`:247`). Then `setupEncodedValueWriter()` (`:257`) picks
     `CompressedVSizeColumnarIntsSerializer` / `VSizeColumnarIntsSerializer` / multi-int variants based on
     compression and multi-value-ness (`:463-482`).
  6. **Row walk / value write**: `makeMergedTimeAndDimsIterator` (`:220` → `:1403`) wraps each adapter's
     `getRows()`, reordering columns if its schema differs from the merged schema (`:1416-1421`).
     `mergeIndexesAndWriteColumns` (`:232` → `:801`) then does, per merged row:
     `timeWriter.serialize(timeAndDims.timestampSelector)` (`:826`), each metric writer (`:828-830`), each
     `merger.processMergedRow(...)` skipping all-null dims (`:832-838`), and fills
     `rowNumConversions` — the old-row → new-row map needed to rewrite bitmaps (`:840-871`).
     `processMergedRow` (`processing/src/main/java/org/apache/druid/segment/DictionaryEncodedColumnMerger.java:362`) also maintains `nullRowsBitmap` (`:367`, `:371`)
     and increments `rowCount` (`:379`).
  7. **Bitmap/index phase**: `makeTimeColumn` (`:244` → `:746`), `makeMetricsColumns` (`:245` → `:653`),
     then per dimension `merger.writeIndexes(indexMergeResult.rowNumConversions)` (`:256`).
     `writeIndexes` (`processing/src/main/java/org/apache/druid/segment/DictionaryEncodedColumnMerger.java:383`) returns immediately if the column has no
     bitmap indexes (`:385-387`), else opens a `GenericIndexedWriter<ImmutableBitmap>` on
     `<col>.inverted` with `setObjectsNotSorted()` (`:393-399`) and loops `dictId` ascending (`:412-423`)
     calling `mergeBitmaps` (`:500`). `mergeBitmaps` gathers each adapter's bitmap for the equivalent
     dictId, wraps it in `ConvertingBitmapValues` using `rowNumConversions` (`:513-516`), merges the
     ascending iterators with dedup and `INVALID_ROW` skipping (`:535-542`) — the ascending merge exists
     because compacting bitmaps are slow on out-of-order inserts (comment `:532-533`) — ORs
     `nullRowsBitmap` into dictId 0 when appropriate (`:544-546`), and writes the immutable bitmap (`:548`).
  8. Columns with `hasOnlyNulls()` (`cardinality == 0`, `:554-557`) are skipped entirely unless
     `dimensionsSpecInspector.shouldStore(...)`, in which case a `NullColumnPartSerde` placeholder is written
     (`IndexMergerV9.java:257-268`).
  9. `makeColumn` (`:768`) serializes the JSON `ColumnDescriptor` header plus the column bytes into one
     smoosh entry via `v9Smoosher.addWithSmooshedWriter(name, specBytes.size() + serdeficator.getSerializedSize())`
     (`:776-782`).
  10. `makeIndexBinary` (`:537`) writes the column/dim name lists, `dataInterval` start/end millis
      (`:621-624`), the bitmap serde type (`:626`), and null-only cols/dims *last* for forward-compat
      (`:628-632`); then `IndexIO.checkFileSize` rejects an `index.drd` > `Integer.MAX_VALUE` (`:635`).
      `makeMetadataBinary` (`:320`) adds `metadata.drd`.
  11. `v9Smoosher.close()` (`:307`) writes `meta.smoosh`.
- **The smoosh / 2 GiB boundary.** `FileSmoosher` defaults `maxChunkSize = Integer.MAX_VALUE`
  (`processing/src/main/java/org/apache/druid/java/util/common/io/smoosh/FileSmoosher.java:101`, field `:81`) — i.e. ~2 GiB per `0000x.smoosh` chunk file, chosen precisely
  because "no file of size greater than Integer.MAX_VALUE" can be mapped (`:64`).
  `addWithSmooshedWriter` throws `IAE("Asked to add buffers[%,d] larger than configured max[%,d]")` for a
  single entry above the chunk size (`:175-177`), and rotates to a new chunk when
  `currOut.bytesLeft() < size` (`:189-192`). Each finished entry records
  `Metadata(fileNum, startOffset, endOffset)` in `internalFiles` (`:244`), and `close()` writes
  `v1,<maxChunkSize>,<numChunks>` plus one line per entry into `meta.smoosh` (`:402-416`).
  A single column part therefore **cannot exceed ~2 GiB**, but a segment can span many chunk files.

### (B) Query: segment-level runner → cursor → filter → aggregate → merged sequence

**B1. Per-segment runner construction.** `ServerManager` (in `server/`) resolves descriptors against its
`VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>` and calls
`ReferenceCountedSegmentProvider#acquireReference()`
(`processing/src/main/java/org/apache/druid/segment/ReferenceCountedSegmentProvider.java:168`) —
`incrementReferenceAndDecrementOnceCloseable()` then `.map(ReferenceClosingSegment::new)` (`:170-171`).
Then `factory.createRunner(segment)`; e.g.
`TimeseriesQueryRunnerFactory.createRunner`
(`processing/src/main/java/org/apache/druid/query/timeseries/TimeseriesQueryRunnerFactory.java:63`)
does `new TimeseriesQueryRunner(engine, segment.as(CursorFactory.class), segment.as(TimeBoundaryInspector.class))`
(`:65`) — the `Segment#as` capability lookup is where mmap-backed vs incremental-index-backed diverge.
The stack is wrapped by `MetricsEmittingQueryRunner`
(`processing/src/main/java/org/apache/druid/query/MetricsEmittingQueryRunner.java:86`), which uses a
`LazySequence` specifically so `run()`'s own preparation time is counted inside the reported segment time
(comment `:94-97`), and reports `status("failed")` / `status("short")` in `after()` (`:111-115`).

**B2. Fan-out: `ChainedExecutionQueryRunner#run`** —
`processing/src/main/java/org/apache/druid/query/ChainedExecutionQueryRunner.java:78`
Runs on the **caller/HTTP thread**; per-segment work runs on the **`QueryProcessingPool`**.
- Reads `priority = query.context().getPriority()` (`:81`) and `ordering = query.getResultOrdering()` (`:82`);
  strips thread-unsafe state from the `QueryPlus` (`:83`).
- Returns a `BaseSequence` whose `make()` (`:88`) is where the work actually starts — nothing is submitted
  until the sequence is consumed.
- Inside `make()`: `Lists.newArrayList(Iterables.transform(...))` (`:91-93`) — the `List` materialization is
  deliberate ("Make it a List<> to materialize all of the values (so that it will submit everything to the
  executor)", `:90`). Each runner is submitted as an
  `AbstractPrioritizedQueryRunnerCallable<>(priority, input)` via `queryProcessingPool.submitRunnerTask`
  (`:100-101`).
- **Each segment task fully materializes its own results**: `result.toList()` (`:112`). This is the first
  place laziness is broken. `null` runner → `ISE("Null queryRunner! Looks to be some segment unmapping
  action happening")` (`:97`); `null` sequence → `ISE("Got a null result! Segments are missing!")` (`:109`).
- `Futures.allAsList(futures)` (`:140`), then `queryWatcher.registerQueryFuture(query, future)` (`:141`) —
  this is the hook that makes external cancellation possible.
- Blocking join: `future.get(context.getTimeout(), MILLISECONDS)` when `context.hasTimeout()`, else
  `future.get()` (`:146-148`), and the results are merged with `new MergeIterable<>(..., ordering.nullsFirst())`
  (`:145-150`) — a k-way merge over already-materialized lists.
- Cancellation/timeout handling (`:152-170`): `InterruptedException` → `GuavaUtils.cancelAll(true, future, futures)`
  then `QueryInterruptedException` (`:155-156`); `CancellationException` → `QueryInterruptedException` (`:159`);
  `TimeoutException` → `cancelAll(true, ...)` then `QueryTimeoutException("Query [%s] timed out")` (`:163-164`);
  `ExecutionException` → `cancelAll` then rethrow cause (`:167-169`).
  `cancelAll(true, ...)` interrupts the worker threads; in-flight segment work notices via
  `Thread.currentThread().isInterrupted()` in `FilteredOffset#increment`
  (`processing/src/main/java/org/apache/druid/segment/FilteredOffset.java:45`) and
  `BaseQuery.checkInterrupted()` (`processing/src/main/java/org/apache/druid/query/BaseQuery.java:52-57`),
  which throws `QueryInterruptedException` on `Thread.interrupted()`.

**B3. Engine entry: `TimeseriesQueryEngine#process`** —
`processing/src/main/java/org/apache/druid/query/timeseries/TimeseriesQueryEngine.java:91` (processing thread)
- `cursorFactory == null` → `SegmentMissingException("Null cursor factory found. Probably trying to issue a
  query against a segment being memory unmapped.")` (`:99-103`).
- `cursorFactory.makeCursorHolder(makeCursorBuildSpec(query, metrics))` (`:107`).
- Vectorization decision: `query.context().getVectorize().shouldVectorize(cursorHolder.canVectorize())`
  (`:114`) → `processVectorized` (`:115`) or `processNonVectorized` (`:117`).
- The `CursorHolder` is attached as sequence baggage (`:122`, `:124`) and closed eagerly on throw (`:128`).

**B4. `QueryableIndexCursorFactory#makeCursorHolder`** —
`processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorFactory.java:61`
If `index.getProjection(spec)` is null → plain `new QueryableIndexCursorHolder(index, spec, timeBoundaryInspector)`
(`:66`); otherwise a subclass that rewrites the build spec and reports `isPreAggregated() == true` (`:70-110`).
Constructor is cheap — everything expensive is behind `Suppliers.memoize(() -> new CursorResources(...))`
(`QueryableIndexCursorHolder.java:110-121`). It also flips the ordering to descending time when the query
prefers it and the index is ascending (`:100-105`).

**B5. `CursorResources` — the filter split.** `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:667`
- Creates a `Closer`, a `ColumnCache(index, closer)` (`:679`) and a `ColumnSelectorColumnIndexSelector`
  (`:681-685`).
- `numRows = index.getNumRows()` (`:687`).
- `computeFilterWithIntervalIfNeeded` (`:689` → `:784`): **only for non-time-ordered segments**
  (`timeOrder == Order.NONE`) does it fold the query interval into the filter as a `RangeFilter` on `__time`
  with `lowerOpen=false, upperOpen=true` (`:794-802`), AND-ed with the user filter (`:807`). For time-ordered
  segments the interval is handled by offset seeking instead (B6).
- `makeFilterBundle` (`:688` → `:729`): returns `null` if the filter is `null` (`:745-747`); otherwise times
  `new FilterBundle.Builder(filter, bitmapIndexSelector, cursorAutoArrangeFilters).build(bitmapResultFactory, numRows, numRows, false)`
  (`:748-758`) and reports `reportBitmapConstructionTime` + `filterBundle(info)` to `QueryMetrics` (`:759-764`).
- **`FilterBundle.Builder`** (`processing/src/main/java/org/apache/druid/query/filter/FilterBundle.java:159`):
  calls `filter.getBitmapColumnIndex(columnIndexSelector)` once (`:172`), recurses into `BooleanFilter`
  children (`:174-182`), and if `cursorAutoArrangeFilters` (default `true`, gated by
  `QueryContexts.CURSOR_AUTO_ARRANGE_FILTERS` at `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:118`) **sorts children
  ascending by `estimatedIndexComputeCost`** (`:185`) so cheap indexes run first. Without it the cost is
  pinned to `Integer.MAX_VALUE` (`:188`).
- **Default split** (`processing/src/main/java/org/apache/druid/query/filter/Filter.java:66`): if a
  `BitmapColumnIndex` exists, compute it (`:79-84`); `needMatcher = result == null || !columnIndex.getIndexCapabilities().isExact()`
  (`:96`). A `null` index result (index declined) or an inexact index both force a post-filter
  `ValueMatcher` built lazily from `this::makeMatcher` / `this::makeVectorMatcher` (`:103-108`).
- **AND partitioning** (`processing/src/main/java/org/apache/druid/segment/filter/AndFilter.java:135`):
  iterates children in cost order, building each sub-bundle with
  `applyRowCount = Math.min(applyRowCount, indexIntersectionSize)` (`:162`) so the *remaining* selectivity is
  fed forward. Intersects bitmaps incrementally and updates `indexIntersectionSize = index.size()` (`:181-183`).
  **Short-circuit**: any empty sub-bitmap returns `FilterBundle.allFalse(...)` immediately (`:167-174`).
  Children with no index become `ValueMatcher`s combined by `makeMatcher(matchers[])` (`:225-232`) /
  `makeVectorMatcher` (`:235-244`).
- **The index-vs-matcher cost cutoff** lives in
  `processing/src/main/java/org/apache/druid/segment/index/DictionaryScanningBitmapIndex.java:60`:
  ```java
  if (applyRowCount != totalRowCount && applyRowCount < (dictionarySize * scaleThreshold)) {
    return null;   // decline the index; caller falls back to a ValueMatcher
  }
  ```
  `estimatedComputeCost()` is just `dictionarySize` (`:53-56`), and `scaleThreshold` defaults to 1.0 (`:43`).
  So once an earlier AND clause has narrowed the row set below a column's dictionary size, scanning the
  remaining rows with a matcher is preferred over scanning the dictionary.
  `DictionaryRangeScanningBitmapIndex` applies the analogous rule for ranges.

**B6. `asCursor()`** — `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:154`
- `metrics.vectorized(false)` (`:157`).
- Base offset: no index → `SimpleAscendingOffset(numRows)` / `SimpleDescendingOffset(numRows)` (`:172-173`);
  index present → `BitmapOffset.of(filterBundle.getIndex().getBitmap(), descending, index.getNumRows())` (`:176`).
- **Interval handling for time-ordered segments**: a linear scan advances `baseOffset` until the timestamp
  reaches `timeStart` (`:184-188` ascending, `:196-200` descending), then wraps in
  `AscendingTimestampCheckingOffset` / `DescendingTimestampCheckingOffset` (`:189-206`, classes at `:588`,
  `:621`). Those carry `allWithinThreshold` (`:193`, `:205`) so the per-row timestamp comparison is skipped
  when the whole segment is inside the interval (`:546`). For `Order.NONE`, `offset = baseOffset` and the
  interval is already inside the filter bundle (`:207-210`).
- `offset.clone()` → `makeColumnSelectorFactoryForOffset(columnCache, baseCursorOffset)` (`:212-216`, `:321`).
- **Post-filter attach**: if the bundle has a matcher, build it against the *cursor's* selector factory and
  wrap in `FilteredOffset` (`:219-227`); otherwise the cursor uses the raw offset (`:229`).
- `FilteredOffset#increment` (`processing/src/main/java/org/apache/druid/segment/FilteredOffset.java:43`) is the row-at-a-time post-filter loop:
  advance base, stop on out-of-bounds or `filterMatcher.matches(false)`, and bail out early if the thread is
  interrupted (`:45-50`). `incrementIfNeededOnCreationOrReset` (`:66`) then calls `BaseQuery.checkInterrupted()`
  because an interrupted `increment()` leaves the offset in an illegal state (comment `:71-74`).
  `FilteredOffset` deliberately does **not** support `clone()` (`:85-90`).

**B7. `canVectorize()`** — `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:125`. Returns `false` when:
1. virtual columns exist and `queryContext.getVectorizeVirtualColumns().shouldVectorize(virtualColumns.canVectorize(inspector))`
   is false (`:128-132`);
2. any `AggregatorFactory.canVectorize(inspector)` is false (`:133-139`);
3. `filterBundle.canVectorizeMatcher()` is false (`:143-147`) — note this forces `CursorResources`
   construction, i.e. **bitmap indexes are computed before the vectorization decision is final**;
4. the effective time ordering is `Order.DESCENDING` — "vector cursors can't iterate backwards yet" (`:149-150`).
GroupBy adds a fifth gate: `VectorGroupByEngine.canVectorizeDimensions`
(`processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/vector/VectorGroupByEngine.java:174`)
rejects `!dimension.canVectorize()` (`:180`), `dimension.mustDecorate()` (`:184-189`), non-primitive output
types (arrays/complex, `:191-194`), and `hasMultipleValues().isMaybeTrue()` (`:197-202`).
The **silent fallback points** are `TimeseriesQueryEngine.java:114-118` and
`processing/src/main/java/org/apache/druid/query/groupby/GroupingEngine.java:519-547`. `Vectorize.TRUE`
(the default, `processing/src/main/java/org/apache/druid/query/QueryContexts.java:149`) silently degrades; only `Vectorize.FORCE` throws
`ISE("Cannot vectorize!")` (`QueryContexts.java:195-204`).

**B8. `asVectorCursor()`** — `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:239`
Re-asserts `canVectorize()` and closes + throws `IllegalStateException("canVectorize()")` if false (`:249-252`).
Uses **binary search** rather than a linear scan for interval bounds: `timeSearch(timestamps, startMillis, 0, numRows)`
(`:263`, method at `:358`) and the same for the end (`:269`). Offsets are
`NoFilterVectorOffset(vectorSize, start, end)` or `BitmapVectorOffset(...)` (`:276-279`), and a post-filter
becomes `FilteredVectorOffset.create(baseOffset, vectorValueMatcher)` with a **second** selector factory
built over the filtered offset (`:289-301`).

**B9. Aggregation loop (non-vectorized timeseries)** — `processing/src/main/java/org/apache/druid/query/timeseries/TimeseriesQueryEngine.java:255`
`cursorHolder.asCursor()`; `null` → `Sequences.empty()` (`:267-269`). Wrapped in `RowCountingCursorDecorator`
(`:270`), then `CursorGranularizer.create(...)` (`:271-277`) — `null` also → empty (`:278-280`).
`Sequences.simple(granularizer.getBucketIterable()).map(bucketInterval -> {...})` (`:282-283`) is the lazy
per-bucket body: `granularizer.advanceToBucket` (`:287`), `factorize` fresh `Aggregator`s per bucket (`:293-296`),
`while (!cursor.isDone()) { aggregator.aggregate(); ... granularizer.advanceCursorWithinBucket(); }` (`:299-308`),
build the result (`:316-323`), and `agg.close()` in `finally` (`:325-330`). Empty buckets return `null` and are
dropped by `.filter(Objects::nonNull)` (`:333`).

**B10. Merged sequence.** `BaseSequence`
(`processing/src/main/java/org/apache/druid/java/util/common/guava/BaseSequence.java:27`) is the laziness
primitive: `accumulate` (`:37`) pulls `maker.make()` then drains, always calling `maker.cleanup(iterator)`
(`:49`, `:56`); `toYielder` (`:61`) stops as soon as `accumulator.yielded()` and returns a `Yielder` whose
`next()` resumes the same iterator (`:111-124`) and whose `close()` runs `maker.cleanup(iter)` (`:133-136`).
`MergeSequence.toYielder`
(`processing/src/main/java/org/apache/druid/java/util/common/guava/MergeSequence.java:53`) keeps one `Yielder`
per input in a `PriorityQueue` (`:55-60`) — **streaming**, k-way, bounded by k not by result size.

## Nuances

- **Rollup timing.** Rollup is a property of the `IncrementalIndex`, decided at construction:
  `incrementalIndexSchema.isRollup()` picks `RollupFactsHolder` vs `PlainTimeOrderedFactsHolder` vs
  `PlainNonTimeOrderedFactsHolder` (`OnheapIncrementalIndex.java:124-130`). `RollupFactsHolder.getPriorIndex`
  does a real `facts.get(key)` (`:754-758`); the plain holders **always return `EMPTY_ROW_INDEX`** (`:844-848`)
  so every row is a new fact. `persist` then passes `rollup=false` (`processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1081`) — persist never
  re-rolls. Only `mergeQueryableIndex` takes a caller-supplied `rollup` flag (`:1096`, `:1109`).
- **Granularity truncation** happens once, at `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:588`, using
  `queryGranularity.bucketStart(...)`, then floored at `minTimestamp` (`:591`). Two rows in the same bucket
  with identical dims collapse only if rollup is on.
- **Dictionary encoding.** Ingest-time dictionaries are per-`DimensionIndexer`, *unsorted*, assigned in
  first-seen order (`processRowValsToUnsortedEncodedKeyComponent`, `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:538`). The
  *sorted* dictionary is produced at merge time (`DictionaryEncodedColumnMerger.java:222-247`), which is why
  merging needs `conversions[i]` id-remap buffers and `rowNumConversions` row-remap buffers.
- **Null handling.** Three separate mechanisms: `nullRowsBitmap` per dimension merger
  (`processing/src/main/java/org/apache/druid/segment/DictionaryEncodedColumnMerger.java:367`, `:371`, OR-ed into dictId 0 at `:544-546`); forced null-dictionary
  insertion when a dim is present in some adapters and absent in others (`:198-211`); and all-null columns,
  which are dropped unless `storeEmptyColumns`, in which case a `NullColumnPartSerde` is written
  (`IndexMergerV9.java:257-268`). On load, null-only column names live at the *end* of `index.drd` so older
  Historicals can ignore them (`IndexMergerV9.java:628-632`, restored by `restoreColumns` at
  `IndexIO.java:636-643`).
- **Exclusive interval ends, everywhere.** `persist`'s `dataInterval.contains(maxTimestamp)` is
  `start <= t < end` (`processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1057`). The synthetic time filter uses `upperOpen = true`
  (`processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:800`). `AscendingTimestampCheckingOffset` compares against
  `timeEnd = interval.getEndMillis()` exclusively (`:180`, `:189-194`).
  `RollupFactsHolder.timeRangeIterable`'s non-time-ordered path filters `row.timestamp >= timeStart &&
  row.timestamp < timeEnd` (`processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:795`).
- **Memory-mapped vs heap.** Segment column bytes are `MappedByteBuffer`s created by
  `SmooshedFileMapper.mapFile`
  (`processing/src/main/java/org/apache/druid/java/util/common/io/smoosh/SmooshedFileMapper.java:140`),
  one map per chunk file, cached in `buffersList` and `duplicate().position().limit().slice()`d per column
  (`:144-146`); unmapped explicitly by `ByteBufferUtils.unmap` in `close()` (`:158`).
  `IncrementalIndex` is fully on heap. GroupBy merge buffers are separate direct `ByteBuffer`s from the
  merge-buffer `BlockingPool`. Decompression scratch buffers are direct and pooled
  (`processing/src/main/java/org/apache/druid/segment/CompressedPools.java:87`, `:102`,
  `BUFFER_SIZE = 0x10000` at `:36`).
- **Lazy vs eager loading.** `IndexIO.loadIndex(File)` is **eager** (`lazy=false`,
  `processing/src/main/java/org/apache/druid/segment/IndexIO.java:197`) — every column is deserialized in
  `registerColumnHolder`'s else-branch (`:872-881`). `loadIndex(dir, true, loadFailed)` wraps each column in
  `Suppliers.memoize` (`:854-871`) and invokes `loadFailed.execute()` on first-touch failure (`:867`).
  Note the *mmap* itself (`Smoosh.map`, `:562`) and `index.drd` parsing are eager in both modes.
  `multiphaseMerge` uses the lazy form for intermediate tiers (`processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1228`).
- **Units.** `maxBytesInMemory`/`maxOnDiskStorage`/`maxMergingDictionarySize`/`buffer.sizeBytes` are bytes
  (`HumanReadableBytes`). `maxRowCount` is rows. `timeout`/`timeoutMs` are milliseconds
  (`processing/src/main/java/org/apache/druid/query/ChainedExecutionQueryRunner.java:147`, `processing/src/main/java/org/apache/druid/collections/DefaultBlockingPool.java:43`). `vectorSize` is rows per batch.
  Bitmap/index build times are reported in nanoseconds (`processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:748`, `:760`).

## Failure modes

| Failure | Where it surfaces (`path:LINE`) | Detection | Blast radius | Recovery / knob |
|---|---|---|---|---|
| Row/byte limit hit mid-ingest | `OnheapIncrementalIndex.java:411-433`; consumed at `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:351` | `canAppendRow()` polled *before* add | one sink; triggers persist | `maxRowsInMemory`, `maxBytesInMemory` |
| Row below index `minTimestamp` | `IncrementalIndex.java:492-498` | `DruidException.defensive` | one row → task fails | fix segmentGranularity / late-data handling |
| Dimension twice in one row | `processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:564` | `ISE` | one row → task fails | fix input schema |
| Per-column parse error | `IncrementalIndex.java:542-544`; `OnheapIncrementalIndex.java:354-366` | collected, not thrown | one column of one row | `maxParseExceptions` (indexing-service) |
| Parse error with `preserveExistingMetrics` | `OnheapIncrementalIndex.java:355-362` | `log.warn` + rethrow | task fails | only used by auto-compaction |
| Persist of empty index | `IndexMergerV9.java:1051-1053` | `IAE` | that persist | avoid persisting empty sinks |
| Interval/timestamp mismatch | `IndexMergerV9.java:1057-1064` | `IAE` | that persist/segment | fix segmentGranularity |
| Metric present in indexes but not requested | `processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1334`, `:1340` | `IAE` | the merge | keep `metricsSpec` stable |
| Sort order missing `__time` | `IndexMergerV9.java:1368-1372` | `DruidException.defensive` | the merge | fix `dimensionsSpec` ordering |
| Column part > ~2 GiB | `FileSmoosher.java:175-177` | `IAE` | the merge | smaller segments / `maxRowsPerSegment` |
| `index.drd` > 2 GiB | `IndexIO.java:213-218` (`checkFileSize`) | `IOE` | the merge | reduce column count |
| Unknown segment version | `IndexIO.java:208-210` | `ISE("Unknown index version[%s]")` | that segment | re-ingest |
| v9 loader version mismatch | `IndexIO.java:557-560` | `IAE("Expected version[9], got[%d]")` | that segment | re-ingest |
| Missing `version.bin` and `index.drd` | `processing/src/main/java/org/apache/druid/segment/SegmentUtils.java:82` | `IOE("Invalid segment dir")` | that segment | re-download |
| Corrupt `meta.smoosh` | `processing/src/main/java/org/apache/druid/java/util/common/io/smoosh/SmooshedFileMapper.java:67`, `:72`, `:75`, `:89` | `ISE` | that segment | re-download |
| Corrupt column bytes (lazy load) | `IndexIO.java:865-869` | warn + `loadFailed.execute()` + rethrow | that segment; callback can evict | eager load to fail fast |
| Merge buffers over-subscribed by one query | `GroupingEngine.java:172-176` | `ResourceLimitExceededException` | that query, fails fast | raise `numMergeBuffers` |
| Merge-buffer pool exhausted (timeout) | `GroupingEngine.java:187-193` (empty batch from `DefaultBlockingPool.java:158-179`) | `QueryCapacityExceededException` | that query | `numMergeBuffers`, query `timeout` |
| GroupBy disk-spill limit | `SpillingGrouper.java:196-201`; `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/LimitedTemporaryStorage.java:85`, `:211` | `TemporaryStorageFullException` → `DISK_FULL` → `ResourceLimitExceededException` at `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/GroupByMergingQueryRunner.java:375` | that query | `maxOnDiskStorage` |
| Merging dictionary full | `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/RowBasedGrouperHelper.java:1947`; `Groupers.java:72-74` | `AggregateResult.partial` | that query | `maxMergingDictionarySize` |
| Query timeout | `ChainedExecutionQueryRunner.java:161-165`; `GroupByMergingQueryRunner.java:366-368`, `:388-392` | `TimeoutException` → `QueryTimeoutException` | that query; workers interrupted | `timeout` context |
| Query cancelled / interrupted | `ChainedExecutionQueryRunner.java:152-160`; `processing/src/main/java/org/apache/druid/segment/FilteredOffset.java:45`, `:74`; `BaseQuery.java:52-57` | `QueryInterruptedException` | that query | — |
| Segment dropped mid-query | `processing/src/main/java/org/apache/druid/segment/ReferenceCountedSegmentProvider.java:168` → `ReferenceCountingCloseableObject.java:89-93` | `Optional.empty()` / `false` from `Phaser.register()` | that segment skipped | retry; timeline handles |
| Segment already unmapped when engine runs | `TimeseriesQueryEngine.java:99-103`; `GroupingEngine.java:488-492` | `SegmentMissingException` / `ISE` | that segment | should be prevented by refcount |
| `null` runner in fan-out | `processing/src/main/java/org/apache/druid/query/ChainedExecutionQueryRunner.java:97`, `:109` | `ISE` | that query | — |
| `vectorize=force` on a non-vectorizable query | `QueryContexts.java:199-201` | `ISE("Cannot vectorize!")` | that query | use `vectorize=true` |

**Row/byte limit hit mid-ingest.** Nothing throws. `canAppendRow` (`processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:411`)
computes `countCheck` and `sizeCheck`, writes one of three human-readable strings into the mutable field
`outOfRowsReason` (`:416`, `:423`, `:425`), and returns `false`. The field is plain (non-volatile) and
overwritten on every call, so it is only meaningful to the single ingest thread that just called it.
`StreamAppenderator.add` polls it (`:351`) and ORs the result with two independent counters it keeps itself
(`:363`, `:371`) before calling `persistAll`. If a caller ignored `canAppendRow` and kept calling `add`, the
index would keep growing — the limit is advisory. Two footguns: `maxBytesInMemory == 0` becomes
`Long.MAX_VALUE` (`:123`) meaning *unlimited*, and the byte accounting excludes dimension dictionaries
(see A3), so a high-cardinality datasource blows past the intended heap ceiling before the counter notices.

**Persist of empty index.** `IndexMergerV9.persist` throws `IAE("Trying to persist an empty index!")`
(`:1051-1053`) before doing any I/O. `isEmpty()` is `numEntries.get() == 0` (`IncrementalIndex.java:655-658`).
This matters because `getMinTime()`/`getMaxTime()` return `null` on an empty index (`:805-813`), which would
NPE the very next validation — so the guard is load-bearing, not cosmetic.

**Interval/timestamp mismatch.** `if (!(dataInterval.contains(firstTimestamp) && dataInterval.contains(lastTimestamp)))
throw new IAE("interval[%s] does not encapsulate the full range of timestamps[%s, %s]")` (`:1057-1064`).
Because Joda's `contains` is half-open, a row whose *truncated* timestamp equals the interval end fails.
The persist **aborts** — it does not silently drop or truncate rows. `getMaxTime()` is the truncated bucket
start (`getFacts().getMaxTimeMillis()`, `IncrementalIndex.java:681-684`), not the raw event time, so the
check is against bucketed values.

**Merge-buffer exhaustion.** Two distinct failures, both in `GroupingEngine.prepareResource`
(`processing/src/main/java/org/apache/druid/query/groupby/GroupingEngine.java:153`). First, a *structurally impossible* query:
`requiredMergeBufferNum > mergeBufferPool.maxSize()` → `ResourceLimitExceededException("Query needs N merge
buffers, but only M merge buffers were configured")` (`:172-176`) — this fails immediately and no amount of
waiting helps (subtotals and nested groupBys inflate `requiredMergeBufferNum`, `processing/src/main/java/org/apache/druid/query/groupby/GroupByQueryResources.java:114`).
Second, *transient contention*: `mergeBufferPool.takeBatch(n, context.getTimeout())` (`:183`).
`DefaultBlockingPool.pollObjects(n, timeoutMs)` (`processing/src/main/java/org/apache/druid/collections/DefaultBlockingPool.java:158`) takes a `ReentrantLock`
interruptibly, `notEnough.awaitNanos(nanos)` in a loop, and returns `Collections.emptyList()` once the
budget is exhausted (`:165-170`) — **all-or-nothing**, never a partial batch. The empty list becomes
`QueryCapacityExceededException.withErrorMessageAndResolvedHost("Cannot acquire N merge buffers. Try again
after current running queries are finished.")` (`:187-193`). Without a timeout, `takeObjects` waits
indefinitely (`:181-189`). Note the timeout is the *whole query* timeout, shared with execution.

**GroupBy disk-spill limit.** `SpillingGrouper.aggregate` (`processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/SpillingGrouper.java:177`) tries the in-buffer
grouper first; on failure (`!result.isOk()`, meaning the hash table is full) it calls `spill()` (`:196`) —
which writes the sorted grouper contents *and* the key dictionary to LZ4-compressed JSON temp files
(`:322-330`, `:332-340`) — then retries (`:207`). `LimitedTemporaryStorage.createFile` throws
`TemporaryStorageFullException` when `bytesUsed >= maxBytesUsed` (`processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/LimitedTemporaryStorage.java:85`), and the
output stream throws again if a write crosses the line (`:211-212`). `SpillingGrouper` catches it, latches
`diskFull = true`, and returns `DISK_FULL = AggregateResult.partial(0, "Not enough disk space to execute this
query. Try raising druid.query.groupBy.maxOnDiskStorage.")` (`:66-69`, `:198-201`). The latch is checked at the
top of every subsequent `aggregate` (`:179-184`) with an explicit comment that a *second segment* may start
processing before cancellation lands. The partial result propagates to
`GroupByMergingQueryRunner.waitForFutureCompletion`, which cancels all futures and throws
`ResourceLimitExceededException(result.getReason())` (`GroupByMergingQueryRunner.java:372-377`).
Spilling is off by default: `maxOnDiskStorage` defaults to 0 bytes (`processing/src/main/java/org/apache/druid/query/groupby/GroupByQueryConfig.java:99`), and
`temporaryStorage.maxSize() <= 0` makes `aggregate` return the raw failure without spilling (`:188`).

**Query timeout / cancellation.** `ChainedExecutionQueryRunner` registers the combined future with
`queryWatcher` (`:141`) *before* blocking, so an external cancel (Broker `DELETE`, `QueryScheduler`) can
reach it. On `TimeoutException` it calls `GuavaUtils.cancelAll(true, future, futures)` — `true` =
`mayInterruptIfRunning` — then throws `QueryTimeoutException` (`:161-164`). The `future` is cancelled *first*,
deliberately, so it settles as INTERRUPTED (comment `:154`). In-flight segment work is interrupted at two
checkpoints: `FilteredOffset#increment`'s `while (!Thread.currentThread().isInterrupted())` (`processing/src/main/java/org/apache/druid/segment/FilteredOffset.java:45`)
and `BaseQuery.checkInterrupted()`'s `Thread.interrupted()` → `QueryInterruptedException`
(`BaseQuery.java:52-57`). Crucially, a segment task that has already returned from `result.toList()`
(`:112`) has finished its work and its results are simply discarded — **the wasted work is per-segment,
not per-row**. `GroupByMergingQueryRunner` has the parallel structure (`:353-397`) plus a pre-check that
throws `QueryTimeoutException` if the budget is already spent before waiting (`:366-368`).

**Corrupt segment on load.** `IndexIO.loadIndex` (`:200`) first reads the version via
`SegmentUtils.getVersionFromDir` (`processing/src/main/java/org/apache/druid/segment/SegmentUtils.java:66`), which prefers `version.bin` and falls back to the
first byte of `index.drd`, throwing `IOE("Invalid segment dir [%s]. Can't find either of version.bin or
index.drd.")` if neither exists (`:82`). An unmapped version → `ISE("Unknown index version[%s]")`
(`processing/src/main/java/org/apache/druid/segment/IndexIO.java:209`). `V9IndexLoader.load` re-reads and re-checks `version.bin` → `IAE("Expected version[9],
got[%d]")` (`:557-560`). `Smoosh.map` parses `meta.smoosh` and throws `ISE` on a bad header line, wrong
version token, or wrong field count (`processing/src/main/java/org/apache/druid/java/util/common/io/smoosh/SmooshedFileMapper.java:67`, `:72`, `:75`, `:89`). A **truncated
`.smoosh` chunk** is the nasty case: `meta.smoosh` offsets still parse, `mapFile` still returns a slice, and
the failure only appears when a column is deserialized — eagerly at load (`IndexIO.java:873-879`, exception
propagates and load fails) or lazily on first query, where `registerColumnHolder` logs
`"Throw exceptions when deserialize column [%s]"`, calls `loadFailed.execute()`, and rethrows (`:865-869`).
`mapFile` returns `null` for an unknown entry name (`SmooshedFileMapper.java:129-131`) — an NPE waiting to
happen at the call site rather than a clean error.

**Segment dropped mid-query.** The guarantee is a `Phaser`, not a counter.
`ReferenceCountingCloseableObject` holds `referents = new Phaser(1)` whose `onAdvance` closes `baseObject`
and always returns `true` to terminate (`ReferenceCountingCloseableObject.java:44-67`). `increment()` is
`referents.register() >= 0` — a **negative return means the Phaser is already terminated**, i.e. the segment
is closed (`:89-93`). `close()` is `closed.compareAndSet(false, true)` then `arriveAndDeregister()` (`:136-143`),
which drops the initial party; the segment is only actually closed when the party count reaches zero, i.e.
after the last in-flight reader deregisters. `acquireReference()` (`processing/src/main/java/org/apache/druid/segment/ReferenceCountedSegmentProvider.java:168`)
composes these: `incrementReferenceAndDecrementOnceCloseable()` returns `Optional.empty()` if `increment()`
failed (`ReferenceCountingCloseableObject.java:107-117`), and the returned `Closer` decrements exactly once
(`decrementOnceCloseable`, `:123-133`, with a warn on double-close). Therefore **a drop concurrent with an
acquire either loses the race (reader gets `empty()` and the segment is skipped) or the drop is deferred
until the reader's `ReferenceClosingSegment` is closed — the mmap can never be unmapped under an active
reader.** The remaining hazard is a *leaked* reference: the Phaser never advances, `close()` never unmaps,
and the segment file stays pinned. The constructor also refuses to wrap a `ReferenceClosingSegment`
(`ReferenceCountedSegmentProvider.java:104-109`) because the inner reference would be silently ignored.

## Optimizations in the current structure

- **Bitmap prefiltering with cost-ordered short-circuits.** `AndFilter.makeFilterBundle` intersects
  child bitmaps in ascending `estimatedIndexComputeCost` order and returns `FilterBundle.allFalse(...)` the
  moment any child yields an empty bitmap (`AndFilter.java:167-174`), feeding `indexIntersectionSize` forward
  as the next child's `applyRowCount` (`:162`, `:183`). *Cost*: the bitmaps that were already computed are
  wasted work, and building each index requires reading the whole `.inverted` entry for every matching
  dictionary value. The cost model is crude — `estimatedComputeCost()` is literally `dictionarySize`
  (`DictionaryScanningBitmapIndex.java:53-56`) — so a low-cardinality but highly non-selective column sorts
  first even though it eliminates nothing.
- **Index-vs-matcher crossover.** `DictionaryScanningBitmapIndex.computeBitmapResult` returns `null` when
  `applyRowCount != totalRowCount && applyRowCount < dictionarySize * scaleThreshold` (`:67-69`), converting
  that clause into a `ValueMatcher` instead. *Cost*: a `null` index forces `needMatcher = true`
  (`processing/src/main/java/org/apache/druid/query/filter/Filter.java:96`), and the matcher runs per surviving row inside `FilteredOffset#increment`
  (`FilteredOffset.java:43-51`) — a row-at-a-time virtual call per predicate.
- **Vectorized execution.** Batches of `vectorSize` rows (default 512, `processing/src/main/java/org/apache/druid/query/QueryContexts.java:151`) through
  `VectorOffset` implementations; `asVectorCursor` even uses binary search for interval bounds
  (`processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:263`, `:269`) where the row-at-a-time path linearly scans (`:184-188`).
  *Cost*: `canVectorize()` has four independent veto conditions plus GroupBy's five
  (`QueryableIndexCursorHolder.java:125-151`, `VectorGroupByEngine.java:174-205`), so the fast path is easy to
  lose silently; the check itself forces `CursorResources` construction (`:141`), meaning bitmap indexes are
  built even if the query then falls back; and the filtered vector path builds a *second* selector factory
  over the filtered offset (`:298-301`).
- **Dictionary encoding + compression.** Dimensions are stored as compressed int codes against a sorted
  dictionary; `IndexSpec` defaults are LZ4 for dimensions and metrics
  (`processing/src/main/java/org/apache/druid/segment/data/CompressionStrategy.java:127`), `LONGS` long encoding (`processing/src/main/java/org/apache/druid/segment/data/CompressionFactory.java:67`), and Roaring bitmaps
  (`processing/src/main/java/org/apache/druid/segment/data/BitmapSerde.java:35`, `DefaultBitmapSerdeFactory extends RoaringBitmapSerdeFactory`). *Cost*: the sorted
  dictionary only exists post-merge, so merging must carry `conversions[i]` id maps and `rowNumConversions`
  row maps and rebuild **every** bitmap dictId-by-dictId (`DictionaryEncodedColumnMerger.java:412-423`) —
  merge cost is O(total dictionary size × adapters), and decompression needs pooled 64 KiB scratch buffers
  (`processing/src/main/java/org/apache/druid/segment/CompressedPools.java:36`).
- **mmap + page-cache reliance.** No segment bytes are copied into heap:
  `SmooshedFileMapper.mapFile` maps once per chunk and hands out zero-copy slices (`:138-146`).
  *Cost*: query latency is at the mercy of the OS page cache; a cold segment turns a cursor scan into
  synchronous major faults on a processing thread, which is invisible to Druid's own metrics. Unmapping is
  manual (`ByteBufferUtils.unmap`, `:158`) and only safe because of the refcount protocol above.
- **Lazy `Sequence` streaming.** `BaseSequence.toYielder` stops at the first yield and resumes the same
  iterator (`BaseSequence.java:61-92`, `:111-124`); `MergeSequence` holds one `Yielder` per input in a
  `PriorityQueue` (`MergeSequence.java:55-60`). *Cost*: laziness is broken in two specific places —
  `ChainedExecutionQueryRunner`'s `result.toList()` per segment (`:112`) and its `MergeIterable` over the
  fully-materialized `future.get()` results (`:145-150`). So a Historical holds *all* per-segment results for
  a query in heap simultaneously, regardless of how lazily the client consumes them.
- **Object/buffer pooling.** `StupidPool` (`processing/src/main/java/org/apache/druid/collections/StupidPool.java:41`)
  backs processing buffers and `CompressedPools`' direct decompression buffers; it caps retention at
  `objectsCacheMaxCount` (`:113`, `:217`) and counts leaks via a `Cleaner`-based notifier (`:107`, `:342`).
  Merge buffers use `DefaultBlockingPool` with a fixed `ArrayDeque` preallocated at construction
  (`DefaultBlockingPool.java:59-64`) and returned by the `ReferenceCountingResourceHolder`'s cleanup
  (`:86-89`). Aggregators reuse a shared `CachingColumnSelectorFactory` for all non-complex metrics
  (`OnheapIncrementalIndex.java:190-206`). `ColumnCache` memoizes `ColumnHolder`s per cursor
  (`processing/src/main/java/org/apache/druid/segment/ColumnCache.java:51`). *Cost*: pool sizing is a hard capacity ceiling — see merge-buffer exhaustion —
  and pooled direct memory is invisible to heap tuning.
- **Per-segment result caching hooks.** `CacheStrategy` (`processing/src/main/java/org/apache/druid/query/CacheStrategy.java`)
  exposes `isCacheable(query, willMergeRunners, bySegment)` (`:69`), `computeCacheKey` (`:83`),
  `prepareForSegmentLevelCache()` / `pullFromSegmentLevelCache()` (`:127`, `:133`). The wrapper that uses them
  (`CachingQueryRunner`) lives in `server/`. *Cost*: cache keys must encode every semantic input, and
  `prepareForCache(isResultLevelCache)` forks behavior between segment-level and result-level caching —
  divergence between the two is a correctness risk, not a perf one.

## Concurrency & thread model

- **`QueryProcessingPool`** (`processing/src/main/java/org/apache/druid/query/QueryProcessingPool.java:40`)
  extends `ListeningExecutorService` with one extra method, `submitRunnerTask(PrioritizedQueryRunnerCallable)`
  (`:50`). Production impls: `ForwardingQueryProcessingPool` (`processing/src/main/java/org/apache/druid/query/ForwardingQueryProcessingPool.java:33`),
  `MetricsEmittingQueryProcessingPool` (`:26`), `DirectQueryProcessingPool` (caller-thread, `:30`).
  The underlying executor is `PrioritizedExecutorService.create` — a fixed `ThreadPoolExecutor` with
  `corePoolSize == maxPoolSize == config.getNumThreads()`, a `PriorityBlockingQueue`, and daemon threads named
  by `formatString` (`PrioritizedExecutorService.java:50-58`). Tasks are wrapped as
  `PrioritizedListenableFutureTask` carrying `getPriority()` and, when `config.isFifo()`, a monotonically
  *decreasing* `queuePosition` used as the tiebreaker so equal-priority tasks run FIFO (`:119-125`, `:135-141`).
  `shutdownNow()` on lifecycle stop (`:73`).
- **Merge-buffer `BlockingPool`.** `DefaultBlockingPool` (`processing/src/main/java/org/apache/druid/collections/DefaultBlockingPool.java:41`) is a fixed
  `ArrayDeque` guarded by a `ReentrantLock` + `notEnough` `Condition` (`:46-49`). All acquisition is
  **batch, all-or-nothing**: `pollObjects(n)` returns empty if fewer than `n` are free (`:144-146`);
  `pollObjects(n, timeoutMs)` waits on `awaitNanos` (`:165-170`); `takeObjects(n)` waits forever (`:181-189`).
  `pendingRequests` is tracked for metrics (`:98`, `:127-130`). Buffers are handed out as
  `ReferenceCountingResourceHolder` whose cleanup `offer`s them back (`:84-90`), which is why a leaked
  holder permanently shrinks the pool. Reservation happens once per query in
  `GroupByResourcesReservationPool` (`:94`, `:143`) rather than per execution unit — so the toolchest-merge
  and merging-runner allotments are carved out of one batch (`GroupingEngine.java:195-198`).
- **Ingest-side single-writer assumption on `IncrementalIndex`.** `addToFacts` is documented as not needing
  thread safety (`processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:384`) and `add` as "Not thread-safe" (`:460`). The concurrent types
  present — `ConcurrentSkipListMap` in `RollupFactsHolder` (`processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:748`),
  `ConcurrentHashMap` for `aggregators` (`:92`), `AtomicInteger`/`AtomicLong` counters — exist so *readers*
  (persist iteration, realtime cursors) see a consistent view. `RollupFactsHolder.putIfAbsent` even documents
  the ordering requirement: "`setRowIndex()` must be called before `facts.putIfAbsent()` for visibility of
  rowIndex from concurrent readers" (`:816-817`). `minTime`/`maxTime` are `volatile` but updated with
  non-atomic `Math.min`/`Math.max` (`:739-740`, `:818-819`) — correct only under a single writer.
  Actual ingest-side locks: `synchronized (dimensionDescs)` around dimension discovery
  (`processing/src/main/java/org/apache/druid/segment/incremental/IncrementalIndex.java:505`, also `:422`, `:436`, `:450`, `:707`) and `private synchronized updateMaxIngestedTime`
  (`:647`).
- **Where contention happens.**
  1. `dimensionDescs` monitor — held for the whole dimension-processing loop of every row
     (`IncrementalIndex.java:505-574`), contended between the ingest thread and any reader calling
     `getColumnCapabilities`/`getColumnFormats`/`getDimensionNames`.
  2. The merge-buffer `ReentrantLock` — every groupBy acquire/release serializes here, and blocked threads
     are *processing* threads (`DefaultBlockingPool.java:141-179`).
  3. `ConcurrentGrouper`'s per-sub-grouper monitors — pre-spill, threads hash to
     `groupers.get(grouperNumberForKeyHash(keyHash))` and `synchronized (subGrouper)`
     (`ConcurrentGrouper.java:295-297`), explicitly trading contention for lower memory ("There is potential
     here for thread contention, but it reduces memory use", `:293-294`). Once any sub-grouper fills,
     `spilling = true` (`:309`) and every thread switches to `synchronized (tlGrouper)` on its own
     thread-local grouper — no cross-thread contention, higher memory. With `mergeThreadLocal` it starts in
     thread-local mode (`:283-285`, `:253-255`).
  4. `PriorityBlockingQueue` in `PrioritizedExecutorService` — a single lock for all submissions.
  5. Disk: `SpillingGrouper.spill()` runs **on the processing thread** ("Warning: this can potentially block
     up a processing thread for a while", `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/SpillingGrouper.java:194`).
  6. `ColumnCache` is a plain `HashMap` (`processing/src/main/java/org/apache/druid/segment/ColumnCache.java:34`, `:43`) — safe only because a `CursorHolder`
     is single-threaded.

## Config knobs that change execution

| Property | Class field (`path:LINE`) | Default | What it changes in the code path |
|---|---|---|---|
| `druid.processing.numThreads` | `processing/src/main/java/org/apache/druid/query/DruidProcessingConfig.java:44`, defaulted `:72-75` | `max(availableProcessors - 1, 1)` | core/max size of the `ThreadPoolExecutor` in `PrioritizedExecutorService.create` (`:52-53`); the parallelism of `ChainedExecutionQueryRunner`'s fan-out |
| `druid.processing.numMergeBuffers` | `processing/src/main/java/org/apache/druid/query/DruidProcessingConfig.java:46`, defaulted `:76` | `max(2, numThreads / 4)` | `DefaultBlockingPool` size; `maxSize()` compared against `requiredMergeBufferNum` at `processing/src/main/java/org/apache/druid/query/groupby/GroupingEngine.java:172` |
| `druid.processing.buffer.sizeBytes` | `processing/src/main/java/org/apache/druid/query/DruidProcessingBufferConfig.java:36`; consumed `DruidProcessingConfig.java:93-134` | auto: `min(directMem / (numThreads + numMergeBuffers + 1), 1 GiB)` (`:118-123`, cap at `processing/src/main/java/org/apache/druid/query/DruidProcessingBufferConfig.java:32`) | size of each processing + merge buffer; sliced per-thread by `ConcurrentGrouper.init` (`sliceSize = capacity / concurrencyHint`, `:230`). Explicit values > 2 GiB throw `IAE` (`:97-99`) |
| `druid.processing.fifo` | `processing/src/main/java/org/apache/druid/query/DruidProcessingConfig.java:48`, `:77` | `true` | whether `PrioritizedExecutorService` sets the FIFO tiebreaker `queuePosition` (`:124`, `:140`) |
| `druid.processing.tmpDir` | `processing/src/main/java/org/apache/druid/query/DruidProcessingConfig.java:50`, `:78` | `java.io.tmpdir` | parent of the `druid-groupBy-<uuid>_<queryId>` spill dir (`GroupByMergingQueryRunner.java:165-168`) |
| `druid.processing.indexes.skipValueRangeIndexScale` | `DruidProcessingConfig.java:178-181` | see `DruidProcessingIndexesConfig` | `scaleThreshold` for `DictionaryRangeScanningBitmapIndex`, i.e. when range indexes are declined in favor of matchers |
| groupBy `maxOnDiskStorage` | `processing/src/main/java/org/apache/druid/query/groupby/GroupByQueryConfig.java:99`; per-query `:328-334` | `0` bytes (spilling disabled) | `LimitedTemporaryStorage.maxBytesUsed` (`GroupByMergingQueryRunner.java:190-194`); `temporaryStorage.maxSize() <= 0` makes `SpillingGrouper.aggregate` skip `spill()` entirely (`:188`) |
| groupBy `maxMergingDictionarySize` | `processing/src/main/java/org/apache/druid/query/groupby/GroupByQueryConfig.java:95`, `:205-224`; per-query `:341` | `AUTOMATIC` (derived from heap) | divided by `concurrencyHint` for per-thread key serdes (`RowBasedGrouperHelper.java:256-262`), full value for the combining serde (`:291`); enforced at `:1947` → `Groupers.DICTIONARY_FULL_ZERO_COUNT` (`processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/Groupers.java:72`) |
| groupBy `bufferGrouperMaxSize` | `processing/src/main/java/org/apache/druid/query/groupby/GroupByQueryConfig.java:80`, `:316` | `Integer.MAX_VALUE` | max entries per `BufferHashGrouper` before it reports full (and therefore before spilling starts) |
| groupBy `numParallelCombineThreads` | `processing/src/main/java/org/apache/druid/query/groupby/GroupByQueryConfig.java:120`, `:358` | `1` | `> 1` requests **two** merge buffers instead of one (`processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/GroupByMergingQueryRunner.java:201`) and enables `parallelCombiner` (`ConcurrentGrouper.java:203-217`) |
| groupBy `groupByIsSingleThreaded` | `processing/src/main/java/org/apache/druid/query/groupby/GroupByQueryConfig.java:137`, `:315` | `false` | when true, `GroupByMergingQueryRunner` waits for each segment future immediately after submitting it (`:279-286`) instead of once at the end (`:294-296`) |
| `vectorize` | `processing/src/main/java/org/apache/druid/query/QueryContexts.java:54`, `:149`; `processing/src/main/java/org/apache/druid/query/QueryContext.java:326` | `TRUE` | `Vectorize.shouldVectorize(canVectorize)`: `FALSE` never vectorizes, `TRUE` silently falls back, `FORCE` throws `ISE("Cannot vectorize!")` (`QueryContexts.java:181-205`). Branch points: `processing/src/main/java/org/apache/druid/query/timeseries/TimeseriesQueryEngine.java:114`, `processing/src/main/java/org/apache/druid/query/groupby/GroupingEngine.java:521` |
| `vectorizeVirtualColumns` | `processing/src/main/java/org/apache/druid/query/QueryContexts.java:55`, `:150` | `TRUE` | first veto in `CursorHolder.canVectorize` (`QueryableIndexCursorHolder.java:128-132`) |
| `vectorSize` | `processing/src/main/java/org/apache/druid/query/QueryContexts.java:56`, `:151` | `512` | rows per `VectorOffset` batch (`processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:108`, `:278-279`) |
| `timeout` / `defaultTimeout` | `processing/src/main/java/org/apache/druid/query/QueryContexts.java:46`, `:49`, `:154`; `QueryContext.java:484-518` | `300000` ms | `future.get(timeout, MS)` in `processing/src/main/java/org/apache/druid/query/ChainedExecutionQueryRunner.java:147`; the same budget covers `mergeBufferPool.takeBatch(n, timeout)` (`processing/src/main/java/org/apache/druid/query/groupby/GroupingEngine.java:183`) and `timeoutAt` in `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/GroupByMergingQueryRunner.java:179` |
| `priority` | `processing/src/main/java/org/apache/druid/query/QueryContexts.java:44`; `processing/src/main/java/org/apache/druid/query/QueryContext.java:395` | `0` | priority of every `AbstractPrioritizedQueryRunnerCallable` (`processing/src/main/java/org/apache/druid/query/ChainedExecutionQueryRunner.java:101`, `processing/src/main/java/org/apache/druid/query/groupby/epinephelinae/GroupByMergingQueryRunner.java:251`) → ordering in the `PriorityBlockingQueue` |
| `cursorAutoArrangeFilters` | read at `processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:118` | `true` | enables cost-based child-filter sorting in `FilterBundle.Builder` (`:183-189`); when false all costs become `Integer.MAX_VALUE` (`:188`) |
| `maxRowsInMemory` | consumed at `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:363`; `AppendableIndexBuilder.setMaxRowCount` (`:87`) → `OnheapIncrementalIndex.maxRowCount` (`:95`) | tuningConfig | `canAppendRow`'s `countCheck` (`processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:413`); `<= 0` is rejected at build time (`AppendableIndexBuilder.java:107-108`) |
| `maxBytesInMemory` | `processing/src/main/java/org/apache/druid/segment/incremental/AppendableIndexBuilder.java:33`, `:93` → `processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:96`, `:123` | tuningConfig | `canAppendRow`'s `sizeCheck` (`:414`). **`0` means unlimited** (`:123`); `-1` disables the check only |
| `indexSpec.bitmap` | `processing/src/main/java/org/apache/druid/segment/IndexSpec.java:50`, `:90-92` | `DefaultBitmapSerdeFactory` = Roaring (`processing/src/main/java/org/apache/druid/segment/data/BitmapSerde.java:35`) | bitmap factory for `IncrementalIndexAdapter` build (`processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1074`) and for `<col>.inverted` writing (`DictionaryEncodedColumnMerger.java:390-401`) |
| `indexSpec.dimensionCompression` | `IndexSpec.java:93-95` | `LZ4` (`processing/src/main/java/org/apache/druid/segment/data/CompressionStrategy.java:127`) | `UNCOMPRESSED` switches the encoded-value serializer from `CompressedVSizeColumnarInts` to `VSizeColumnarInts` (`DictionaryEncodedColumnMerger.java:463-482`, flag at `processing/src/main/java/org/apache/druid/segment/StringDimensionMergerV9.java:154`) |
| `indexSpec.metricCompression` / `longEncoding` | `IndexSpec.java:100-106` | `LZ4` / `LONGS` (`processing/src/main/java/org/apache/druid/segment/data/CompressionFactory.java:67`) | `createLongColumnSerializer` for `__time` and long metrics (`IndexMergerV9.java:891-895`, `:677`) |
| `indexSpec.stringDictionaryEncoding` | `IndexSpec.java:96-98` | `StringEncodingStrategy.DEFAULT` | which dictionary writer `StringDimensionMergerV9` uses (`:121-125`) |
| `indexSpec.segmentLoader` | `processing/src/main/java/org/apache/druid/segment/IndexSpec.java:108` | `null` → `MMappedQueryableSegmentizerFactory` | contents of `factory.json` (`IndexMergerV9.java:168-175`) |
| `segmentWriteOutMediumFactory` | `processing/src/main/java/org/apache/druid/segment/writeout/SegmentWriteOutMediumFactory.java:30-35` | `tmpFile` | on-heap-then-spill temp files (`processing/src/main/java/org/apache/druid/segment/writeout/TmpFileSegmentWriteOutMedium.java:76`) vs `offHeapMemory` vs `onHeapMemory` during merge (`IndexMergerV9.java:157-160`) |
| `maxColumnsToMerge` | `IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE`; `processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1167`, `:1180` | `-1` (unlimited) for `persist` (`:1089`) | `> 0` switches on multi-tier merging with temp dirs and lazy re-loads (`:1185-1235`) |

## Sharp edges

1. **`canVectorize()` builds the filter bundle.** `QueryableIndexCursorHolder.canVectorize` calls
   `resourcesSupplier.get()` (`:141`), which constructs `CursorResources` and therefore computes every bitmap
   index. Deciding *not* to vectorize costs you the full index-construction work anyway.
2. **`maxBytesInMemory == 0` means unlimited, not zero.** `processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:123` maps `0` to
   `Long.MAX_VALUE`. `-1` is the "ignore the size check" sentinel (`:414`). Only `> 0` is a real limit.
3. **`bytesInMemory` excludes dimension dictionaries.** The accounting at `:271-275` covers the row key,
   aggregators, and a 44-byte constant — not the `DimensionIndexer` dictionaries, not the `aggregators`
   `ConcurrentHashMap` entries. High-cardinality dimensions overshoot `maxBytesInMemory` silently.
4. **`outOfRowsReason` is a mutable non-volatile field** (`processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:108`) rewritten by
   every `canAppendRow()` call (`:416-427`). It is a diagnostic for the calling thread only, and it is *not*
   plumbed through `IncrementalIndexAddResult` (the `reasonOfNotAdded` constructor is used only by `Sink`,
   `server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:70`, `:73`).
5. **`addToFacts` has no lock.** The defensive throw at `processing/src/main/java/org/apache/druid/segment/incremental/OnheapIncrementalIndex.java:267` is the *only* thing
   standing between a second writer and silently lost aggregations. Anything that calls `add()` from two
   threads is a bug that manifests as `DruidException.defensive("Encountered existing fact entry for new
   key, possible concurrent add?")` — or worse, as a lost `doAggregate` on the rollup-hit branch (`:248-250`),
   which has no such guard.
6. **`multiphaseMerge` deletes `outDir` first.** `FileUtils.deleteDirectory(outDir)` at
   `processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1162` — passing a directory with anything else in it destroys it.
7. **Half-open intervals bite at persist.** A row whose truncated timestamp equals `dataInterval.getEnd()`
   fails the `contains` check (`:1057`) and aborts the whole persist. `getMaxTime()` is the *bucketed* max
   (`IncrementalIndex.java:681-684`), so coarse `queryGranularity` can push it onto the boundary.
8. **`FilterBundle` returning `null` is not "no filter".** `makeFilterBundle` returns `null` only when the
   filter itself is `null` (`QueryableIndexCursorHolder.java:745-747`). A non-null filter with no usable index
   returns a bundle with `getIndex() == null` and a matcher — the two are checked separately at `:171` and
   `:219`. Conflating them means either scanning all rows or dropping the predicate.
9. **`FilteredOffset` can't be cloned** (`FilteredOffset.java:85-90`), which is why `asCursor` clones the
   *base* offset before wrapping (`processing/src/main/java/org/apache/druid/segment/QueryableIndexCursorHolder.java:212`).
10. **Interrupted `FilteredOffset.increment()` leaves an illegal state.** It returns early on interrupt
    (`:45-50`) possibly pointing at a row that should have been filtered; only the explicit
    `BaseQuery.checkInterrupted()` at `:74` prevents that row from being read. Any new caller of
    `increment()` outside `incrementIfNeededOnCreationOrReset` inherits this hazard.
11. **`ChainedExecutionQueryRunner` materializes twice.** `result.toList()` per segment (`:112`) and
    `future.get()` for all segments (`:147`) — a Historical holds every per-segment result list for a query
    in heap at once, no matter how lazily the Broker consumes the response.
12. **`DISK_FULL` is sticky and cross-segment.** `SpillingGrouper.diskFull` is latched (`:199`) and returned
    for all subsequent `aggregate` calls (`:179-184`) precisely because the *next segment* may start before
    cancellation propagates. A single spill-limit hit poisons the rest of the query on that thread.
13. **Merge buffers are reserved once per query, not per execution unit.**
    `GroupByResourcesReservationPool` (`:94`, `:143`) splits one batch between toolchest merge and the
    merging runner (`GroupingEngine.java:195-198`); asking for more later throws
    `DruidException.defensive` rather than blocking (`GroupByMergingQueryRunner.java:336-344`).
14. **`SmooshedFileMapper.mapFile` returns `null` for unknown names** (`:129-131`) — silent NPE-at-a-distance
    rather than a clear "missing column" error.
15. **Lazy segment load defers corruption to query time.** `loadIndex(dir, true, cb)` memoizes suppliers
    (`IndexIO.java:854-871`); a bad column surfaces as a query failure plus `loadFailed.execute()`, not as a
    load failure. Eager `loadIndex(File)` (`:197`) trades startup time for fail-fast.
16. **`getMinTime()`/`getMaxTime()` return `null` on an empty index** (`IncrementalIndex.java:805-813`).
    Only the `isEmpty()` guard at `processing/src/main/java/org/apache/druid/segment/IndexMergerV9.java:1051` keeps the next line from NPE-ing.
17. **`Sink`'s "not writable" / "write after index swapped" results carry `rowCount = -1, bytesInMemory = -1`**
    (`server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:70`, `:73`), which
    `StreamAppenderator` turns into `SegmentNotWritableException` via a negative-value check
    (`StreamAppenderator.java:332-334`) — a sentinel, not a real measurement.
