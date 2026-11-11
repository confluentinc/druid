# Code deep dive — druid-server

Execution mechanics, not class inventory. This traces the *runtime* of two paths through
`server/`: (A/B) the `index_kafka` ingestion path — appenderator persist → merge → S3 push →
one metadata transaction (segments + Kafka offsets) → Coordinator load loop → Historical load +
announce → Broker view update → task handoff released; and (C) the query path Broker → Historicals.
Every claim below comes from a method body read in this checkout (branch `34.0.0-confluent`,
Druid 34.0.0); line numbers verified with `grep -n`. Compaction, lookups, SQL planning, security,
MSQ and ZK-based legacy paths are out of scope.

**Read `server/CLAUDE.md` first** — it maps classes → responsibilities. This file assumes that map
and goes to method-body granularity: which thread runs what, what state is mutated, what the return
value gates, and where each failure is actually caught.

> Note: the handoff poll period default is **`PT1S`** —
> `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifierConfig.java:29`.

---

## Happy path, traced

### (A) Task side: add → persist → merge → push → metadata commit

All of (A) runs inside the peon JVM. Three executors matter, all created in
`StreamAppenderator.initializeExecutors()`
(`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1230`):

| Executor | Construction | Threads | Queue |
|---|---|---|---|
| `persistExecutor` | `Execs.newBlockingThreaded("[id]-appenderator-persist", numPersistThreads, maxPendingPersists)` (`:1237`) | `numPersistThreads` | bounded `maxPendingPersists` — **submitter blocks** |
| `pushExecutor` | `Execs.newBlockingSingleThreaded("[id]-appenderator-merge", 1)` (`:1247`) | 1 | 1 |
| `intermediateTempExecutor` | `Execs.newBlockingSingleThreaded("[id]-appenderator-abandon", 0)` (`:1254`) | 1 | 0 (SynchronousQueue) |

**A1. `StreamAppenderator.add()`** — `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:300`, runs on the **task's read-loop
thread** (`SeekableStreamIndexTaskRunner.runInternal`).

- `throwPersistErrorIfExists()` (`:307`, body `:292`) rethrows any `persistError` recorded
  asynchronously by a prior persist. **This is the only place a background persist failure surfaces**
  — it kills the *next* `add()`, not the persist call site.
- `getOrCreateSink(identifier)` (`:317`, body `:503`): on first row for a segment id, constructs
  `Sink` with `tuningConfig.getMaxRowsInMemory()` / `maxBytesTuningConfig` (`:513-515`), charges
  `ROUGH_OVERHEAD_PER_SINK` (5000, `:124`) to `bytesCurrentlyInMemory` (`:517`), inserts into the
  `sinks` `ConcurrentHashMap` (`:520`) **before** `segmentAnnouncer.announceSegment()` (`:523`) so the
  sink is queryable the instant it is announced. An announcement `IOException` is only alerted, not
  thrown (`:525-529`).
- `sink.add(row)` (`:325`) → `Sink.add`
  (`server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:173`):
  under `synchronized (hydrantLock)` (`:179`) it returns `NOT_WRITABLE` if `finishWriting()` already
  ran (`:180-182`), `ALREADY_SWAPPED` if the current hydrant's index is null (`:185-187`), else
  `currHydrant.getIndex().add(row)` (`:189`). A negative row count from the result makes `add()`
  throw `SegmentNotWritableException` (`StreamAppenderator.java:332-334`).
- Counters mutated (all atomics, `:343-345`): `rowsCurrentlyInMemory += delta`,
  `bytesCurrentlyInMemory += delta`, `totalRows += delta`. These are **process-wide across all
  sinks** of this appenderator — that is how one task bounds memory over many concurrent segments.
- Four independent persist triggers (`:351-378`), each appending a human-readable reason:
  `!sink.canAppendRow()` (`:351`), `currTs > nextFlush` (`:355`, i.e. `intermediatePersistPeriod`
  elapsed — `nextFlush` set in `resetNextFlush()` `:1278-1280`),
  `rowsCurrentlyInMemory >= maxRowsInMemory` (`:363`), `bytesCurrentlyInMemory >= maxBytesInMemory`
  (`:371`).
- If persisting and `allowIncrementalPersists`: for every sink it pre-charges the memory that will
  *stay* resident after the swap — `calculateMMappedHydrantMemoryInUsed(sink.getCurrHydrant())`
  (`:393-394`) — because a swapped hydrant becomes an mmapped `QueryableIndexSegment` that still
  costs heap. Then the **hard safety valve** (`:399-427`): if
  `bytesCurrentlyInMemory - bytesToBePersisted > maxBytesInMemory` even after this persist, it emits
  an alert and throws `RuntimeException` ("Task has exceeded safe estimated heap usage limits") —
  disableable via `skipBytesInMemoryOverheadCheck`.
- `Futures.addCallback(persistAll(...), ...)` on `directExecutor()` (`:429-446`): `onFailure` merely
  sets `persistError = t` (`:442`). Return: `AppenderatorAddResult(identifier, sink.getNumRows(),
  isPersistRequired)` (`:452`) — `isPersistRequired` is true only when
  `allowIncrementalPersists == false`, and is what the task runner reads as `isPushRequired()`.

**A2. `persistAll(committer)`** — `:631`. Runs on the caller thread up to the submit, then on
`persistExecutor`.

- Caller thread: walks `sinks` (`:641`). For each sink, `limit = isWritable ? size-1 : size`
  (`:653`) — the *current* hydrant is excluded because it is still being written; every earlier
  hydrant that has not `hasSwapped()` is queued (`:656-662`). If `sink.swappable()` (non-empty
  current index, `server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:222`), `sink.swap()` (`:666`) is called →
  `Sink.makeNewCurrIndex` (`server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:307`): builds a fresh `IncrementalIndex` sized by
  `maxRowCount`/`maxBytesInMemory` (`:323-324`), under `hydrantLock` promotes
  `numRowsExcludingCurrIndex += old.numRows()` (`:376`), appends the new hydrant (`:378`), and
  **returns the old one** for persisting. It throws `ISE("finishWriting() called during swap")`
  (`:382`) if the sink was finished concurrently.
- `persistExecutor.submit(...)` (`:676`): per pair, `persistHydrant()` (`:685`, body `:1622`) —
  `synchronized (indexToPersist)` (`:1624`), no-op if already swapped (`:1625-1632`),
  `indexMerger.persist(...)` writes hydrant `N` to `<persistDir>/N` using
  `indexSpecForIntermediatePersists` (`:1642-1648`), then `swapSegment(new QueryableIndexSegment(...))`
  (`:1658`) replaces the in-memory index with the mmapped one. Returns rows persisted.
- Then `committer.run()` (`:704`) and, under `commitLock` (`:707-718`), a read-merge-write of
  `<basePersistDir>/commit.json`: `readCommit()` → merge hydrant counts → `writeCommit(new
  Committed(commitHydrants, commitMetadata))` (`:715`). This on-disk commit file is what makes a
  peon restart resumable.
- Back on the caller thread (`:755-767`): `metrics.incrementPersistBackPressureMillis(startDelay)`
  and a warn if the submit blocked > `WARN_DELAY` (1000ms, `:129`) — **this is the observable signal
  that `maxPendingPersists` is throttling ingestion**; then `rowsCurrentlyInMemory -=
  numPersistedRows`, `bytesCurrentlyInMemory -= bytesPersisted`, `resetNextFlush()`.
- Returns `ListenableFuture<Object>` resolving to the commit metadata (Kafka offsets), or `null` if
  `committer == null` (`:740`).

**A3. `push(identifiers, committer, useUniquePath=true)`** — `:773`.

- Caller thread: `sink.finishWriting()` per identifier (`:787`) makes the sink permanently
  non-writable and subtracts its rows from `totalRows`.
- `Futures.transform(persistAll(committer), <merge fn>, pushExecutor)` (`:794-849`) — the merge
  function runs on the **single-threaded** `pushExecutor`, i.e. **all merges for one task are
  serialized**. Sinks in `droppingSinks` are skipped (`:813-816`). Returns
  `SegmentsAndCommitMetadata(dataSegments, commitMetadata, segmentSchemaMapping)` (`:846`).

**A4. `mergeAndPush(identifier, sink, useUniquePath)`** — `:874`, on `pushExecutor`.

- Identity guard: `if (sinks.get(identifier) != sink) return null` (`:882-885`) — a concurrently
  dropped/replaced sink is silently skipped (caller logs `"mergeAndPush[...] returned null"` `:840`).
- Preconditions: every hydrant must have swapped and the sink must not be writable, else `ISE`
  (`:893-903`).
- **Push idempotency is a file check, not a metadata check** (`:906-926`): if
  `<persistDir>/descriptor.json` exists and `useUniquePath == false`, it deserializes and returns the
  previously-pushed `DataSegment` without touching deep storage (`:917-924`). Streaming ingestion
  always passes `useUniquePath = true` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:288`), so a **re-push writes a
  new S3 path** — this is deliberate (a replica or retried task must not overwrite bytes another
  writer may already be serving).
- `removeDirectory(mergedTarget)` (`:928`) then `indexMerger.mergeQueryableIndex(indexes, ...)`
  (`:948-959`) merges all persisted hydrants into one index using `indexSpec` and
  `maxColumnsToMerge`. Hydrant segments are acquired into a `Closer` (`:938-946`) and closed in
  `finally` (`:969`).
- `dataSegmentPusher.push(mergedFile, segmentToPush, useUniquePath)` (`:977`).
  Comment at `:976`: *"dataSegmentPusher retries internally when appropriate; no need for retries
  here."* — **there is no retry loop here**; S3 retry policy lives in `S3DataSegmentPusher`
  (`extensions-core/s3-extensions`).
- `objectMapper.writeValue(descriptorFile, segment)` (`:981`) — the local marker that push
  succeeded. Any exception: `metrics.incrementFailedHandoffs()` + warn + `RuntimeException`
  (`:1003-1007`), which fails the whole `push` future.

**A5. `BaseAppenderatorDriver.pushInBackground`** —
`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:493`.
Transforms the `push` future on `executor` = `Execs.singleThreaded("[id]-publish")` (`:280-281`).
Sanity check (`:509-532`): if the set of pushed segment ids ≠ the requested set, it
`dataSegmentKiller.killQuietly()` every pushed segment and throws `ISE` — deliberately deleting from
deep storage because nothing has been published yet.

**A6. `StreamAppenderatorDriver.publish`** —
`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:277`.
`Futures.transformAsync(pushInBackground(wrapCommitter(committer), theSegments, /*useUniquePath*/ true),
publishInBackground, directExecutor())` (`:285-297`), then on success removes the sequence names from
`segments` under `synchronized (segments)` (`:301-303`).

**A7. `BaseAppenderatorDriver.publishInBackground`** — `:584`, submitted to the single-threaded
`-publish` executor (`:620`).

- Empty-publish handling (`:592-614`): if nothing to publish and the publisher does not
  `supportsEmptyPublish()`, returns the input unchanged; if it does, it asserts
  `appenderator.getTotalRowCount() == 0` (`:607-612`).
- `RetryUtils.retry(..., predicate, /*maxTries*/ 1)` (`:623`, `:719-723`) — comment at `:722`:
  *"Do not retry here since the TransactionalSegmentPublisher itself performs required retries"*.
  The predicate only matches the "new start metadata is ahead of last committed end state" message.
- `publisher.publishSegments(null, ourSegments, identity, callerMetadata, schemaMapping)` (`:627`) —
  `callerMetadata` **is** the Kafka offsets. Impl is `ActionBasedSegmentPublisher`
  (`indexing-service`) → `SegmentTransactionalAppendAction`/`InsertAction` → Overlord →
  `IndexerSQLMetadataStorageCoordinator`.
- On `publishResult.isSuccess()` (`:634`): logs, computes `upgradedSegments` (segments the metadata
  store created on our behalf during a concurrent replace) and returns
  `segmentsAndCommitMetadata.withUpgradedSegments(...)` (`:651`).
- On non-success (`:652-702`) it must decide whether *someone else* published our ids:
  `publishedSegmentRetriever.findPublishedSegments(segmentIds)` (`:668`). If they match ours
  (`:669`), a replica or a lost-response retry won — and it kills our pushed files **only if the
  `loadSpec`s are physically disjoint** (`:682-689`), i.e. only when our unique path is definitely
  unreferenced. Otherwise it kills our segments and throws (`:697-701`).
- The outer `catch` (`:707-717`) is the important one: **"Must not remove segments here, we aren't
  sure if our transaction succeeded or not"** (`:708`) — on an ambiguous failure (timeout, connection
  loss) it deliberately leaks S3 objects rather than risk deleting live data.

**A8. The metadata transaction** —
`server/src/main/java/org/apache/druid/metadata/IndexerSQLMetadataStorageCoordinator.java`.

Non-concurrent-append streaming lands in `commitSegmentsAndMetadata` (`:428`):

```java
verifySegmentsToCommit(segments);                                      // :436
IndexerMetadataStorageCoordinator.validateDataSourceMetadata(...);     // :437
return inReadWriteDatasourceTransaction(dataSource, transaction -> {
  if (startMetadata != null) {
    final SegmentPublishResult result = updateDataSourceMetadataInTransaction(...); // :446
    if (!result.isSuccess()) { return result; }                        // :455-457  → rollback
  }
  return SegmentPublishResult.ok(Set.copyOf(insertSegments(transaction, segments, schema))); // :461
});
```

Concurrent-append mode goes to `commitAppendSegmentsAndMetadata` (`:549`) →
`commitAppendSegmentsAndMetadataInTransaction` (`:1175`), which additionally (a) reads pending
segments in a *separate read-only transaction first* (`:1188-1191`), (b) synthesizes upgraded
segment rows for versions created by a concurrent replace (`:1199-1213`), and then inside the write
transaction does metadata-first (`:1221-1233`) → `insertIntoUpgradeSegmentsTable` (`:1235`) →
`deletePendingSegments` in batches (`:1238`) → `insertSegments` (`:1246`).

**Optimistic concurrency: `updateDataSourceMetadataInTransaction`** (`:2234`).

1. `retrieveDataSourceMetadataAsBytes(transaction, supervisorId)` (`:2248`) reads the current
   `druid_dataSource` row payload.
2. `oldCommitMetadataSha1FromDb = base16(sha1(bytes))` (`:2256-2258`) — the fingerprint used for the
   CAS.
3. Two comparisons on the deserialized value (`:2262-2283`):
   - `startMetadataGreaterThanExisting` = `startMetadata.asStartMetadata().compareTo(old.asStartMetadata()) > 0`
     (`:2274`);
   - `startMetadataMatchesExisting` = `startMetadata.asStartMetadata().matches(old.asStartMetadata())`
     (`:2282`) — i.e. *the task's start offsets must equal the store's last committed end offsets*.
     With no row at all, it is `startMetadata.isValidStart()` (`:2266`).
4. Verdicts:
   - ahead **and** not matching → `SegmentPublishResult.retryableFailure("The new start metadata
     state[...] is ahead of the last committed end state[...]")` (`:2289-2293`) — a *previous task is
     still publishing*, so retrying later can succeed.
   - not matching (and not ahead) → `SegmentPublishResult.fail("Inconsistency between stored metadata
     state[...] and target state[...]")` (`:2298-2301`) — terminal; needs a supervisor reset.
5. New value = `old.plus(endMetadata)` (`:2305-2307`) — only *end* offsets are stored.
6. Write:
   - no existing row → plain `INSERT` (`:2316-2326`); `numRows == 1` else
     `retryableFailure("Insert failed")` (`:2328-2330`). Comment at `:2315` notes SELECT→INSERT races
     are expected and callers must retry.
   - existing row → **compare-and-swap `UPDATE`** (`:2333-2345`):
     `... WHERE dataSource = :dataSource AND commit_metadata_sha1 = :old_commit_metadata_sha1`;
     `numRows == 1` else `retryableFailure("Compare-and-swap update failed")` (`:2347-2349`).

**Why this is exactly-once.** The offset CAS and the segment `INSERT`s are two statements in **one
JDBI transaction at `READ_COMMITTED`** (`server/src/main/java/org/apache/druid/metadata/SQLMetadataConnector.java:181`). Returning a non-success
`SegmentPublishResult` from the callback returns *normally*, so the outer wrapper
`SqlSegmentMetadataTransactionFactory.executeWriteAndClose` (`:145`) does **not** call
`setRollbackOnly()` (that only happens on a thrown `Throwable`, `:153-155`) — but because the
metadata update is evaluated **first** and short-circuits before any segment insert (`:455-457`,
`:1230-1232`), the transaction commits with *no writes at all*. So: offsets advance ⟺ exactly those
segment rows became `used = true`. Duplicate work is absorbed twice over: `insertSegments`
(`:1988`) drops ids that already exist via `transaction.findExistingSegmentIds(segmentIds)`
(`:2004-2007`), and a replica whose CAS loses gets a failure that
`publishInBackground` resolves by *reading back* the published set (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:668`).

**What breaks it:**
- `startMetadata == null` → the `if` at `:445`/`:1220` is skipped entirely and segments are
  published with **no offset update**. Streaming passes non-null only when `ioConfig.isUseTransaction()`;
  `useTransaction: false` silently takes this branch (see `indexing-service/CLAUDE.md`).
- `resetDataSourceMetadata` (`:2382`) writes the payload with **no** sha1 predicate (`:2390-2393`) —
  a supervisor reset intentionally bypasses the CAS.
- Non-atomic side effect: the S3 object is written in A4, *before* the transaction. A failed
  transaction leaves an orphan object unless `publishInBackground` can prove it is unreferenced.

**Retries & backoff.** `SqlSegmentMetadataTransactionFactory.inReadWriteDatasourceTransaction`
(`server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentMetadataTransactionFactory.java:107`)
→ `connector.retryTransaction(callback, getQuietRetries(), getMaxRetries())` (`:112`, `:140-141`)
with `QUIET_RETRIES = 2`, `MAX_RETRIES = 3`
(`server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentMetadataReadOnlyTransactionFactory.java:38-39`).
`SQLMetadataConnector.retryTransaction` (`:177`) wraps
`getDBI().inTransaction(TransactionIsolationLevel.READ_COMMITTED, callback)` in
`RetryUtils.retry` (`:180-187`). Transient ⇔ `isTransientException` (`:195-205`):
`RetryTransactionException`, `SQLTransientException`, `SQLRecoverableException`,
`UnableToObtainConnectionException`, vendor-specific via `connectorIsTransientException` (`:210`),
or any of those as a nested cause of `SQLException`/`DBIException`/`UnableToExecuteStatementException`.
**Everything else — including a constraint violation or a `retryableFailure` result — is not retried
here**; `retryableFailure` is a *return value*, not an exception, so it propagates to the task.
Backoff: `RetryUtils.nextRetrySleepMillis`
(`processing/src/main/java/org/apache/druid/java/util/common/RetryUtils.java:226`) =
`min(60_000, 1000 * 2^(n-1)) * fuzz`, `fuzz ∈ [0,2]` Gaussian (`:229-233`).

**Segment-row batching.** `SqlSegmentMetadataTransaction.insertSegmentsInBatches`
(`server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentMetadataTransaction.java:499`):
`Lists.partition(segments, MAX_SEGMENTS_PER_BATCH)` with `MAX_SEGMENTS_PER_BATCH = 100` (`:64`), one
JDBI `PreparedBatch` (`:515`), `batch.execute()` per partition (`:545`), and any row whose
`affectedRows[i] != 1` raises `InternalServerError.exception("Failed to insert segments in metadata
store: ...")` (`:555-560`) — which *does* throw, so it rolls back.

**A9. `registerHandoff`** — `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:321`. Called from the task's
`publishFuture` success callback (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:1070`), so **offsets are
already durable at this point**.

Builds `waitingSegmentIdList` from pushed + upgraded segments (`:327-334`), returns an immediate
future if empty (`:337-346`), else a `SettableFuture` plus an `AtomicInteger
numRemainingHandoffSegments` (`:351-352`). For each segment it calls
`handoffNotifier.registerSegmentHandoffCallback(descriptor, Execs.directExecutor(), runnable)`
(`:355-361`) — note `directExecutor()`: the callback runs **on the notifier's poll thread**. The
runnable does `appenderator.drop(segmentIdentifier)` (`:366`) and on drop success decrements the
counter; at zero it reports `maxSegmentHandoffTime`, warns if over `HANDOFF_TIME_THRESHOLD` (`:378`)
and `resultFuture.set(...)` (`:382`). A drop failure sets the exception (`:398`).

---

### (B) Coordinator load loop → Historical load → announce → Broker view → handoff released

**B1. Duty scheduling** — `server/src/main/java/org/apache/druid/server/coordinator/DruidCoordinator.java`.

`start()` (`:366`) only registers a leader listener (`:375-390`); everything real happens in
`becomeLeader()` (`:433`), which builds one `DutiesRunnable` per group (`:455-489`). Each has its own
**single-thread** scheduled executor: `executorFactory.create(1, "Coordinator-Exec-" + alias + "-%d")`
(`:708`). Periods: historical management = `config.getCoordinatorPeriod()` (default **1 min**,
`server/src/main/java/org/apache/druid/server/coordinator/config/CoordinatorRunConfig.java:42`),
indexing = **30 min**, metadata-store management = **60 min**
(`server/src/main/java/org/apache/druid/server/coordinator/config/CoordinatorPeriodConfig.java:41-45`);
initial delay = `startDelay`, default **5 min** (`server/src/main/java/org/apache/druid/server/coordinator/config/CoordinatorRunConfig.java:41`).

Leadership is gated **three** times per tick: in the scheduling wrapper before running (`:505-508`)
and again before re-arming (`:511-516`); inside `DutiesRunnable.run()` under `lock` (`:715-720`); and
per-duty in `CoordinatorDutyGroup.run()` via `coordinator.isLeader()` (`:114`) which also compares
`startingLeaderCounter == coordLeaderSelector.localTerm()` (`:768-769`) so a stale term aborts
mid-pipeline.

**Overrun semantics.** `ScheduledExecutors.scheduleAtFixedRate`
(`processing/src/main/java/org/apache/druid/java/util/common/concurrent/ScheduledExecutors.java:132`)
schedules the *next* invocation **before** invoking the callable (`:148-150`) on the **same
single-thread** executor. So runs never overlap: an overrunning duty group makes the queued next run
start immediately on completion, and the effective period stretches to the run time. No pile-up, no
concurrency — just silent drift. `CoordinatorDutyGroup` tracks this as `gapTimes`/`runTimes`
(20-entry `EvictingQueue`,
`server/src/main/java/org/apache/druid/server/coordinator/duty/CoordinatorDutyGroup.java:56-57`),
exposed at `/druid/coordinator/v1/duties`.

`DutiesRunnable.run()` (`:712`) also refuses to run unless `metadataManager.isStarted() &&
serverInventoryView.isStarted()` (`:722`) — otherwise it logs and calls `stopBeingLeader()` (`:746`).
It snapshots `metadataManager.segments().getRecentDataSourcesSnapshot()` (`:734`) and the current
dynamic + compaction configs into an immutable `DruidCoordinatorRuntimeParams` (`:737-742`). A
duty returning `null` stops the rest of the group (`CoordinatorDutyGroup.java:120-122`);
`pauseCoordination` skips the whole group (`:106-110`).

**B2. Used-segment poll** —
`server/src/main/java/org/apache/druid/metadata/SqlSegmentsMetadataManager.java`.

`startPollingDatabasePeriodically()` (`:272`) uses `exec.scheduleWithFixedDelay(task, 0,
periodicPollDelay)` (`:291-296`) — **fixed *delay***, so poll N+1 starts `pollDuration` after poll N
*finishes*. Default `pollDuration` = **1 minute**
(`server/src/main/java/org/apache/druid/metadata/SegmentsMetadataManagerConfig.java:53`).

`poll()` (`:636`) is `synchronized (pollLock)` and dispatches to `pollSegments()` (`:648`) or
`pollSegmentAndSchema()` (`:690`). `pollSegments` is a **full poll, every time**: one read-only
transaction running `SELECT payload FROM <segments> WHERE used=true` (`:659`) with
`setFetchSize(connector.getStreamingFetchSize())` (`:660`) — streaming, but it materialises a
`List<DataSegment>` of *every used segment in the cluster* into heap. Two mitigations: corrupt rows
map to `null` and are filtered instead of aborting the poll (`:663-671`, `:808`), and
`replaceWithExistingSegmentIfPresent` (`:664`, body `:827`) interns each `DataSegment` against the
previous snapshot so most deserialized objects die in young-gen (rationale at `:818-826`).
`createDatasourcesSnapshot` (`:795`) then builds an immutable `DataSourcesSnapshot` and computes
overshadowed segments (`:807-815`) — the expensive part, which is why the snapshot is **never**
recomputed on individual mark-used/unused calls (comment `:798-806`).

`SqlSegmentsMetadataManagerV2`
(`server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentsMetadataManagerV2.java:59`) is the
front door: if `useIncrementalCache()` (`:90`) it serves snapshots from `HeapMemorySegmentMetadataCache`
(which syncs incrementally on its own `pollDuration`,
`server/src/main/java/org/apache/druid/metadata/segment/cache/HeapMemorySegmentMetadataCache.java:208`,
`:490`, `:513`), otherwise it delegates to the V1 full poll (`:145`).

**Staleness window for a freshly published segment:** up to one poll period before the Coordinator
even *sees* it, plus up to one coordinator period before `RunRules` acts.

**B3. `RunRules`** — `server/src/main/java/org/apache/druid/server/coordinator/duty/RunRules.java:71`.

Bails if `cluster.isEmpty()` (`:74`). Iterates `params.getUsedSegmentsNewestFirst()` (`:82`) —
newest-first specifically so round-robin spreads new segments and so throttling bites older ones
first (comment `:79-81`). Skips `overshadowed` (`:92`). For each segment, the **first** rule whose
`appliesTo(segment, now)` returns true wins and gets `rule.run(segment, segmentAssigner)`
(`:99-105`); no match increments a per-datasource counter that becomes an alert (`:107-109`, `:140-148`).
Then `processSegmentDeletes` (`:112`, body `:121`) calls back into
`DruidCoordinator.markSegmentsAsUnused` — an **Overlord HTTP call** (`DruidCoordinator.java:650-679`)
that tolerates a 404 from an older Overlord (`:667-673`).

`LoadRule.run` (`server/src/main/java/org/apache/druid/server/coordinator/rules/LoadRule.java:67`) is
one line: `handler.replicateSegment(segment, getTieredReplicants())`. `shouldMatchingSegmentBeLoaded()`
(`:79`) returns the ctor-computed `sum(tieredReplicants.values()) > 0` (`:51`).

**B4. `StrategicSegmentAssigner`** —
`server/src/main/java/org/apache/druid/server/coordinator/loading/StrategicSegmentAssigner.java`.

`replicateSegment(segment, tierToReplicaCount)` (`:202`): records required counts per tier in
`SegmentReplicaCountMap` (`:211-221`), flags tiers that don't exist in the cluster
(`datasourceToInvalidLoadTiers`, `:217-220`), tracks zero-replica segments (`:225-229`), computes a
cluster-wide `replicaSurplus` (`:231-232`), then calls `updateReplicasInTier` for **every** tier in
the cluster (`:236-243`) — not just the tiers named in the rule, so a tier shift drops correctly.

`updateReplicasInTier` (`:256`): `projectedReplicas = loadedNotDropping + loading -
max(0, moveCompletedPendingDrop)` (`:266-268`); early-returns when it already equals the requirement
(`:274-276`). On deficit it first *cancels queued drops* (`:290-291`) — cancelled drops count as
loaded replicas — and only then calls `loadReplicas` (`:297`).

`loadReplicas` (`:502`): if the segment is already on ≥1 server in the tier **and** the tier is
throttled, it returns 0 immediately (`:510-515`) — first copies are never throttled, replicas are.
Candidate servers come from `SegmentStatusInTier.getServersEligibleToLoad()` (`:517`;
`server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentStatusInTier.java:50`),
ordered either round-robin (`serverSelector`, `:525`) or by
`strategy.findServersToLoadSegment` (`:526`) depending on `useRoundRobinAssignment`. Then it queues
up to `numToLoad` via `loadSegment` (`:544`, first copy) or `replicateSegment` (`:556`, replicas —
re-checks the throttler `:559` and increments it on success `:568`).

`ReplicationThrottler`
(`server/src/main/java/org/apache/druid/server/coordinator/loading/ReplicationThrottler.java:36`):
built once per run (`createReplicationThrottler`, `:574-591`) with
`tierToMaxAssignments[tier] = max(0, replicationThrottleLimit - alreadyLoadingReplicas)` (`:64-67`),
so **in-flight loads from previous runs consume this run's budget**. `isReplicationThrottledForTier`
(`:72`) is a pure counter comparison.

`SegmentLoadingConfig.create`
(`server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentLoadingConfig.java:46`):
with `smartSegmentLoading` (default **true**, `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:465`) it *ignores* the
configured values and uses `maxSegmentsInLoadQueue = 0` (unlimited),
`replicationThrottleLimit = max(100, 5% of used segments)` (`:51`), `maxLifetimeInLoadQueue = 60`,
round-robin on (`:59-65`).

Per-server admission is `ServerHolder`
(`server/src/main/java/org/apache/druid/server/coordinator/ServerHolder.java`):
`maxAssignmentsInRun = maxSegmentsInLoadQueue - peon.getSegmentsToLoad().size()` (`:130-132`);
`canLoadSegment` (`:264`) requires not decommissioning, not already serving, no other queued action,
`totalAssignmentsInRun < maxAssignmentsInRun`, and `getAvailableSize() >= segment.getSize()` (`:270`).

`SegmentLoadQueueManager.loadSegment`
(`server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentLoadQueueManager.java:52`):
`server.startOperation(action, segment)` (`:55`, body `server/src/main/java/org/apache/druid/server/coordinator/ServerHolder.java:384`) reserves the slot;
`server.getPeon().loadSegment(segment, action, null)` (`:59`); any exception →
`server.cancelOperation(...)` + `log.error` + `return false` (`:62-67`) so the run continues.

**B5. `HttpLoadQueuePeon`** —
`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java`.

State: `segmentsToLoad`/`segmentsToDrop` `ConcurrentHashMap`s (`:94-95`), a priority-ordered
`TreeSet<SegmentHolder> queuedSegments` (`:104`), `activeRequestSegments` for requests that can no
longer be cancelled (`:110`), and a `mainLoopInProgress` `AtomicBoolean` (`:124`). All guarded by a
single `lock` (`:116`).

Ctor probes the Historical once: `fetchSegmentLoadingCapabilities()` (`:153`) does a blocking
`GET druid-internal/v1/segments/loadCapabilities` with a 10s `DEFAULT_TIMEOUT` (`:89`, `:166`);
a 404 falls back to `config.getBatchSize()` or 1 (`:169-177`); any other non-200 throws `RE` (`:180`).

`loadSegment` (`:474`) creates a `SegmentHolder(segment, action, config.getLoadTimeout(), callback)`
(`:496`) — `loadTimeout` is a hard-coded **15 minutes**
(`server/src/main/java/org/apache/druid/server/coordinator/config/HttpLoadQueuePeonConfig.java:32`,
`:80`) — inserts into both maps and *kicks*
`processingExecutor.execute(this::doSegmentManagement)` (`:499`).

`doSegmentManagement()` (`:193`) is the whole protocol:
- `mainLoopInProgress.compareAndSet(false, true)` guard (`:195`) — one in-flight batch per server.
- `batchSize = calculateBatchSize(loadingMode)` (`:201`, body `:371`) = the Historical's
  `numTurboLoadingThreads` in TURBO mode, else `config.getBatchSize()` or the Historical's
  `numLoadingThreads` (`:377`), floored at 1.
- Under `lock` (`:205-230`), drain `queuedSegments` up to `batchSize`. A holder whose
  `hasRequestTimedOut()` is true is failed with `SegmentChangeStatus.failed("timed out")` and evicted
  from every structure (`:211-219`); otherwise `markRequestSentToServer()` (`:222`) and add to
  `activeRequestSegments` (`:223`).
- `POST druid-internal/v1/segments/changeRequests?timeout=<hostTimeout>&loadingMode=<mode>`
  (`:248-255`) with an HTTP read timeout of **`hostTimeout + 5000`** (`:264`). `hostTimeout` default
  **5 min**, `repeatDelay` default **1 min** (`HttpLoadQueuePeonConfig.java:51-52`).
- Response (`:272-333`): `204 NO_CONTENT` = server had nothing to report (`:276`); `200` parses
  `List<DataSegmentChangeResponse>` and per entry `SUCCESS`/`FAILED` → `handleResponseStatus`
  (`:301`, body `:383`) which removes the holder from the maps + `queuedSegments` +
  `activeRequestSegments` (`:406-407`) and fires callbacks; **`PENDING` is left in the queue and
  merely trace-logged** (`:303-305`); an unknown state suppresses the immediate re-run (`:306-309`).
  Anything non-200 → `logRequestFailure` and no immediate re-run (`:321-323`).
- `finally` (`:326-332`): release `mainLoopInProgress`, and if the batch made progress,
  `processingExecutor.execute(this::doSegmentManagement)` again immediately (`:329-331`) — this is
  how the queue drains fast without waiting for `repeatDelay`.
- `onFailure` (`:336-344`) **only logs and releases the flag** — the holders stay in
  `queuedSegments`, so the next `repeatDelay` tick (or the next `loadSegment` kick) re-sends them.
  A load request is therefore retried indefinitely until its 15-minute `loadTimeout` expires, at
  which point the next `doSegmentManagement` marks it failed and drops it. The Coordinator will
  simply re-queue it next cycle because `RunRules` recomputes replica deficits from scratch.

`start()` (`:419`) arms `ScheduledExecutors.scheduleAtFixedRate(processingExecutor,
config.getRepeatDelay(), ...)` (`:426-440`). **`processingExecutor` is one shared single thread for
the entire cluster**: `services/src/main/java/org/apache/druid/cli/CliCoordinator.java:282` builds it
as `factory.create(1, "Master-PeonExec--%d")`, with a single `callBackExec`
(`"LoadQueuePeon-callbackexec--%d"`, `:278`), both handed to every `HttpLoadQueuePeon` via
`LoadQueueTaskMaster` (`:280-288`,
`server/src/main/java/org/apache/druid/server/coordinator/loading/LoadQueueTaskMaster.java:48-49`, `:84-85`).

**B6. Historical: receiving the request.**
`server/src/main/java/org/apache/druid/server/http/SegmentListerResource.java:234`
(`POST /changeRequests`) → `loadDropRequestHandler.processBatch(list, loadingMode)` (`:258`) and then
**async servlet** semantics: `req.startAsync()` (`:260`), `asyncContext.setTimeout(timeout)`, and an
`onTimeout` listener that cancels the future and returns **`204 NO_CONTENT`** (`:271-277`). So a
Historical that is slow to *finish* loading answers 204, the peon leaves the holders queued, and the
cycle repeats — no Jetty thread is held while loading.

`SegmentLoadDropHandler.processBatch`
(`server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:277`) calls
`processRequest` per request (`:287`) and resolves the returned future immediately if *any* request
already has a terminal status (`:296-297`), else parks it in `waitingFutures` (`:299-301`).

`processRequest` (`:312`) is the **idempotency gate**, under `requestStatusesLock`:
`requestStatuses` is a Guava LRU `Cache` keyed by the *request object* with max size
`config.getStatusQueueMaxSize()` (default 100,
`server/src/main/java/org/apache/druid/segment/loading/SegmentLoaderConfig.java:68`; built at
`:133`). New or previously-`FAILED` → mark `PENDING` (`:332`) and
`getLoadingExecutor(mode).submit(() -> addSegment(...))` (`:333-339`). Previously-`SUCCESS` →
`invalidate` the entry and return it (`:358-362`) so a reload can be requested later. Already
`PENDING` → return the same ref, no second submission.

Executors (`:97-110`): `normalLoadExec` = `ScheduledThreadPoolExecutor(config.getNumLoadingThreads())`
— default `max(1, availableProcessors / 6)` (`server/src/main/java/org/apache/druid/segment/loading/SegmentLoaderConfig.java:53`); `turboLoadExec` = fixed
pool of `numBootstrapThreads` with 60s keep-alive and `allowCoreThreadTimeOut(true)` (`:130`).
Selected by `getLoadingExecutor` (`:439-442`).

**B7. `SegmentLoadDropHandler.addSegment`** (`:146`), on a loading thread.

1. `segmentDropLatches.remove(segment)` then `currentDropLatch.cancelOrAwait()` (`:159-162`) —
   cancels a scheduled drop, or blocks until an already-started drop finishes, so a load can never
   race a delayed drop of the same segment.
2. `segmentManager.loadSegment(segment)` (`:165`); on any exception it calls
   `removeSegment(segment, NOOP, /*scheduleDrop*/ false)` to clean up **synchronously** and rethrows
   as `SegmentLoadingException` (`:167-170`).
3. `announcer.announceSegment(segment)` (`:173`) — comment `:172`: *"Announce segment even if the
   segment file already exists."*
4. Any `Throwable` → `log.makeAlert` + `SegmentChangeStatus.failed(rootCause)` (`:181-187`).
5. `finally` → `updateRequestStatus(new SegmentChangeRequestLoad(segment), result)` (`:189`) and the
   callback, which is `resolveWaitingFutures()` (`:336`, body `:381`) — that is what lets the
   Coordinator's outstanding POST return early with a status.

**B8. `SegmentManager.loadSegment`** —
`server/src/main/java/org/apache/druid/server/SegmentManager.java:199`.

`cacheManager.getSegment(dataSegment)` (`:203`); a `SegmentLoadingException` triggers
`cacheManager.cleanup(dataSegment)` before rethrowing (`:211-214`). Then `loadSegmentInternal`
(`:218`) does the timeline insert **inside `dataSources.compute(dataSource, ...)`** (`:227`) —
comment `:226`: *"compute() is used to ensure that the operation for a data source is executed
atomically"*. That `ConcurrentHashMap.compute` is the **per-datasource lock** for load *and* drop.
Inside: `loadedIntervals.findChunk(...)` duplicate check → warn + `false` (`:233-241`);
`IndexedTable` joinability consistency checks (`:244-253`); `loadedIntervals.add(interval, version,
shardSpec.createChunk(segment))` (`:254-258`); row-count bookkeeping (`:259-266`);
`pageCacheLoadFunction.accept(dataSegment)` (`:268`). Finally, outside the compute,
`cacheManager.storeInfoFile(dataSegment)` (`:277`) — the on-disk `info_dir` entry that makes the
segment reload on restart without the Coordinator.

**B9. Fetching bytes: `SegmentLocalCacheManager`** —
`server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java`.

`getSegment` (`:212`) = `getSegmentFiles` (`:214`) + `SegmentizerFactory.factorize` (`:217`) +
`ReferenceCountedSegmentProvider.wrapSegment` (`:218`).

`getSegmentFiles` (`:357`): takes a **per-segment** `ReferenceCountingLock` and
`synchronized (lock)` (`:359-360`), so concurrent loads of the same segment serialize while
different segments proceed in parallel. `findStoragePathIfCached` (`:362`, body `:305`) scans all
locations; if the dir exists it first checks `checkSegmentFilesIntact` (`:311`, body `:344`) which is
simply *"is `downloadStartMarker` still present?"* — if so the directory is a **partial download**
and is deleted (`cleanupCacheFiles` + `location.removeSegmentDir`, `:316-317`); otherwise
`location.maybeReserve(storageDir, segment)` re-accounts the space and the path is returned (`:320-322`).

`loadSegmentWithRetry` (`:385`): if a location already `isReserved(segmentDir)` only that one is
tried and a failure is fatal (`:391-402`); otherwise it walks `strategy.getLocations()` (`:405`),
`loc.reserve(segmentDir, segment)` (`:411`), and downloads (`:413`). All locations failing →
`SegmentLoadingException("Failed to load segment[%s] in all locations.")` (`:419`).
`loadInLocationWithStartMarkerQuietly` (`:431`) alerts, releases the reservation and deletes files on
failure, returning `false` so the next location is tried (`:437-452`).

`loadInLocationWithStartMarker` (`:455`): creates `downloadStartMarker` under
`directoryWriteRemoveLock` (`:460-471`), calls `loadInLocation` (`:472`), then deletes the marker
(`:474`) — failure to delete is itself an error. `loadInLocation` (`:479`) materialises the
`LoadSpec` from the segment's `loadSpec` map (`:483`) and calls `loadSpec.loadSegment(storageDir)`
(`:484`) — the S3 GET + unzip. **A size mismatch only warns** (`:485-492`).

`StorageLocation` (`server/src/main/java/org/apache/druid/segment/loading/StorageLocation.java`):
`reserve` (`:163`) refuses if already reserved (`:166-168`) and otherwise calls `canHandle` (`:194`),
which rejects when `availableSizeBytes() < segmentSize` (`:196-205`) or when
`freeSpaceToKeep + segmentSize > path.getFreeSpace()` (`:207-221`) — a real `statfs` call.
`maybeReserve` (`:137`) deliberately **skips** the space check (used for already-downloaded segments)
and only alerts if accounting goes negative (`:146-154`).

**B10. `BatchDataSegmentAnnouncer.announceSegment`** —
`server/src/main/java/org/apache/druid/server/coordination/BatchDataSegmentAnnouncer.java:146`.

Idempotent: double-checked `segmentLookup.containsKey` before and inside `synchronized (lock)`
(`:148-157`). Applies `segmentTransformer` (drops dimensions/metrics/loadSpec per config, `:105-114`).
**The line that matters for HTTP server views is `changes.addChangeRequest(new
SegmentChangeRequestLoad(toAnnounce))` (`:161`)** — that appends to the `ChangeRequestHistory`
(`:81`) that `GET /druid-internal/v1/segments` serves. If ZK announcement is skipped
(`isSkipSegmentAnnouncementOnZk`, `:116`) it returns right there (`:163-166`).
Otherwise: a single segment larger than `maxBytesPerNode` throws `ISE("byte size %,d exceeds %,d")`
(`:169-171`); else it packs into the first `availableZNode` whose current bytes + new bytes stays
under `maxBytesPerNode` (`:179-192`), evicting znodes that no longer fit (`:194-198`) and retiring a
znode once it holds `segmentsPerNode` segments (`:189-191`); if nothing fits it creates a new znode
(`:202-217`). Defaults: `segmentsPerNode = 50`, `maxBytesPerNode = 512 * 1024`
(`server/src/main/java/org/apache/druid/server/initialization/BatchDataSegmentAnnouncerConfig.java:33`,
`:38`). `announceSegments` (`:252`) does the same in batches (`:284-292`).

`ChangeRequestHistory`
(`server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHistory.java`):
`MAX_SIZE = 1000` (`:50`) entries in a `CircularBuffer` (`:54`); `addChangeRequests` is
`synchronized`, increments the counter per request (`:103`) and resolves waiting long-polls on a
single-threaded executor (`:106`). `getRequestsSinceWithoutWait` (`:166`) is the counter protocol:
same counter + matching hash → empty success (`:170-174`); mismatched hash or a client counter *ahead*
of the server → fail (`:176-187`); **`lastCounter - counter >= maxSize` → fail, i.e. request a full
resync** (`:188-197`) — a client that falls more than 1000 changes behind gets reset.

**B11. Broker (and Coordinator) view update.**
`ChangeRequestHttpSyncer`
(`server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHttpSyncer.java`) per
Historical, created by `HttpServerInventoryView.DruidServerHolder`
(`server/src/main/java/org/apache/druid/client/HttpServerInventoryView.java:529`, `:548-550`).

`sendSyncRequest` (`:231`) issues `GET <baseRequestPath>?counter=<c>&hash=<h>&timeout=<serverTimeout>`
(`getRequestString`, `:362-377`; first request uses `counter=-1`, `:374`) with HTTP timeout
`serverHttpTimeout = serverTimeout + HTTP_TIMEOUT_EXTRA_MS(5000)` (`:125`, `:63`). The Historical
side (`SegmentListerResource.getSegments`, `:121`) parks the request asynchronously (`:146`) and
returns `204` on `onTimeout` (`:157-163`) — **a long poll, not a fixed-interval poll**.

`onSuccess` (`:262`), under `startStopLock`: `204` → just restart `sinceLastSyncSuccess` and return
(`:272-275`); non-200 → `handleFailure` (`:276-279`); `changes.isResetCounter()` → set
`counter = null` and return, so the **next** request is a full sync (`:285-289`);
`counter == null ? listener.fullSync(...) : listener.deltaSync(...)` (`:291-295`); store the new
counter (`:297`); count down `initializationLatch` on first success (`:299-302`); reset
`consecutiveFailedAttemptCount` (`:304-308`). `finally` → `addNextSyncToWorkQueue()` (`:316`).

`addNextSyncToWorkQueue` (`:379`): healthy → `executor.execute(this::sendSyncRequest)` **immediately**
(`:396`); after a failure → `executor.schedule(..., min(MAX_RETRY_BACKOFF /*2 min*/,
RetryUtils.nextRetrySleepMillis(consecutiveFailedAttemptCount)))` (`:388-394`).
`markServerUnstableAndAlert` (`:421`) starts `sinceUnstable` on the first failure and emits an alert
only once `maxUnstableDuration` (= `serverUnstabilityTimeout`, default **1 min**) has elapsed
(`:434-439`).

Self-healing: `HttpServerInventoryView.checkAndResetUnhealthyServers` (`:466`) runs on a dedicated
1-thread `monitoringExecutor` (`:162`) every **5 min after a 60s delay** (`:226-231`); for any syncer
where `needsReset()` (`:207` — no request issued for `maxDelayBetweenSyncRequests = 3 *
serverHttpTimeout + 2 min`, `:130`) it does `serverRemoved(...)` then
`serverAdded(server.copyWithoutSegments())` (`:481-482`), i.e. **drops and rebuilds that server's
entire segment set**. Metrics emit every minute (`:232-237`).

`fullSync` (`:585`) diffs: build `toRemove` from the current known segments, `addSegment` each
reported segment and un-mark it, then `removeSegment` whatever is left (`:587-608`). `deltaSync`
(`:612`) simply applies loads/drops (`:614-628`). `addSegment` (`:633`) applies the watch predicate,
interns via `DataSegmentInterner` (`:637`), mutates `druidServer` and fires `segmentAdded` callbacks
(`:638-648`); duplicates are warned only on delta syncs (`:649-656`). Config defaults:
`serverTimeout` **4 min**, `serverUnstabilityTimeout` **1 min**, `numThreads` **5**
(`server/src/main/java/org/apache/druid/client/HttpServerInventoryViewConfig.java:55`, `:59`, `:61`).

`BrokerServerView` (`server/src/main/java/org/apache/druid/client/BrokerServerView.java`) registers
its callbacks on a **single-threaded** `Execs.singleThreaded("BrokerServerView-%s")` (`:124-125`), so
all timeline mutation is serialized on one thread *and* additionally guarded by
`synchronized (lock)`. `serverAddedSegment` (`:262`): skips brokers (`:269`); on first sight of a
segment id creates a `ServerSelector` (`:273`), lazily creates the datasource
`VersionedIntervalTimeline` with tombstone-skipping enabled (`:278`), `timeline.add(...)` (`:282`)
and records the selector (`:283`); then `selector.addServerAndUpdateSegment(queryableDruidServer,
segment)` (`:300`). If the announcing server is not in the inventory it **skips the segment entirely**
(`:289-296`). `serverRemovedSegment` (`:307`): `selector.removeServer(...)` (`:335`) and the
timeline entry is removed **only when `selector.isEmpty()`** (`:345-351`) — so losing one replica
never makes a segment unqueryable. `getTimeline` (`:367`) reads under the same lock.

**B12. Handoff released.** `CoordinatorBasedSegmentHandoffNotifier`
(`server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifier.java`).

`start()` (`:74`): `Execs.scheduledSingleThreaded("coordinator_handoff_scheduled_%d")` (`:76`) and
`scheduleAtFixedRate(this::checkForSegmentHandoffs, 0L, pollDuration)` (`:77-82`).
`handOffCallbacks` is a `ConcurrentHashMap<SegmentDescriptor, Pair<Executor, Runnable>>` (`:42`)
populated by `registerSegmentHandoffCallback` via `putIfAbsent` (`:66`) — duplicate registrations
return `false` and are ignored.

`checkForSegmentHandoffs()` (`:85`): iterates the map (`:88-90`) and per descriptor does a
**blocking** `FutureUtils.getUnchecked(coordinatorClient.isHandoffComplete(dataSource, descriptor),
true)` (`:95`). On `TRUE` it runs the callback via the registered executor and `itr.remove()`s the
entry (`:96-100`). A **per-descriptor** `catch (Exception)` logs and moves on (`:102-111`), and an
outer `catch (Throwable)` protects the scheduled task itself (`:117-125`) — so a Coordinator outage
never kills the poller; it just keeps retrying every `pollDuration`. Because the calls are serial and
blocking on one thread, N waiting segments cost N sequential RPCs per tick.

`CoordinatorClientImpl.isHandoffComplete`
(`server/src/main/java/org/apache/druid/client/coordinator/CoordinatorClientImpl.java:72`) builds
`GET /druid/coordinator/v1/datasources/{ds}/handoffComplete?interval=&partitionNumber=&version=`
(`:74-80`) over `ServiceClient` (leader-aware, retry policy injectable at `:194`).

`DataSourcesResource.isHandOffComplete`
(`server/src/main/java/org/apache/druid/server/http/DataSourcesResource.java:909`):
1. `metadataRuleManager.getRulesWithDefault(dataSourceName)` (`:917`).
2. First rule matching the interval decides `eligibleForLoad = rule instanceof LoadRule &&
   ((LoadRule) rule).shouldMatchingSegmentBeLoaded()` (`:924-929`). **If not eligible → `200 true`
   immediately** (`:930-932`) — the silent short-circuit.
3. `serverInventoryView.getTimeline(new TableDataSource(ds))` — the **Coordinator's own**
   `CoordinatorServerView`, not an RPC to Historicals; `null` → `false` (`:934-940`).
4. If the newest chunk in the timeline has a *higher* version than ours → `200 true` (`:946-948`),
   because a concurrent replace means our version will never be handed off.
5. `prepareServedSegmentsInInterval` (`:951`, body `:883`) uses
   `lookupWithIncompletePartitions` (`:889`), then `isSegmentLoaded` (`:964`) returns true if some
   served segment contains our interval, matches our partition number, has version `>=` ours, **and**
   any holding server satisfies `DruidServerMetadata::isSegmentReplicationTarget` (`:967-972`).
6. Any exception → `500` with an error body (`:958-961`) — which the notifier treats as "not yet".

**Where `handoffConditionTimeout` is enforced: the task, not here.**
`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:880-899`
— `0` means `Futures.allAsList(handOffWaitList).get()` with **no timeout** (`:881`); otherwise
`get(handoffConditionTimeout, MILLISECONDS)` (`:885-886`) and a `TimeoutException` is **swallowed**
after an alert (`:888-895`) with the comment *"Handoff timeout is not an indexing failure, but
coordination failure"* — the task still reports success. `handoffWaitMs` is recorded in `finally`
(`:897`).

---

### (C) Query: Broker → Historicals → merged response

**C1. Timeout budget** is fixed once, at the Broker's `QueryLifecycle`, by
`SetAndVerifyContextQueryRunner.withTimeoutAndMaxScatterGatherBytes`
(`server/src/main/java/org/apache/druid/server/SetAndVerifyContextQueryRunner.java:58`):
default timeout = `min(defaultQueryTimeout, maxQueryTimeout)` (`:64`), `maxScatterGatherBytes`
applied (`:66`), `verifyMaxQueryTimeout` (`:68`), and then an **absolute deadline** is written into
the context: `QUERY_FAIL_TIME = startTimeMillis + timeout` (`:73-82`). Every downstream component
reads that absolute value, so the budget is not re-extended per hop.

**C2. `CachingClusteredClient.run`** —
`server/src/main/java/org/apache/druid/client/CachingClusteredClient.java:330` (per-query state in
`SpecificQueryRunnable`, `:255`; entry points `getQueryRunnerForIntervals` `:181` /
`getQueryRunnerForSegments` `:220`). Runs on the **Jetty request thread**.

1. `serverView.getTimeline(ev.getBaseTableDataSource())` (`:336`); absent → `Sequences.empty()`
   (`:339-341`). The `timelineConverter` is applied at `:343`.
2. `computeUncoveredIntervals(timeline)` (`:345`, body `:492`) only if `uncoveredIntervalsLimit > 0`.
3. `computeSegmentsToQuery(timeline, specificSegments)` (`:348`, body `:434`): `lookup` vs
   `lookupWithIncompletePartitions` depending on `specificSegments` (`:439-440`),
   `toolChest.filterSegments` (`:444`), then optional **secondary partition pruning** —
   `DimFilterUtils.filterShards` with a memoised `dimensionRangeCache` (`:456-478`) — producing a
   `LinkedHashSet<SegmentServerSelector>` (order preserved so shard merge order is stable).
4. **Result-level etag short-circuit** (`:354-363`): if the request carried `If-None-Match` and the
   recomputed etag matches, return an empty sequence.
5. **Per-segment cache** (`:366`, body `:532`): `computePerSegmentCacheKeys` (`:565`) builds
   `CacheUtil.computeSegmentCacheKey(segmentId, descriptor, queryCacheKey)` per segment (`:573-577`),
   `computeCachedValues` (`:583`) does **one** `cache.getBulk(...)` limited to
   `cacheConfig.getCacheBulkMergeLimit()` (`:586`). Hits are **removed from the query set** and their
   bytes kept (`:552-555`) — this is what makes partial cache hits work; misses are registered for
   population when `populateCache` (`:556-560`).
6. `scheduler.prioritizeAndLaneQuery(queryPlus, segmentServers)` (`:368`) rewrites priority/lane;
   segment count is reported to the response context and metrics (`:371-372`).
7. `groupSegmentsByServer(segmentServers, cloneQueryMode)` (`:374`, body `:607`): per segment,
   `ServerSelector.pick(query, cloneQueryMode)` applies the tier-selector strategy; a `null` pick
   raises an alert (`:617-620`). Result is a `TreeMap<DruidServer, List<SegmentDescriptor>>` — one
   subquery per server.
8. `LazySequence` (`:379-384`): **nothing is sent yet.** Only when the sequence is consumed does it
   `addSequencesFromCache` (`:381`, body `:631` — deserialize cached bytes through
   `strategy.pullFromSegmentLevelCache()`), then `addSequencesFromServer` (`:382`, body `:679`), then
   `merge` (`:383`, body `:389` — `ParallelMergeCombiningSequence` on the shared `pool` when enabled
   and a merge fn exists, `:393-400`).
9. Return `scheduler.run(query, mergedResultSequence)` (`:386`) — see C4.

`addSequencesFromServer` (`:679`): `serverView.getQueryRunner(server)` (`:685`) — a
`DirectDruidClient`; `maxQueuedBytesPerServer = maxQueuedBytes / numServers` (`:693-694`) so
backpressure is divided fairly; then one of three variants (`:697-703`) — `bySegment` passthrough,
plain results, or cache-populating results (only for `isSegmentReplicationTarget()` servers, i.e.
never cache from a realtime task). Note `makeDownstreamQueryContext` (`:300`) forces
`bySegment = true` and `populateCache = false` downstream when the Broker is populating (`:312-316`).

**C3. `DirectDruidClient.run`** —
`server/src/main/java/org/apache/druid/client/DirectDruidClient.java:155`.

Reads the absolute deadline `timeoutAt = context.getLong(QUERY_FAIL_TIME)` (`:172`) and
`maxScatterGatherBytes`/`maxQueuedBytes` (`:173-176`). The response handler (`:178`) is fully
streaming: chunks go into a `LinkedBlockingQueue<InputStreamHolder>` (`:183`); `enqueue` returns
`false` once `queuedByteCount >= maxQueuedBytes`, which suspends the Netty channel (`:203-213`);
`dequeue` (`:215`) uses `queue.poll(checkQueryTimeout(), MILLISECONDS)` and throws
`QueryTimeoutException` on `null` (`:217-220`), resuming the channel via `TrafficCop.resume` when the
queue drains (`:222-227`). `checkQueryTimeout` (`:451`) recomputes `timeoutAt - now` on every
interaction and, when exhausted, calls `setupResponseReadFailure` and throws
`QueryTimeoutException` (`:454-457`). `checkTotalBytesLimit` (`:463`) throws
`ResourceLimitExceededException` when the shared `totalBytesGathered` crosses
`maxScatterGatherBytes` (`:465-472`).

Before sending: a final deadline check (`:477-481`); `openConnections.getAndIncrement()` (`:485`);
`httpClient.go(POST <scheme>://<host>/druid/v2/ with Queries.withTimeout(query, timeLeft), handler,
Duration.millis(timeLeft))` (`:487-498`) — the remaining budget is both the body's timeout and the
HTTP read timeout. `queryWatcher.registerQueryFuture` (`:505`) makes it cancellable, and a callback
decrements `openConnections` and, if the future was cancelled, POSTs a cancel to
`/druid/v2/{queryId}` (`:511-523`). The returned `Sequence` wraps a `JsonParserIterator` (`:539`)
which also honours `QUERY_FAIL_TIME`
(`server/src/main/java/org/apache/druid/client/JsonParserIterator.java:77`) — **results are parsed
lazily as bytes arrive**, never fully buffered.

**C4. `QueryScheduler` admission** —
`server/src/main/java/org/apache/druid/server/QueryScheduler.java`.
`totalCapacity` = `druid.query.scheduler.numThreads` only if it is `> 0`, `< serverConfig.numThreads`
and request queuing is off; otherwise `serverConfig.getNumThreads()` and **no total limiter is
installed** (`:111-120`). Lanes are resilience4j `Bulkhead`s from `getLaneConfigs` (`:330`) built with
`maxWaitDuration(Duration.ZERO)` (`:336`) — **rejection is immediate, never queued**.
`run` (`:191`) wraps the sequence so `acquireLanes` happens in `before()` (`:199`) and `finishLanes`
in `after()` (`:206`) — i.e. permits are held for the whole streaming lifetime of the response, not
just the planning phase. `acquireLanes` (`:274`) takes the named lane first (`:282-288`), then
`TOTAL` (`:294-300`), throwing `QueryCapacityExceededException`, and releases anything acquired on
failure (`:303-306`).

**C5. Historical side: `ServerManager`** —
`server/src/main/java/org/apache/druid/server/coordination/ServerManager.java`.
`getQueryRunnerForIntervals` (`:129`) resolves intervals against the local timeline and returns
`NoopQueryRunner` if the datasource is unknown (`:141`) — deliberately *not* an error, since
intervals are a filter. `getQueryRunnerForSegments` (`:170`) returns
`ReportTimelineMissingSegmentQueryRunner` when there is no timeline (`:176`), which is what makes the
Broker retry/report missing segments. `acquireAllSegments` (`:194`) materialises the list eagerly to
**take all references up front** (comment `:201`): per descriptor, `timeline.findChunk(...)`
(`:207-211`); a missing chunk yields `SegmentReference(descriptor, Optional.empty())` (`:213-215`)
which becomes a "missing segment" report; otherwise `segmentMapFn.apply(referenceCounter)`
(`:218`) → `ReferenceCountedSegmentProvider.acquireReference()`
(`processing/src/main/java/org/apache/druid/segment/ReferenceCountedSegmentProvider.java:168`) and the
resulting `Segment` is registered with the query's `Closer`.

**C6. Drop vs in-flight query.** `SegmentLoadDropHandler.removeSegment` (`:202`) **unannounces
first** (`:210`) so no new query is routed there, then registers a `SegmentDropLatch` (`:213`;
duplicate → warn and return, `:215-218`) and schedules the actual drop `dropSegmentDelayMillis`
later (default **30s**, `server/src/main/java/org/apache/druid/segment/loading/SegmentLoaderConfig.java:47`; scheduled at `:242-246`).
`SegmentManager.dropSegment` (`:281`) removes the chunk from the timeline inside the same
`dataSources.compute` (`:286-303`) and registers the `ReferenceCountedSegmentProvider` with a
`Closer` (`:317`) — closing only *decrements*; the underlying files/mmaps survive until the last
in-flight query releases its reference.

---

## Nuances

- **Every hop is poll-driven.** End-to-end "publish → queryable" latency ≈ metadata poll
  (≤1 min) + coordinator period (≤1 min) + load queue drain (`repeatDelay` 1 min, or immediate on
  progress) + actual S3 download + inventory long-poll round trip + handoff poll (`PT1S`).
  Nothing in this chain is event-driven except the load-queue kick
  (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:499`) and the inventory long poll.
- **`ScheduledExecutors` never overlaps runs but silently stretches periods** — the next run is
  queued before the current one starts, on a 1-thread executor
  (`ScheduledExecutors.java:148-150`). The observable symptom is `gapTimes` growing in
  `/druid/coordinator/v1/duties`.
- **The metadata poll is full, not incremental**, in the V1 path
  (`server/src/main/java/org/apache/druid/metadata/SqlSegmentsMetadataManager.java:648`) — cost is O(all used segments) heap per poll,
  mitigated only by interning (`:827`) and streaming fetch size. The V2 incremental cache
  (`server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentsMetadataManagerV2.java:90`) avoids this when enabled.
- **Idempotency**:
  - *push*: `descriptor.json` short-circuit (`StreamAppenderator.java:906-926`), but **bypassed**
    when `useUniquePath = true`, which streaming always sets.
  - *segment insert*: `findExistingSegmentIds` dedupe (`server/src/main/java/org/apache/druid/metadata/IndexerSQLMetadataStorageCoordinator.java:2004`).
  - *offset update*: sha1 CAS (`:2337`) — replay is a no-op-then-failure, never a double advance.
  - *load/drop request*: `requestStatuses` LRU keyed by request (`server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:318`);
    `SUCCESS` is invalidated on read so a reload can be requested (`:358-362`).
  - *timeline insert*: duplicate `findChunk` check warns and no-ops (`SegmentManager.java:239-241`).
  - *announce*: `segmentLookup.containsKey` double-check (`BatchDataSegmentAnnouncer.java:148-157`).
- **Survives process restart**: the metadata store (segments + offsets); S3 objects; the Historical's
  `info_dir` files (`server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:194`) plus the on-disk segment cache; the
  peon's `commit.json` + per-hydrant persist dirs (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:715`, `:1642`).
  **Does not survive**: the Coordinator's `DataSourcesSnapshot`, `DruidCluster` and
  `ReplicationThrottler` (all per-run), every `HttpLoadQueuePeon` queue (`stop()` cancels everything,
  `HttpLoadQueuePeon.java:453-469`), the Broker/Coordinator server views (rebuilt by full sync), and
  `SegmentLoadDropHandler.requestStatuses`.
- **Cached vs recomputed**: recomputed **every** coordinator run — `SegmentLoadingConfig`,
  `DruidCluster`, `ServerHolder.maxAssignmentsInRun`, the throttler budget, and every replica count.
  Cached — the metadata snapshot (1 poll), the segment-level query cache, the Broker's timeline
  (incrementally synced), `DataSegmentInterner`ed segments, and `HttpLoadQueuePeon`'s
  `serverCapabilities` (fetched **once in the constructor**, `:150` — a Historical that changes
  `numLoadingThreads` needs its peon recreated).
- **Silent short-circuits** worth knowing:
  1. `isHandOffComplete` returns `true` when no `LoadRule` matches or replicas total zero
     (`DataSourcesResource.java:930-932`) — the task believes handoff completed though nothing was
     ever loaded.
  2. `isHandOffComplete` returns `true` when a newer version exists in the timeline (`:946-948`).
  3. `handoffConditionTimeout` expiry is swallowed and the task still succeeds
     (`SeekableStreamIndexTaskRunner.java:888-895`).
  4. `mergeAndPush` returns `null` for a stale sink and `push` just logs (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:882`, `:840`).
  5. `getOrCreateSink` alerts but does not throw on announcement failure (`:525-529`).
  6. Segment size mismatch after download only warns (`SegmentLocalCacheManager.java:485-492`).
  7. `SegmentLoadQueueManager.loadSegment` swallows exceptions and returns `false` (`:62-67`).
  8. `HttpLoadQueuePeon` `PENDING` responses are trace-logged only (`:303-305`).
- **Units**: `maxRowsInMemory` = rows (realtime default **150 000**,
  `server/src/main/java/org/apache/druid/segment/indexing/TuningConfig.java:50`);
  `maxBytesInMemory` = bytes, `0` ⇒ appendable-index default, `< 0` ⇒ unbounded
  (`TuningConfig.java:73-83`); `intermediatePersistPeriod` = ISO period;
  `maxBytesPerNode` = bytes (512 KiB); `hostTimeout`/`repeatDelay`/`serverTimeout` = ISO
  durations; `handoffConditionTimeout` = **milliseconds**; `dropSegmentDelayMillis` = ms;
  `replicationThrottleLimit`/`maxSegmentsInNodeLoadingQueue` = counts **per tier** / **per server per
  run**; `maxQueuedBytes`/`maxScatterGatherBytes` = bytes.

---

## Failure modes

| Failure | Where it surfaces (`path:LINE`) | Detection | Blast radius | Recovery / knob |
|---|---|---|---|---|
| Metadata txn transient error (deadlock, conn loss) | `server/src/main/java/org/apache/druid/metadata/SQLMetadataConnector.java:180` | `isTransientException` (`:195`) | one publish | 3 tries, exp. backoff; then task fails and retries |
| Offset CAS lost (concurrent/duplicate task) | `server/src/main/java/org/apache/druid/metadata/IndexerSQLMetadataStorageCoordinator.java:2347` | `numRows != 1` | one publish | `retryableFailure`; task-level retry |
| Start offsets ahead of committed end | `server/src/main/java/org/apache/druid/metadata/IndexerSQLMetadataStorageCoordinator.java:2289` | `compareTo > 0 && !matches` | task group stalls | wait for predecessor, or supervisor reset |
| Start offsets inconsistent | `server/src/main/java/org/apache/druid/metadata/IndexerSQLMetadataStorageCoordinator.java:2298` | `!matches` | task group blocked | terminal — `resetDataSourceMetadata` (`:2382`) |
| Segment row insert affected ≠ 1 | `server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentMetadataTransaction.java:555` | `affectedRows[i] != 1` | whole txn rolls back | throws → rollback → retry |
| Push to S3 fails | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1003` | exception from `dataSegmentPusher.push` (`:977`) | one segment, then whole task | `S3DataSegmentPusher` internal retries; task retry |
| Push produced wrong segment set | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:515` | set inequality | segments killed in deep storage | task retry |
| Ambiguous publish outcome | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:707` | exception, unknown commit state | orphan S3 objects | intentional leak; `killUnusedSegments` duty later |
| Background persist failure | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:442` → `:294` | `persistError` rethrown by next `add()` | task fails | task retry |
| Heap overhead exceeded | `StreamAppenderator.java:399-427` | post-persist byte projection | task fails with alert | `maxBytesInMemory`, `skipBytesInMemoryOverheadCheck` |
| Coordinator not leader mid-run | `server/src/main/java/org/apache/druid/server/coordinator/DruidCoordinator.java:715`, `server/src/main/java/org/apache/druid/server/coordinator/duty/CoordinatorDutyGroup.java:114` | `isLeader() && term ==` | run aborts | new leader re-runs from scratch |
| Coordinator unreachable during handoff poll | `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifier.java:102` | exception per descriptor | task waits | retried every `pollDuration`; bounded by `handoffConditionTimeout` |
| `handoffComplete` throws | `server/src/main/java/org/apache/druid/server/http/DataSourcesResource.java:958` | `500` | treated as "not yet" | fix rules / inventory |
| Load request HTTP failure | `server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:336` | future `onFailure` | that server's batch | holders stay queued; resent on next tick |
| Load request server timeout | `server/src/main/java/org/apache/druid/server/http/SegmentListerResource.java:271` | async `onTimeout` → `204` | none | poll again |
| Load holder exceeded `loadTimeout` (15 min) | `server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:211` | `hasRequestTimedOut()` | one segment dropped from queue | re-queued next `RunRules` |
| Historical disk full / too small | `server/src/main/java/org/apache/druid/segment/loading/StorageLocation.java:196`, `:209` | `canHandle` false | all locations exhausted → load fails | `maxSize`, `freeSpacePercent`, add locations |
| Corrupt / partial download | `server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:344`, `:311-318` | leftover `downloadStartMarker` | that segment | dir deleted, re-downloaded |
| All storage locations fail | `server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:419` | `SegmentLoadingException` | one segment | `addSegment` cleans up (`server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:167`) |
| Announcement byte overflow | `BatchDataSegmentAnnouncer.java:169-171`, `:280-282` | `newBytesLen > maxBytesPerNode` | segment never announced ⇒ handoff never completes | `maxBytesPerNode`, `skipDimensionsAndMetrics`, `skipLoadSpec` |
| Broker view desync (counter too far behind) | `server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHistory.java:188` | `lastCounter - counter >= 1000` | that server's view | `resetCounter` → full sync (`server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHttpSyncer.java:285`) |
| Syncer stuck / unhealthy | `HttpServerInventoryView.java:472-483` | `needsReset()` (`server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHttpSyncer.java:207`) | one server's segments stale up to ~15 min | remove+re-add server; `serverTimeout` |
| Historical dies mid-query | `server/src/main/java/org/apache/druid/client/DirectDruidClient.java:415` (`exceptionCaught`), `:517` | HTTP failure / cancel | that subquery ⇒ query | retry at Broker; `ServerSelector` picks a live replica next time |
| Query exceeds deadline | `server/src/main/java/org/apache/druid/client/DirectDruidClient.java:451`, `:217` | `timeoutAt - now <= 0` | one query | `timeout`, `maxQueryTimeout` |
| Scatter-gather bytes exceeded | `server/src/main/java/org/apache/druid/client/DirectDruidClient.java:463` | `totalBytesGathered >` limit | one query | `maxScatterGatherBytes` |
| Lane / total capacity exceeded | `server/src/main/java/org/apache/druid/server/QueryScheduler.java:285`, `:297` | `tryAcquirePermission()` false | one query rejected (429) | `druid.query.scheduler.*` |
| Cost-balancer computation timeout | `server/src/main/java/org/apache/druid/server/coordinator/balancer/CostBalancerStrategy.java:350`, `:377` | `TimeoutException` after 1 min | that segment's move skipped | `balancerComputeThreads` |

**The eight that matter most.**

1. **Metadata transaction conflict.** Two writers for the same supervisor race on one row. The
   loser's `UPDATE ... WHERE commit_metadata_sha1 = :old` affects 0 rows
   (`IndexerSQLMetadataStorageCoordinator.java:2340-2349`) and the callback returns
   `retryableFailure` — a *value*, so `SQLMetadataConnector.retryTransaction` does **not** retry it
   (only `isTransientException` throwables are retried, `:195-205`). It bubbles to
   `publishInBackground`, which reads back the published set (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:668`) to
   decide between "someone else already did it" and "real failure". True DB deadlocks *are*
   retryable (`SQLTransientException`/vendor-specific via `connectorIsTransientException:210`) with
   3 attempts and 1s→2s→4s (fuzzed) backoff. Because the metadata update is evaluated first and
   short-circuits before any insert, a failed conflict commits **nothing**.

2. **Push to S3 fails.** `dataSegmentPusher.push` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:977`) has **no local
   retry** by design (`:976`); the catch bumps `failedHandoffs` and wraps in `RuntimeException`
   (`:1003-1007`), failing `push` → `pushInBackground` → the task. Nothing was published, so nothing
   is inconsistent; the retried task re-merges from the persisted hydrants (still on disk) and pushes
   to a **new unique path** (`useUniquePath = true`), so partially-written objects are never reused.

3. **Coordinator not leader / unreachable during handoff poll.** Leadership loss aborts the current
   duty pipeline at the next `isLeader()` check (`server/src/main/java/org/apache/druid/server/coordinator/duty/CoordinatorDutyGroup.java:114`) — mid-flight load
   POSTs already sent still complete on the Historical, and the new leader rebuilds `DruidCluster`
   and re-derives the deficits, so nothing is lost, only re-planned. On the task side an unreachable
   Coordinator makes `FutureUtils.getUnchecked` throw inside the **per-descriptor** try
   (`CoordinatorBasedSegmentHandoffNotifier.java:93-111`); the entry stays in `handOffCallbacks` and
   is retried every `pollDuration` forever. The only bound is task-side
   `handoffConditionTimeout`, and hitting it is *not* a failure — the task alerts and succeeds
   anyway (`SeekableStreamIndexTaskRunner.java:888-895`). Offsets were committed long before, so no
   data is reread; the segment just isn't queryable yet.

4. **Load request timeout.** Two distinct timeouts. (a) The Historical simply hasn't finished:
   the async servlet fires `onTimeout` and returns `204` (`SegmentListerResource.java:271-277`); the
   peon's `onSuccess` sees 204 and does nothing (`HttpLoadQueuePeon.java:276-277`), leaving the
   holders queued for the next `repeatDelay` tick. (b) The holder has been queued longer than
   `loadTimeout` (15 min, hard-coded at `server/src/main/java/org/apache/druid/server/coordinator/config/HttpLoadQueuePeonConfig.java:32`): the next
   `doSegmentManagement` marks it `failed("timed out")` and evicts it from `queuedSegments`,
   `segmentsToLoad` and `activeRequestSegments` (`HttpLoadQueuePeon.java:211-219`). Either way,
   because `RunRules` recomputes replica deficits from the *current* cluster state each cycle
   (`StrategicSegmentAssigner.java:266-297`), the segment is simply re-queued next coordinator
   period — there is no persistent retry counter and no give-up.

5. **Disk full on a Historical.** `StorageLocation.canHandle` (`:194`) rejects on either accounted
   space (`availableSizeBytes() < segmentSize`, `:196`) or real free space
   (`freeSpaceToKeep + segmentSize > path.getFreeSpace()`, `:209`), so `reserve` returns `null`
   (`:174`) and `loadSegmentWithRetry` moves to the next location; all failing →
   `SegmentLoadingException("Failed to load segment[%s] in all locations.")`
   (`server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:419`). `SegmentLoadDropHandler.addSegment` then calls
   `removeSegment(..., scheduleDrop=false)` to unwind and reports `FAILED` (`:167-170`, `:186`). The
   Coordinator sees `FAILED`, drops the holder, and re-assigns next run — potentially to the same
   full server, since `ServerHolder.canLoadSegment` uses the *announced* `getAvailableSize()`
   (`server/src/main/java/org/apache/druid/server/coordinator/ServerHolder.java:270`) which can disagree with the disk. Note `maybeReserve` (`:137`)
   deliberately allows overcommit for already-downloaded segments and only alerts (`:147-153`).

6. **Corrupt downloaded segment.** Integrity is a marker file, not a checksum:
   `loadInLocationWithStartMarker` creates `downloadStartMarker` before downloading and deletes it
   after (`SegmentLocalCacheManager.java:459-476`). On the next load attempt
   `checkSegmentFilesIntactWithStartMarker` (`:344`) sees the leftover marker and
   `findStoragePathIfCached` deletes the directory and un-reserves the space (`:311-318`), forcing a
   fresh download. **Content corruption that still produces a complete download is not detected** —
   the size check only warns (`:485-492`); it surfaces later as an exception inside
   `SegmentizerFactory.factorize` (`:217`), which fails the load and is reported as `FAILED`.

7. **Announcement size overflow.** A single segment whose JSON exceeds `maxBytesPerNode` (512 KiB)
   throws `ISE("byte size %,d exceeds %,d")` from `announceSegment`
   (`BatchDataSegmentAnnouncer.java:169-171`). That propagates out of
   `SegmentLoadDropHandler.addSegment`'s announce block as `SegmentLoadingException("Failed to
   announce segment")` (`:175-177`) → `FAILED`. The segment is loaded on disk and in the local
   timeline but **never announced**, so `isHandOffComplete` never returns true and the task waits out
   `handoffConditionTimeout`. Mitigations are all config: raise `maxBytesPerNode`, or
   `skipDimensionsAndMetrics` / `skipLoadSpec` to shrink the payload (`:107-112`). Note the payload
   is only reduced for the ZK path; the HTTP `changes.addChangeRequest` at `:161` happens *before*
   the size check, so HTTP-based views do see it.

8. **Broker view desync.** Three mechanisms, in order of severity. (i) The Historical's
   `ChangeRequestHistory` keeps only 1000 changes; a client more than that behind gets
   `ChangeRequestsSnapshot.fail(...)` (`ChangeRequestHistory.java:188-197`), the syncer sets
   `counter = null` (`ChangeRequestHttpSyncer.java:285-289`) and the next request is a **full sync**
   which diffs and emits both adds and removes (`HttpServerInventoryView.java:585-608`). (ii)
   Repeated request failures back off exponentially to 2 min (`:388-394`) and alert once
   `serverUnstabilityTimeout` is exceeded (`:434`), during which the Broker's timeline keeps the
   **stale** entries — queries route to a server that may no longer hold the segment, which surfaces
   as a Historical-side `ReportTimelineMissingSegmentQueryRunner` (`server/src/main/java/org/apache/druid/server/coordination/ServerManager.java:176`). (iii) A
   syncer that stops issuing requests entirely is caught by `checkAndResetUnhealthyServers` every 5
   minutes (`HttpServerInventoryView.java:226-231`, `:466-487`), which removes and re-adds the
   server. Worst-case staleness ≈ `maxDelayBetweenSyncRequests` (`3 * (serverTimeout + 5s) + 2 min`
   ≈ 14 min with defaults) plus up to 5 min of monitor latency.

Two more, briefly. **Historical dies mid-query**: `DirectDruidClient`'s response handler
`exceptionCaught` (`:415`) and the future's `onFailure` (`:517`) decrement `openConnections` and
cancel remotely; the exception propagates through the merge and fails the Broker query. The Broker
learns of the death only via the syncer (see 8), so the *next* query still targets that server until
the view updates — which is why `ServerSelector` keeping multiple servers per segment
(`server/src/main/java/org/apache/druid/client/BrokerServerView.java:345`) matters. **Merge-buffer / lane rejection**: `QueryScheduler`
bulkheads use `maxWaitDuration(Duration.ZERO)` (`:336`), so an over-capacity query is rejected
instantly with `QueryCapacityExceededException` (`:285`, `:297`) rather than queueing; permits are
held for the entire streamed response (`before()`/`after()`, `:197-208`), so slow clients consume
lane capacity for their full duration.

---

## Optimizations in the current structure

- **Load-queue batching** — `HttpLoadQueuePeon.doSegmentManagement` sends up to `batchSize` change
  requests in one POST (`:203-230`), where `batchSize` defaults to the *target Historical's*
  `numLoadingThreads` (`:377`), and immediately re-runs when the batch made progress (`:329-331`).
  Cost: `batchSize` is derived from a capability probe done **once in the constructor** (`:150`), and
  the whole thing runs on one cluster-wide thread (`services/src/main/java/org/apache/druid/cli/CliCoordinator.java:282`).
- **Announcement batching** — `BatchDataSegmentAnnouncer` packs many segments per znode up to
  `segmentsPerNode`/`maxBytesPerNode` (`:174-217`, `:284-292`), turning N znodes into N/50. Cost: a
  single oversized segment hard-fails (`:169`), and updating one segment rewrites the whole znode
  (`announcer.update`, `:186`).
- **Segment-row batching** — `insertSegmentsInBatches` uses JDBI `PreparedBatch` in partitions of 100
  (`SqlSegmentMetadataTransaction.java:505-545`). Cost: one oversized batch can trip a
  packet-size limit — hence `isRootCausePacketTooBigException` exists
  (`server/src/main/java/org/apache/druid/metadata/SQLMetadataConnector.java:221`).
- **Deletes batched too** — pending-segment deletes partition at 100 (`:443`, `:301-306`).
- **Result caching, two levels** — segment-level via a single bulk get
  (`server/src/main/java/org/apache/druid/client/CachingClusteredClient.java:586`, capped by `cacheBulkMergeLimit`) with **partial hits**: hits
  are removed from the query set and misses still go to servers (`:552-560`); plus result-level etag
  short-circuit (`:354-363`). Cost: `populateCache` forces `bySegment = true` downstream
  (`:315`), which changes the wire format and disables Historical-side caching.
- **Incremental server-view sync** — counter+hash long poll, delta by default, full only on reset
  (`ChangeRequestHttpSyncer.java:291-297`; `ChangeRequestHistory.java:166-215`). Cost: a 1000-change
  history bound (`:50`) and a full-sync diff over every segment on that server when it trips.
- **`DataSegment` interning** — `DataSegmentInterner.intern` on every synced segment
  (`server/src/main/java/org/apache/druid/client/HttpServerInventoryView.java:637`) and `replaceWithExistingSegmentIfPresent` on every polled row
  (`server/src/main/java/org/apache/druid/metadata/SqlSegmentsMetadataManager.java:664`, rationale `:818-826`), keeping most deserialized objects in
  young-gen.
- **Lazy / streaming responses** — the Broker builds a plan but sends nothing until the
  `LazySequence` is consumed (`CachingClusteredClient.java:379-384`); `DirectDruidClient` parses
  chunks as they arrive through `JsonParserIterator` (`:539`) with channel-level backpressure
  (`:203-227`) budgeted as `maxQueuedBytes / numServers` (`:693-694`).
- **Async servlets on both internal endpoints** — `startAsync()` for the inventory long poll and the
  change-request POST (`server/src/main/java/org/apache/druid/server/http/SegmentListerResource.java:146`, `:260`), so no Jetty thread is held while a
  Historical loads segments or waits for changes.
- **Parallel segment loads on the Historical** — `normalLoadExec` has
  `numLoadingThreads = max(1, cores/6)` threads (`SegmentLoadDropHandler.java:97-100`,
  `server/src/main/java/org/apache/druid/segment/loading/SegmentLoaderConfig.java:53`), plus a TURBO pool sized `numBootstrapThreads` with core-thread
  timeout so it costs nothing when idle (`:103-110`, `:130`). Locking is per-segment
  (`server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:359`), so distinct segments download concurrently.
- **Cost-balancer caching and pruning** — `computePlacementCost` operates on
  `server.getProjectedSegmentCounts()`, a pre-aggregated interval→count map rather than per-segment
  iteration (`CostBalancerStrategy.java:285-310`), and `getCostComputeInterval` clips to ±45 days
  because the joint cost beyond that is negligible (`:393-401`). Costs are computed in parallel, one
  future per candidate server (`:331-337`). Cost: still O(candidate servers × distinct intervals) per
  segment considered, with a hard 1-minute barrier (`:350`) — the reason `BalanceSegments` is usually
  the most expensive duty and why `smartSegmentLoading` derives `maxSegmentsToMove` from the
  coordinator period and thread count
  (`server/src/main/java/org/apache/druid/server/coordinator/duty/BalanceSegments.java:89-90`).
- **Round-robin assignment as the default** (`server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentLoadingConfig.java:63`) skips cost computation
  entirely for first-copy loads (`StrategicSegmentAssigner.java:524-525`).
- **Per-datasource `ConcurrentHashMap.compute` instead of a global lock** on the Historical
  (`server/src/main/java/org/apache/druid/server/SegmentManager.java:227`, `:286`) — loads and drops of different datasources never contend.

---

## Concurrency & thread model

**Peon (task) JVM**
- read loop: the task's own thread — `StreamAppenderator.add` (`:300`), `Sink.add`.
- `[<id>]-appenderator-persist` — `numPersistThreads`, bounded queue `maxPendingPersists`
  (`:1237`). Bounded ⇒ **`add()` blocks** when persists lag; measured by
  `incrementPersistBackPressureMillis` (`:756`).
- `[<id>]-appenderator-merge` — **1 thread, queue 1** (`:1247`). All `mergeAndPush` (merge + S3 PUT)
  for a task is serialized here.
- `[<id>]-appenderator-abandon` — 1 thread, `SynchronousQueue` (`:1254`), to break persist↔push
  queue cycles (comment `:192-194`).
- `[<id>]-publish` — 1 thread (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:280`), runs `publishInBackground` and the
  push sanity check.
- `StreamAppenderSegmentRemoval-%s` — 1 scheduled thread (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:253`).
- `coordinator_handoff_scheduled_%d` — 1 scheduled thread
  (`server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifier.java:76`); handoff callbacks run **on this thread**
  because `registerHandoff` passes `Execs.directExecutor()`
  (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:361`), so `Appenderator.drop` latency delays subsequent polls.
- Locks: `Sink.hydrantLock` guards every hydrant transition (`server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:179`, `:224`, `:241`, `:328`);
  `synchronized (hydrant)` in `persistHydrant` (`:1624`) and in `mergeAndPush`'s sanity loop (`:898`);
  `StreamAppenderator.commitLock` serialises `commit.json` read-modify-write (`:163`, `:707`);
  `synchronized (segments)` in the driver (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:301`);
  `synchronized (abandonedSegments)` (`:603`); an exclusive `FileLock` on the base persist dir
  (`:1205`) prevents two appenderators sharing it.

**Coordinator**
- `Coordinator-Exec-<group>-%d` — **1 thread per duty group** (`server/src/main/java/org/apache/druid/server/coordinator/DruidCoordinator.java:708`). Runs
  `RunRules`, `BalanceSegments`, etc. sequentially; overruns stretch the period.
- `Master-PeonExec--%d` — **1 thread for the whole cluster**
  (`services/src/main/java/org/apache/druid/cli/CliCoordinator.java:282`), driving every `HttpLoadQueuePeon.doSegmentManagement` and every
  response callback (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:355`). **Primary contention point**: one slow
  Historical's JSON parsing or a `mainLoopInProgress` hold delays load management for all servers.
- `LoadQueuePeon-callbackexec--%d` — 1 thread for load/drop callbacks (`services/src/main/java/org/apache/druid/cli/CliCoordinator.java:278`).
- balancer pool — `balancerComputeThreads` threads via `BalancerStrategyFactory`
  (`server/src/main/java/org/apache/druid/server/coordinator/duty/PrepareBalancerAndLoadQueues.java:100`),
  consumed by `CostBalancerStrategy.exec` (`:218`, `:333`).
- metadata poll — `scheduleWithFixedDelay` on the manager's `exec`
  (`server/src/main/java/org/apache/druid/metadata/SqlSegmentsMetadataManager.java:291`).
- `CoordinatorServerView` inventory — same `HttpServerInventoryView` pools as the Broker (below).
- Locks: `DruidCoordinator.lock` (`synchronized` in `start`/`becomeLeader`/`stopBeingLeader`/
  `DutiesRunnable.run`, `:368`, `:435`, `:525`, `:715`) — leadership transitions serialise against
  duty ticks; `HttpLoadQueuePeon.lock` guards `queuedSegments`/`activeRequestSegments`/maps (`:116`);
  `SqlSegmentsMetadataManager.startStopPollLock` (read-write) + `pollLock`
  (`:113-129`, `:274`, `:639`); `LoadQueueTaskMaster` methods are `synchronized` (`:107`).
  `ServerHolder`, `DruidCluster`, `SegmentReplicaCountMap` and `ReplicationThrottler` are
  **single-run, single-thread** and therefore unsynchronized by design.

**Historical**
- `SegmentLoadDropHandler-normal-%s` — `numLoadingThreads` scheduled threads (`:97-100`); also runs
  delayed drops (`:242`).
- `SegmentLoadDropHandler-turbo-%s` — `numBootstrapThreads`, core-timeout 60s (`:103-110`, `:130`).
- Jetty request threads — `SegmentListerResource`, `QueryResource`; released immediately via
  `startAsync()` for both internal endpoints (`:146`, `:260`).
- query processing pool + merge buffers — `processing/` (out of scope here), gated by
  `QueryScheduler` bulkheads.
- `ChangeRequestHistory` single-threaded executor for resolving long polls (`:106`).
- Locks: `SegmentManager.dataSources.compute` = the **per-datasource** lock for timeline
  add/remove (`:227`, `:286`); `SegmentLocalCacheManager` per-segment `ReferenceCountingLock`
  (`:359`) plus a global `directoryWriteRemoveLock` for marker-file create/delete (`:460`);
  `StorageLocation` methods are `synchronized` on the location (`:101`, `:115`, `:137`, `:163`,
  `:226`) — so all locations' accounting serialises per location;
  `SegmentLoadDropHandler.requestStatusesLock` (`:80`, `:317`, `:373`) and
  `synchronized (waitingFutures)` (`:299`, `:384`); `BatchDataSegmentAnnouncer.lock` serialises all
  announce/unannounce (`:74`, `:153`, `:224`, `:261`); `ChangeRequestHistory` methods are
  `synchronized` (`:92`, `:128`, `:166`).

**Broker**
- `HttpServerInventoryView-%s` — `numThreads` (default 5) shared by **all** syncers
  (`HttpServerInventoryView.java:158-161`); every `sendSyncRequest`, response callback and
  `addNextSyncToWorkQueue` runs here (`server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHttpSyncer.java:349`, `:396`). With many
  Historicals this is a real contention point.
- `HttpServerInventoryView-monitor-%s` — 1 thread for `checkAndResetUnhealthyServers` +
  `emitServerStatusMetrics` (`:162`, `:226-237`).
- `BrokerServerView-%s` — **1 thread** for all segment/server callbacks (`server/src/main/java/org/apache/druid/client/BrokerServerView.java:124`).
  Timeline mutation is therefore both single-threaded *and* `synchronized (lock)`.
- Netty HTTP client pool — `numConnections` (default 20) per destination, `numMaxThreads` worker
  threads (`server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:44`, `:51`).
- Jetty request threads run `CachingClusteredClient.run` and consume the merged sequence;
  `ParallelMergeCombiningSequence` uses the shared fork-join `pool`
  (`CachingClusteredClient.java:394-395`).
- Locks/limiters: `BrokerServerView.lock` (`:265`, `:312`, `:369`) — held across timeline
  add/remove and `getTimeline`, so a heavy sync burst can briefly stall query planning;
  `QueryScheduler` bulkheads with zero wait (`server/src/main/java/org/apache/druid/server/QueryScheduler.java:336`); `queryFutures`/
  `queryDatasources` are synchronized multimaps (`:107-108`).

---

## Config knobs that change execution

| Property | Class field (`path:LINE`) | Default | What it changes |
|---|---|---|---|
| `druid.coordinator.period` | `server/src/main/java/org/apache/druid/server/coordinator/config/CoordinatorRunConfig.java:42` | `PT1M` | Historical-management duty group period; also feeds `BalanceSegments` `maxSegmentsToMove` sizing (`server/src/main/java/org/apache/druid/server/coordinator/duty/BalanceSegments.java:90`) |
| `druid.coordinator.startDelay` | `server/src/main/java/org/apache/druid/server/coordinator/config/CoordinatorRunConfig.java:41` | `PT5M` | Delay before the first duty run after leadership (`server/src/main/java/org/apache/druid/server/coordinator/DruidCoordinator.java:502`) |
| `druid.coordinator.period.indexingPeriod` | `server/src/main/java/org/apache/druid/server/coordinator/config/CoordinatorPeriodConfig.java:41` | `PT30M` | Indexing duty group period (kill, compact) |
| `druid.coordinator.period.metadataStoreManagementPeriod` | `CoordinatorPeriodConfig.java:42-45` | `PT60M` | Metadata-cleanup duty group period |
| `druid.manager.segments.pollDuration` | `server/src/main/java/org/apache/druid/metadata/SegmentsMetadataManagerConfig.java:53` | `PT1M` | Fixed **delay** between full `SELECT ... used=true` polls (`server/src/main/java/org/apache/druid/metadata/SqlSegmentsMetadataManager.java:291`); sets the used-segment staleness window |
| `druid.coordinator.loadqueuepeon.http.hostTimeout` | `server/src/main/java/org/apache/druid/server/coordinator/config/HttpLoadQueuePeonConfig.java:51` | `PT5M` | `?timeout=` on the change-request POST and (+5s) the HTTP read timeout (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:252`, `:264`) |
| `druid.coordinator.loadqueuepeon.http.repeatDelay` | `server/src/main/java/org/apache/druid/server/coordinator/config/HttpLoadQueuePeonConfig.java:52` | `PT1M` | Base tick for `doSegmentManagement` (`HttpLoadQueuePeon.java:426-428`) |
| `druid.coordinator.loadqueuepeon.http.batchSize` | `server/src/main/java/org/apache/druid/server/coordinator/config/HttpLoadQueuePeonConfig.java:53` | null ⇒ target's `numLoadingThreads` | Requests per POST (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:377`) |
| (hard-coded) load timeout | `server/src/main/java/org/apache/druid/server/coordinator/config/HttpLoadQueuePeonConfig.java:32`, `:80` | 15 min | When a queued holder is failed as `"timed out"` (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:211`) |
| `replicationThrottleLimit` (dynamic) | `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:460` | 500 (overridden to `max(100, 5%)` by smart loading, `server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentLoadingConfig.java:51`) | Replica assignments per tier per run (`ReplicationThrottler.java:64-76`) |
| `maxSegmentsInNodeLoadingQueue` (dynamic) | `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:461` | 500 (⇒ **0 = unlimited** under smart loading, `server/src/main/java/org/apache/druid/server/coordinator/loading/SegmentLoadingConfig.java:60`) | `ServerHolder.maxAssignmentsInRun` (`ServerHolder.java:130-132`) and thus `canLoadSegment` (`:269`) |
| `smartSegmentLoading` (dynamic) | `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:465` | `true` | Ignores the three knobs above and derives them (`SegmentLoadingConfig.java:48-65`) |
| `useRoundRobinSegmentAssignment` (dynamic) | `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:464` | `true` | Round-robin vs `CostBalancerStrategy` for first-copy placement (`StrategicSegmentAssigner.java:524-526`) |
| `balancerComputeThreads` (dynamic) | `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:60`, `:139` | derived | Size of `CostBalancerStrategy.exec`; too low ⇒ 1-min timeouts (`server/src/main/java/org/apache/druid/server/coordinator/balancer/CostBalancerStrategy.java:350`, `:379`) |
| `maxSegmentsToMove` (dynamic) | `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:458` | 100 | Upper bound per tier in `BalanceSegments` (`server/src/main/java/org/apache/druid/server/coordinator/duty/BalanceSegments.java:57`, `:98`) |
| `pauseCoordination` (dynamic) | `server/src/main/java/org/apache/druid/server/coordinator/CoordinatorDynamicConfig.java:462` | `false` | Skips every duty group run (`CoordinatorDutyGroup.java:106-110`) |
| `druid.segmentCache.numLoadingThreads` | `server/src/main/java/org/apache/druid/segment/loading/SegmentLoaderConfig.java:53` | `max(1, cores/6)` | Historical parallel loads (`SegmentLoadDropHandler.java:97-100`); also the peon's default batch size |
| `druid.segmentCache.dropSegmentDelayMillis` | `server/src/main/java/org/apache/druid/segment/loading/SegmentLoaderConfig.java:47` | 30 000 | Delay before a drop is executed (`SegmentLoadDropHandler.java:242-246`) |
| `druid.segmentCache.locations[].maxSize` / `freeSpacePercent` | `server/src/main/java/org/apache/druid/segment/loading/StorageLocation.java:48`, `:69` | — | `canHandle` admission (`:196`, `:209`) |
| `druid.segmentCache.lazyLoadOnStart` | `server/src/main/java/org/apache/druid/segment/loading/SegmentLoaderConfig.java:41` | `false` | Bootstrap-only: `factorize(..., lazy, callback)` (`server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:230`) |
| `druid.announcer.maxBytesPerNode` | `server/src/main/java/org/apache/druid/server/initialization/BatchDataSegmentAnnouncerConfig.java:38` | 524 288 | Announcement batching and the hard per-segment limit (`server/src/main/java/org/apache/druid/server/coordination/BatchDataSegmentAnnouncer.java:169`, `:179`) |
| `druid.announcer.segmentsPerNode` | `server/src/main/java/org/apache/druid/server/initialization/BatchDataSegmentAnnouncerConfig.java:33` | 50 | Segments per znode before retirement (`server/src/main/java/org/apache/druid/server/coordination/BatchDataSegmentAnnouncer.java:189`) |
| `druid.serverview.serverTimeout` | `server/src/main/java/org/apache/druid/client/HttpServerInventoryViewConfig.java:55` | 4 min | Long-poll `?timeout=`; drives `serverHttpTimeout`, `maxDurationToWaitForSync` (3×) and `maxDelayBetweenSyncRequests` (3×+2 min) (`ChangeRequestHttpSyncer.java:125-130`) |
| `druid.serverview.serverUnstabilityTimeout` | `server/src/main/java/org/apache/druid/client/HttpServerInventoryViewConfig.java:59` | 1 min | When sync failures start alerting (`server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHttpSyncer.java:434`) |
| `druid.serverview.numThreads` | `server/src/main/java/org/apache/druid/client/HttpServerInventoryViewConfig.java:61` | 5 | Threads shared by all syncers (`HttpServerInventoryView.java:158-161`) |
| `handoffConditionTimeout` (tuningConfig, ms) | enforced at `SeekableStreamIndexTaskRunner.java:880-886` | 0 = wait forever | Bounds the task's handoff wait; timeout is alerted and **ignored** (`:888-895`) |
| `druid.segment.handoff.pollDuration` | `server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifierConfig.java:29` | **`PT1S`** | `handoffComplete` poll interval (`server/src/main/java/org/apache/druid/segment/handoff/CoordinatorBasedSegmentHandoffNotifier.java:80`) |
| `maxRowsInMemory` (tuningConfig) | `server/src/main/java/org/apache/druid/segment/indexing/TuningConfig.java:50` | 150 000 (realtime) | Persist trigger (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:363`) and per-`IncrementalIndex` row cap (`server/src/main/java/org/apache/druid/segment/realtime/sink/Sink.java:323`) |
| `maxBytesInMemory` (tuningConfig) | `TuningConfig.java:73-83` | 0 ⇒ index default; `<0` ⇒ unbounded | Persist trigger (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:371`) and the heap safety valve (`:399-427`) |
| `intermediatePersistPeriod` | used at `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1280` | — | Time-based persist trigger via `nextFlush` (`:355`) |
| `maxPendingPersists` | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1232`, `:1239` | — | Persist executor queue depth ⇒ how long `add()` blocks |
| `numPersistThreads` | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1235`, `:1239` | — | Persist parallelism (merge stays single-threaded) |
| `druid.broker.http.numConnections` | `server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:44` | 20 | Concurrent connections per Historical from `DirectDruidClient` |
| `druid.broker.http.readTimeout` | `server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:47` | `PT15M` | Ceiling on the per-request read timeout (actual = remaining budget, `server/src/main/java/org/apache/druid/client/DirectDruidClient.java:497`) |
| `druid.broker.http.maxQueuedBytes` | `server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:123` | — | Per-server backpressure budget, divided by server count (`CachingClusteredClient.java:693-694`) |
| `druid.broker.cache.useCache` / `populateCache` | `server/src/main/java/org/apache/druid/client/cache/CacheConfig.java:35`, `:38` | `false` / `false` | Whether `computeCachedValues` reads (`server/src/main/java/org/apache/druid/client/CachingClusteredClient.java:585`) and whether `bySegment` is forced downstream (`:315`) |
| `druid.broker.cache.cacheBulkMergeLimit` | `server/src/main/java/org/apache/druid/client/cache/CacheConfig.java:52` | `MAX_VALUE` | Cap on keys per `cache.getBulk` (`server/src/main/java/org/apache/druid/client/CachingClusteredClient.java:586`) |
| `druid.broker.cache.maxEntrySize` | `server/src/main/java/org/apache/druid/client/cache/CacheConfig.java:55` | 1 000 000 | Largest cacheable segment result |
| `druid.query.scheduler.numThreads` | `server/src/main/java/org/apache/druid/server/QueryScheduler.java:112-120` | 0 (no total limit) | Installs the `TOTAL` bulkhead only if `0 < n < serverConfig.numThreads` and queuing is off |
| `druid.server.http.numThreads` | read at `server/src/main/java/org/apache/druid/server/QueryScheduler.java:113`, `:119` | — | Fallback `totalCapacity` and the base for lane limits (`:339`) |

---

## Sharp edges

1. **`handoffComplete == true` does not mean the data is queryable.** Two independent shortcuts
   return `true` without any Historical holding the segment: no matching `LoadRule` / zero total
   replicas (`DataSourcesResource.java:930-932`), and a newer version present in the timeline
   (`:946-948`). A third path — timeout — also lets the task succeed
   (`SeekableStreamIndexTaskRunner.java:888-895`). **Committed offsets and a green task never imply
   queryability.**
2. **`handoffComplete` reads the Coordinator's own inventory, not the Historicals.**
   `server/src/main/java/org/apache/druid/server/http/DataSourcesResource.java:934` uses `CoordinatorServerView`, which is fed by the same
   `HttpServerInventoryView` long-poll machinery as the Broker. A degraded syncer makes handoff
   answers stale in *both* directions — false negatives (segment loaded, not yet seen) and false
   positives (segment gone, still in the view).
3. **The handoff notifier polls serially and blocks.** `checkForSegmentHandoffs` does a blocking
   `FutureUtils.getUnchecked` per descriptor on **one** thread
   (`CoordinatorBasedSegmentHandoffNotifier.java:88-100`), and the success callback runs inline via
   `directExecutor()` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:361`). With the default `PT1S` period and many
   waiting segments, a slow Coordinator turns the "1 second poll" into a much longer effective cycle.
4. **One Coordinator thread drives every Historical's load queue.** `Master-PeonExec--%d` is
   `factory.create(1, ...)` (`services/src/main/java/org/apache/druid/cli/CliCoordinator.java:282`)
   and is used for `doSegmentManagement`, the HTTP response callbacks
   (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:355`) and the immediate re-runs (`:330`). Response parsing for a big
   batch happens on that thread, under `lock` (`:283`).
5. **`serverCapabilities` is probed once, in the peon constructor** (`server/src/main/java/org/apache/druid/server/coordinator/loading/HttpLoadQueuePeon.java:150`),
   and a 404 permanently pins the batch size to `config.getBatchSize()` or 1 (`:169-177`). Changing
   a Historical's `numLoadingThreads` has no effect until its peon is recreated.
6. **`PENDING` load responses are invisible.** They are only trace-logged (`:303-305`); the holder
   stays queued with no distinct metric, so a Historical that is slowly loading looks the same as one
   that is ignoring requests — until the 15-minute `loadTimeout` fires.
7. **Under `smartSegmentLoading` (default on), `maxSegmentsInNodeLoadingQueue` and
   `replicationThrottleLimit` are ignored.** `SegmentLoadingConfig.create` substitutes `0`
   (unlimited) and `max(100, 5% of used segments)` (`SegmentLoadingConfig.java:48-65`).
   Tuning them without turning smart loading off is a no-op.
8. **A single oversized segment announcement is a permanent handoff stall.** `announceSegment` throws
   `ISE` when one segment's JSON exceeds `maxBytesPerNode`
   (`BatchDataSegmentAnnouncer.java:169-171`) — the segment loads fine and is locally queryable, but
   is never announced, so `handoffComplete` stays `false` forever.
9. **`useUniquePath = true` defeats the push idempotency check.** `mergeAndPush`'s
   `descriptor.json` short-circuit only applies when `useUniquePath` is false
   (`StreamAppenderator.java:909-925`), and streaming always passes `true`
   (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:288`). Every retry writes fresh S3 objects; cleanup depends on
   `publishInBackground`'s disjoint-`loadSpec` heuristic (`BaseAppenderatorDriver.java:682-689`) and
   otherwise on the `killUnusedSegments` duty.
10. **On ambiguous publish failures, deep-storage objects are intentionally leaked.**
    `BaseAppenderatorDriver.java:707-716` explicitly refuses to delete: *"Must not remove segments
    here, we aren't sure if our transaction succeeded or not."* Correct, but it means a flapping
    metadata store grows S3 garbage.
11. **`retryableFailure` is a return value, not an exception.** It does not trigger
    `SQLMetadataConnector.retryTransaction`'s retry (`:180-187`), and because it returns normally,
    `executeWriteAndClose` does not `setRollbackOnly()`
    (`server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentMetadataTransactionFactory.java:150-159`).
    It is safe only because the metadata check runs before any insert.
12. **Background persist failures surface late and elsewhere.** `persistError` is set in a
    `directExecutor` callback (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:442`) and only rethrown by the **next**
    `add()` (`:307` → `:294`). If ingestion pauses, the failure sits silent.
13. **`maxRowsInMemory` / `maxBytesInMemory` are appenderator-wide, not per-sink.**
    `rowsCurrentlyInMemory` / `bytesCurrentlyInMemory` aggregate all sinks (`:343-344`), and a
    trigger persists **all** sinks (`persistAll`, `:641`). A task with many concurrent segments
    persists far more often than a per-segment reading of the config suggests.
14. **Segment integrity is a marker file, not a checksum.** Only an interrupted download is detected
    (`server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:344`); a size mismatch merely warns (`:485-492`).
15. **`maybeReserve` can overcommit disk.** It skips `canHandle` entirely and only alerts when
    accounting goes negative (`StorageLocation.java:137-154`), so a Historical restarting with more
    cached data than `maxSize` proceeds past its configured limit.
16. **The Broker timeline lock is held across mutation and `getTimeline`.** `BrokerServerView.lock`
    guards `serverAddedSegment` (`:265`), `serverRemovedSegment` (`:312`) and `getTimeline` (`:369`),
    all fed from a **single** callback thread (`:124`). A burst full-sync from a large Historical can
    briefly serialise query planning.
17. **A segment announced by a server absent from the inventory is silently dropped** from the
    Broker's timeline (`BrokerServerView.java:289-296`) — an ordering hazard between server-add and
    segment-add callbacks.
18. **Query lane permits are held for the whole streamed response**, acquired in `before()` and
    released in `after()` (`QueryScheduler.java:197-208`), with zero wait time (`:336`). Slow
    consumers occupy capacity and cause immediate `QueryCapacityExceededException` for others rather
    than queueing.
19. **`populateCache` on the Broker silently changes the wire protocol.**
    `makeDownstreamQueryContext` forces `bySegment = true` and `populateCache = false` downstream
    (`CachingClusteredClient.java:312-316`), so enabling Broker caching disables Historical caching
    and changes result framing.
20. **The V1 metadata poll is O(all used segments) of heap per cycle** — one full
    `SELECT payload ... WHERE used=true` (`server/src/main/java/org/apache/druid/metadata/SqlSegmentsMetadataManager.java:659`) plus overshadow
    computation (`:807-815`) every `pollDuration`. Interning (`:827`) is the only mitigation short of
    enabling the V2 incremental cache (`server/src/main/java/org/apache/druid/metadata/segment/SqlSegmentsMetadataManagerV2.java:90`).
21. **The `1000`-change `ChangeRequestHistory` bound is not configurable**
    (`server/src/main/java/org/apache/druid/server/coordination/ChangeRequestHistory.java:50`). A Historical that loads >1000 segments faster than a client
    polls forces that client into a full resync (`:188-197`).
