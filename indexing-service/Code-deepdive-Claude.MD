# Code deep dive — druid-indexing-service

Scope: the *execution mechanics* of streaming ingestion in this module — how
`SeekableStreamIndexTaskRunner.runInternal` actually drives its loop, which thread each
mutation happens on, how the publish/handoff future graph is wired, what survives a restart,
and what each retry/timeout really does. Class-to-responsibility mapping is **not** repeated
here: see `indexing-service/CLAUDE.md` for the class map, endpoint list and cross-module
pointers. Everything below was read from method bodies; line numbers verified with `grep -n`
against branch `34.0.0-confluent` (Druid 34.0.0). Inferences are marked `(inferred)`,
unconfirmed items `(unverified)`.

Ordering fact this document assumes and reconfirms: Kafka offsets are committed in the **same
metadata transaction as the segment publish**, **before** any Historical loads the segment.
Handoff only gates *when the task may exit*.

---

## Happy path, traced

Threads referenced below:
- **`task-runner-%d-priority-N`** — the Peon's single `SingleTaskBackgroundRunner` executor
  (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/SingleTaskBackgroundRunner.java:148-156`).
  This is `runThread`; the whole of `runInternal` runs here.
- **`[<appenderatorId>]-appenderator-persist`** — persist pool, `numPersistThreads` threads,
  bounded queue `maxPendingPersists` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1236-1241`).
- **`[<appenderatorId>]-appenderator-merge`** — blocking *single*-threaded push/merge executor
  (`StreamAppenderator.java:1246-1248`).
- **`[<appenderatorId>]-publish`** — single-threaded publish executor
  (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:280-282`).
- **Jetty threads** — the `@POST /pause`, `/resume`, `/offsets/end`, `/stop` chat handlers on
  the runner itself.
- **`TaskQueue-Manager`, `TaskQueue-StorageSync`, `TaskQueue-OnComplete-%d`** — Overlord.
- **`SegmentAllocQueue-Manager-%s` / `-Worker-%s`** — Overlord segment allocation.

### 0. Peon bootstrap (Indexer/MiddleManager → Peon)

1. `ForkingTaskRunner.run(task)` on `forking-task-runner-%d`
   (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java:163`,
   pool sized to `workerConfig.getCapacity()` at `:157-159`). Inside `synchronized (tasks)` it
   creates the work item and submits the body to `exec`.
2. On that pool thread: `getTracker().pickStorageSlot(task.getId())` (`:177`) →
   `taskDir = <slot>/<taskId>`, `attemptId = getNextAttemptID(taskDir)`, `attemptDir` (`:187-189`).
   Ports are leased from `PortFinder` (`:196-202`) and `TaskLocation` is built at `:204`.
3. Under a second `synchronized (tasks)` (`:215`) the command line is assembled: classpath
   (`:236-243`), `-XX:ActiveProcessorCount=numProcessorsPerTask` (`:259`), `javaOpts` /
   `javaOptsArray` then **task-context overrides** of both (`:261-288`), allow-listed parent
   properties (`:290-300`), `druid.indexer.fork.property.*` (`:303-310`), then
   `org.apache.druid.cli.Main internal peon <taskDir> <attemptId>` (`:367-371`).
   `task.json` is written only if absent (`:389-391`) — so a restored attempt reuses it.
4. `runTaskProcess` (`:397`, `:498-505`) starts the JVM with `redirectErrorStream(true)`;
   listeners get `notifyLocationChanged` then `TaskStatus.running` (`:403-408`).
5. The pool thread then **blocks in `waitForTaskProcessToComplete`** (`:411`, `:508-529`), which
   is simultaneously the log capture loop: `ByteStreams.copy(process.getInputStream(), toLogfile)`
   then `process.waitFor()` (`:518-519`). Task death is therefore detected as *stdout EOF +
   process exit*, nothing else. In the `finally`, logs and `report.json` are pushed (`:524-527`).
6. Exit code 0 → status is **read from `status.json`** (`:416`); non-zero → synthesized
   `TaskStatus.failure` (`:420-427`). Then `notifyStatusChanged` (`:434`) and the `finally`
   cleanup: remove work item, `processHolder.shutdown()`, release ports, return storage slot,
   delete `taskDir` unless `stopping` (`:448-485`).

Peon side, `ExecutorLifecycle.start()`
(`indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java:90`):
reads `task.json` (`:97`); if a parent stream exists, takes an exclusive `FileLock` on the task
lock file, polling every 100 ms up to `directoryLockTimeout` (`:112-140`); spawns
`parent-monitor-%d` which reads the inherited stdin and calls `System.exit(2)` on EOF (`:148-163`)
— this is how "MiddleManager closed my stdout" becomes "Peon dies"; calls `task.isReady(...)`
(`:169`); then `taskRunner.run(task)` with a `Futures.transform` that **writes `status.json`**
(`:177-204`). `join()` blocks on that future (`:207-215`).

### 1. `runInternal` prologue (runThread)

`run(toolbox)` (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:292`) is a thin wrapper: any escaping
exception is turned into `TaskStatus.failure` after writing a completion report (`:297-305`).

`runInternal` (`:396`):
- `startTime = now`, `status = STARTING` (`:398-399`) — `status` is `volatile` and, per the
  comment at `:236`, only ever written by runThread.
- `rowIngestionMeters`, `ParseExceptionHandler` (`:404-410`), `StreamChunkParser` with the
  min/max message-time predicate (`:413-422`).
- `initializeSequences()` (`:424` → `:335`). This is the first restart-relevant fork:
  `restoreSequences()` (`:1122-1136`) reads `<persistDir>/sequences.json`
  (`getSequencesPersistFile`, `:1117-1120`; `persistDir` = `<taskWorkDir>/persist`,
  `indexing-service/src/main/java/org/apache/druid/indexing/common/TaskToolbox.java:429-432`).
  If that file exists, the in-memory `sequences` list is restored wholesale and the supervisor's
  checkpoints are **ignored**. Otherwise `getCheckPointsFromContext` (abstract `:2047`; Kafka
  impl just deserializes the `checkpoints` context string,
  `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskRunner.java:223-237`)
  is walked pairwise: each adjacent pair becomes a *closed* (`checkpointed=true`) `SequenceMetadata`
  and the last entry becomes an *open* one ending at `endOffsets` (`:346-377`). With no checkpoints
  at all, one open sequence `<baseSequenceName>_0` spanning start→end offsets (`:379-389`).
- `runThread = Thread.currentThread()` (`:429`); metrics monitor added (`:433-435`);
  chat handler registered so the supervisor can reach `/pause` etc. (`:427`).
- `try (recordSupplier = task.newTaskRecordSupplier(toolbox))` (`:456-457`) — the Kafka consumer's
  lifetime is exactly the body of this try.
- Node announcements (`:458-461`), `appenderator` (`:462`), `driver` (`:463`).
- **`driver.startJob(lockAcquireFn)`** (`:466`). The callback re-acquires a lock per restored
  segment: `SegmentLockAcquireAction` for `LockGranularity.SEGMENT`, else
  `TimeChunkLockAcquireAction` with a **1000 ms** timeout and `lock.assertNotRevoked()`
  (`:469-492`). `IOException` is rethrown as `RuntimeException` (`:494-496`).
- Restored-metadata branch (`:499-545`). Null ⇒ brand-new/replacement task: assert every
  `sequences[0].startOffsets` is `>=` the ioConfig start offsets, then `currOffsets.putAll(...)`
  (`:502-510`). Non-null ⇒ `currOffsets` comes from the persisted `nextPartitions` (`:520`),
  with hard `ISE`s if the stream name (`:523-529`) or the partition *key set* (`:531-537`) differs.
  If `sequences` is empty or the last sequence is already checkpointed, `endOffsets` is
  overwritten from that state (`:540-544`).
- `currOffsets.entrySet().removeIf(isEndOfShard)` (`:555`) — defensive, Kinesis-only.
- `lastReadOffsets` seeded **only when end offsets are inclusive** (`:565-575`); for Kafka
  (`isEndOffsetExclusive() == true`) this map starts empty, so the start record is never skipped.
- `committerSupplier` (`:578-597`): snapshots `currOffsets` **and overwrites
  `lastPersistedOffsets`** with that snapshot as a side effect of *calling the supplier* (`:580-581`).
  The `Committer.run()` is a no-op (`:592-595`).
- `maybePersistAndPublishSequences(committerSupplier)` (`:600`) — restarts publishing of any
  sequence restored in a closed state.
- `assignPartitions` (`:602` → `:1234-1261`): a partition joins the assignment only if
  `!isRecordAlreadyRead(p, endOffset) && isMoreToReadBeforeReadingRecord(currOffset, endOffset)`
  (`:1244`); then `recordSupplier.assign(assignment)` (`:1258`).
- `possiblyResetDataSourceMetadata` (`:603`; Kafka's is a **no-op**,
  `KafkaIndexTaskRunner.java:194-201`), `seekToStartingSequence` (`:604` → `:1353-1363`).
- `ingestionState = BUILD_SEGMENTS` (`:606`); `stillReading = !assignment.isEmpty()` (`:610`);
  `status = READING` (`:611`).

### 2. The main `while (stillReading)` loop (runThread, `:615`)

Per iteration, in order:

1. **`possiblyPause()`** (`:616` → `:1381-1407`). Takes `pauseLock.lockInterruptibly()`; if
   `pauseRequested`, sets `status = PAUSED`, `hasPaused.signalAll()`, then
   `while (pauseRequested) shouldResume.await()` — an *unbounded* wait. On wake:
   `status = READING`, `shouldResume.signalAll()`, emit `ingest/pause/time`, return `true`.
   Returning `true` re-runs `assignPartitions` (`:620`) — the supervisor may have shrunk the
   assignment via `setEndOffsets`; the comment at `:617-619` notes no re-seek is needed because
   only drops can happen. If the assignment is now empty, `publishOnStop` and `stopRequested`
   are both set (`:623-627`).
2. `if (stopRequested || sequences.isEmpty() || getLastSequenceMetadata().isCheckpointed())
   status = PUBLISHING` (`:631-633`), then `if (stopRequested) break` (`:635-637`).
3. `backgroundThreadException` check (`:639-641`) — this is how a failed async persist
   (`:736`) or a failed `setEndOffsets` (`:1827`) kills the loop.
4. **`checkPublishAndHandoffFailure()`** (`:643` → `:984-1011`): filters `publishWaitList` and
   `handOffWaitList` for `isDone()`, calls `get()` on each (rethrowing any failure into catch
   block (1)), and **removes the completed futures from the lists** (`:997`, `:1010`). Consequence:
   the terminal `Futures.allAsList(...)` calls at `:871`/`:881` only ever see futures created
   *after* the last loop iteration plus those still pending.
5. `maybePersistAndPublishSequences(committerSupplier)` (`:645` → `:1208-1232`): for each
   sequence, `updateAssignments(currOffsets, isMoreToReadBeforeReadingRecord)`; if it is now
   `!isOpen()` and not already publishing/published, add to `publishingSequences`, do a
   **synchronous** `driver.persist(committer)` (`:1218`) and then `publishAndRegisterHandoff`
   (`:1224`). This is the "sequence rolled over, publish the finished one" path.
6. **`getRecords(recordSupplier, toolbox)`** (`:649-652`). Kafka: `recordSupplier.poll(pollTimeout)`
   (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskRunner.java:93`); `OffsetOutOfRangeException` is caught and routed to
   `possiblyResetOffsetsOrWait` (`:95-104`) which returns an *empty* list, so the loop spins.
   `stillReading` is recomputed from `assignment` immediately after (`:655`), because `getRecords`
   may mutate it.
7. **Per-record body** (`:659-761`):
   - `verifyRecordInRange` (`:660` → `:1999-2031`): throws `ISE` if the record offset is *below*
     `currOffsets` (`:2015-2022`); returns `false` if already read or at/after `endOffsets`.
   - `parser.parse(record.getData(), isEndOfShard(...), record.isFiltered())` (`:671`) —
     the third argument is a **Confluent fork addition**; `StreamChunkParser` counts an empty
     filtered chunk as `filtered` rather than `thrownAway`
     (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/StreamChunkParser.java:117-121`).
   - Sequence selection by `canHandle` (`:674-678`); no match ⇒ `ISE` (`:681-686`).
     `SequenceMetadata.canHandle`
     (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SequenceMetadata.java:199-233`)
     applies `>= start` (or `> start` for exclusive-start partitions) and `< end` when end is
     exclusive, `<= end` otherwise, and requires `isOpen()`.
   - `driver.add(row, sequenceName, committerSupplier, true, false)` (`:690-698`). The last
     `false` **disables incremental persist mid-batch** on purpose (`:695-697`).
   - `addResult.isPushRequired(maxRowsPerSegment, maxTotalRowsOr(DEFAULT_MAX_TOTAL_ROWS))`
     (`:703-707`); if true and the sequence is not yet checkpointed, record
     `pushTriggeringAddResult` + `sequenceToCheckpoint` (`:708-711`).
   - `isPersistRequired |= addResult.isPersistRequired()` (`:712`);
     `partitionsThroughput.merge(...)` (`:713`).
   - `!addResult.isOk()` ⇒ hard `ISE("Could not allocate segment ...")` (`:718`).
   - After the row batch, if `isPersistRequired`, `driver.persistAsync(committer)` with a
     callback whose `onFailure` sets `backgroundThreadException` and logs "Persist failed, dying"
     (`:722-740`, `MoreExecutors.directExecutor()` at `:739` ⇒ the callback runs on the persist
     pool thread).
   - **Offset advance**: `lastReadOffsets.put(p, seq)` and
     `currOffsets.put(p, getNextStartOffset(seq))` (`:746-747`). For Kafka `getNextStartOffset`
     is `seq + 1` (`KafkaIndexTaskRunner.java:80-83`).
   - `isMoreToReadAfterReadingRecord(seq, endOffsets.get(p))` (`:752-755` → `:1341-1351`) — note
     this deliberately ignores exclusivity (`:1349`). When false and the partition is removed
     from the assignment, `recordSupplier.assign(assignment)` is re-issued (`:757-761`).
8. `if (!stillReading) segmentGenerationMetrics.markProcessingDone()` (`:764-768`).
9. **Time-based checkpoint**: `if (System.currentTimeMillis() > nextCheckpointTime)
   sequenceToCheckpoint = getLastSequenceMetadata()` (`:770-776`). `nextCheckpointTime` is set by
   `resetNextCheckpointTime()` = `now + intermediateHandoffPeriod` (`:1844-1847`), called in the
   constructor (`:289`) and on every `setEndOffsets` (`:1759`, `:1791`).
10. **Checkpoint request** (`:786-811`), only `if (sequenceToCheckpoint != null && stillReading)`:
    a `Preconditions.checkArgument` that the sequence is the latest (`:787-794`), then
    `requestPause()` (`:795` — just `pauseRequested = true`, `:1414-1417`), then a **blocking**
    `CheckPointDataSourceMetadataAction` submit carrying the sequence's *start* offsets
    (`:796-807`). A `false` return is fatal: `ISE("Checkpoint request ... failed, dying")` (`:808-810`).
    Because the pause was requested *before* the action, the next loop iteration parks in
    `possiblyPause()` until the supervisor calls `/offsets/end` + `/resume`.

Overlord side of that action: `CheckPointDataSourceMetadataAction.perform`
(`indexing-service/src/main/java/org/apache/druid/indexing/common/actions/CheckPointDataSourceMetadataAction.java:113-120`)
→ `SupervisorManager.checkPointDataSourceMetadata` → `SeekableStreamSupervisor.checkpoint`
(`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:4289-4310`),
which validates stream identity and the `SeekableStreamStartSequenceNumbers` type and then just
enqueues a `CheckpointNotice` (`:4309`). `CheckpointNotice.handle` (`:750-789`) runs on the
supervisor's single notice thread: it scans `taskGroup.checkpointSequences` **descending** for a
matching offset map (`:761-772`); not found ⇒ `ISE("No such previous checkpoint")` (`:774`);
found but not latest ⇒ a replica already checkpointed, return quietly (`:775-780`); else
`checkpointTaskGroup(taskGroup, false)` (`:781`) and `addNewCheckpoint` (`:783`).

`checkpointTaskGroup` (`:3397-3547`, running on `workerExec`): `taskClient.pauseAsync` for every
task in the group (`:3443-3447`), then builds the **max** per-partition offset across replicas
(`:3477-3485`), then `taskClient.setEndOffsetsAsync(taskId, endOffsets, finalize)` for each
(`:3511-3513`). Any task that errors on pause, returns empty offsets, or fails `setEndOffsets` is
killed and dropped from the group (`:3466-3476`, `:3522-3526`).

Task side, `setEndOffsets` (`:1716-1842`, **Jetty thread**): under `pauseLock` it treats a
duplicate request as success and resumes (`:1754-1761`); rejects if the latest sequence is already
checkpointed (400, `:1762-1769`); rejects if not paused (400, `:1770-1774`); rejects any offset
that regresses below `currOffsets` (400, `:1776-1789`). Then `resetNextCheckpointTime()` (`:1791`),
`latestSequence.setEndOffsets(...)` — which also flips `checkpointed = true`
(`SequenceMetadata.java:162-172`) — and either `endOffsets.putAll` when `finish` (`:1801`) or a
**new open sequence** `<base>_<id+1>` (`:1804-1821`). `persistSequences()` (`:1823`) writes
`sequences.json`. Any exception here sets `backgroundThreadException` and still resumes so the
task dies promptly (`:1825-1833`). Finally `resume()` (`:1839` → `:1955-1973`) which waits up to
**5 s** for the runner to leave `PAUSED` and throws otherwise (`:1965`).

### 3. Loop exit and final publish (runThread)

Exit happens by `break` on `stopRequested` (`:636`) or `stillReading == false` (loop condition).
The inner `try/catch/finally` (`:614-833`) always runs `markProcessingDone()` and a **synchronous
`driver.persist(committerSupplier.get())`** in the `finally` (`:820-833`), suppressing its
exception into the original if one is already in flight.

`synchronized (statusLock)` (`:835-841`): if `stopRequested && !publishOnStop`, throw
`InterruptedException("Stopping without publishing")`; otherwise `status = PUBLISHING`. This is
the handshake described at `:196-202` — it prevents `stopGracefully()` from missing the transition.

Then a **snapshot** of `sequences` is iterated (`:846-864`; the copy is required because the
publish success callback removes entries, `:843-845`). For each not-yet-publishing sequence:
`setEndOffsets(currOffsets)` if it is the last one (`:852-855`), `updateAssignments(...)` to clear
assignments (`:859`), add to `publishingSequences` (`:860`), `publishAndRegisterHandoff(...)` (`:862`).

### 4. `publishAndRegisterHandoff` — the future graph (`:1013-1115`)

```java
final ListenableFuture<SegmentsAndCommitMetadata> publishFuture = Futures.transform(
    driver.publish(
        sequenceMetadata.createPublisher(this, toolbox, ioConfig.isUseTransaction()),
        sequenceMetadata.getCommitterSupplier(this, stream, lastPersistedOffsets).get(),
        Collections.singletonList(sequenceMetadata.getSequenceName())
    ),
    publishedSegmentsAndMetadata -> { if (null) throw new ISE(...); return ...; },
    MoreExecutors.directExecutor());
publishWaitList.add(publishFuture);                 // :1035
final SettableFuture<SegmentsAndCommitMetadata> handoffFuture = SettableFuture.create(); // :1038
handOffWaitList.add(handoffFuture);                 // :1039
```

`driver.publish`
(`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderatorDriver.java:277-308`)
chains `pushInBackground(..., useUniquePath=true)` (`:288`) → `publishInBackground(...)`
(`:289-295`) and, on success, drops the sequence from the driver's `segments` map (`:300-306`).
`publishInBackground`
(`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:584-736`)
runs on the single `-publish` thread (`:620`) and calls `publisher.publishSegments(...)` (`:627`).

The publisher is `SequenceMetadataTransactionalSegmentPublisher.publishAnnotatedSegments`
(`SequenceMetadata.java:352-432`). It sanity-checks that `getEndOffsets()` equals the committed
`publishPartitions` (`:373-379`) and then chooses:
- **no segments pushed** → `commitMetadataOnlyAction` if offsets moved, else return OK and skip
  the commit entirely (`:383-410`);
- **`useTransaction`** → `SegmentTransactionalAppendAction.forSegmentsAndMetadata` when
  `taskLockType == APPEND`, else `SegmentTransactionalInsertAction.appendAction`, both carrying
  `startMetadata`/`endMetadata` (`:411-424`);
- **`useTransaction == false`** → the same actions with `null` metadata (`:425-428`).

`toolbox.getTaskActionClient().submit(action)` (`:431`) is the single call that carries segments +
offsets to the Overlord. `SegmentTransactionalInsertAction.perform`
(`indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentTransactionalInsertAction.java:198-263`)
runs `TaskLocks.checkLockCoversSegments` (`:224`) then, **inside**
`taskLockbox.doInCriticalSection(...)` (`:235`), `commitSegmentsAndMetadata(segments, supervisorId,
startMetadata, endMetadata, schemas)` (`:240`) — one metadata-store transaction. If the locks are
invalid, `SegmentPublishResult.fail("Invalid task locks...")` (`:249-252`).

**The ordering that matters:** `driver.registerHandoff(...)` is only reached inside
`publishFuture`'s `onSuccess` (`:1046-1104`), at `:1070`. Before that call, the success callback
mutates state on the `-publish` thread (via `directExecutor` at `:1113`):
`publishedSequences.add(name)`, `sequences.remove(sequenceMetadata)`,
`publishingSequences.remove(name)` (`:1056-1058`), then `persistSequences()` (`:1061`) —
a `synchronized` rewrite of `sequences.json` (`:1138-1145`); an `IOException` there sets the
exception on `handoffFuture` and rethrows (`:1063-1067`). Then `registerHandoff`'s result is
transformed so that a `null` (recoverable handoff failure) is *logged as a warning* and still
`handoffFuture.set(null)` (`:1077-1088`). `ingest/segments/count` is emitted at `:1099-1103`.
`onFailure` just propagates: `handoffFuture.setException(t)` (`:1110`).

`driver.registerHandoff` (`StreamAppenderatorDriver.java:321-407`) registers a per-segment
callback with the `SegmentHandoffNotifier` using `Execs.directExecutor()` (`:355-361`) and drops
the local segment on each notification (`:366`), completing when the countdown hits zero (`:374`).

### 5. Waiting, and task exit (runThread)

```java
Futures.allAsList(publishWaitList).get();                                   // :871
ingestionState = IngestionState.SEGMENT_AVAILABILITY_WAIT;                  // :879
if (tuningConfig.getHandoffConditionTimeout() == 0) {
  handedOffList = Futures.allAsList(handOffWaitList).get();                 // :881  (forever)
} else {
  try { handedOffList = Futures.allAsList(handOffWaitList)
        .get(tuningConfig.getHandoffConditionTimeout(), TimeUnit.MILLISECONDS); } // :885-886
  catch (TimeoutException e) { log.makeAlert("Timeout waiting for handoff")...emit(); } // :888-895
  finally { handoffWaitMs = ...; }                                          // :896-898
}
```

A handoff timeout is explicitly **not** an ingestion failure (`:889-890`): the alert fires, the
loop at `:901-906` logs whatever did hand off, `appenderator.close()` (`:908`), and the method
falls through to `ingestionState = COMPLETED`, report write, and `TaskStatus.success` (`:974-976`).
The offsets are already durable at that point.

`finally` block `:952-972`: `driver.close()` (`:955`) — which `shutdownNow()`s the `-publish`
executor (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:756`) — unregister chat handler, remove monitor,
unannounce, `rejectionPeriodUpdaterExec.shutdown()` (`:963`).

### 6. Overlord observes completion

The Peon's `TaskStatus` reaches the Overlord through the runner's status future;
`TaskQueue.attachCallbacks` (`:761-824`) increments `statusUpdatesInQueue` **on the producing
thread** (`Execs.directExecutor()` at `:822`, deliberately, `:755-759`) and hands `handleStatus`
to `TaskQueue-OnComplete-%d` (`:772`). `handleStatus` bails if `!active` (`:796-799`), else calls
`notifyStatus` (`:801-804`) then emits completion metrics (`:807`).

`notifyStatus` (`:682-748`), on the callback executor and under an `activeTasks` hash-segment
lock: ignores incomplete statuses (`:700-702`) and already-complete entries (`:703-707`); sets
`entry.isComplete = true` (`:710`); reads `taskRunner.getTaskLocation` (`:712`);
**persists status first** (`:716-733`) with the comment "so if we crash while doing the rest of
the shutdown, our successor remembers"; then `taskRunner.shutdown(...)` (`:737`),
`removeTaskLock(task)` (`:744`), `requestManagement()` (`:745`). Failures in either the persist
(`:726-733`) or the runner shutdown (`:739-742`) are logged and **execution continues**.

`TaskQueue-Manager` runs `manage()` (`:358-373`): sleeps `startDelay` (`:361`), calls
`taskRunner.restore()` once (`:364`), then loops `manageQueuedTasks()` + `awaitManagement()`.
`awaitManagement` (`:339-353`) sleeps `MIN_WAIT_TIME_MS = 100` (`:107`, `:342`) then polls
`managementMayBeNecessary` for up to `MANAGEMENT_WAIT_TIMEOUT_NANOS = 60 s` (`:106`), clearing
all pending requests afterwards (`:352`). So the queue reacts within ~100 ms of a
`requestManagement()` and at worst every ~60 s. `TaskQueue-StorageSync` runs `syncFromStorage()`
every `storageSyncRate` (`:255-273`, `:831-877`).

---

## Nuances

- **Offsets are durable before availability.** Confirmed above: `publishFuture` at `:1017`,
  `registerHandoff` inside its success callback at `:1070`, the blocking wait at `:878-901`.
  A stuck Coordinator produces a task that hangs in handoff-wait with offsets already committed.
- **Handoff timeout does not fail the task.** `:888-895` swallows `TimeoutException`; the task
  still returns `TaskStatus.success` (`:976`). Default is 15 min
  (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskTuningConfig.java:44`),
  and `0` means *wait forever* (`:880`), not "don't wait".
- **`sequences.json` beats the supervisor's checkpoint context.** `initializeSequences` only
  consults `CHECKPOINTS_CTX_KEY` when `restoreSequences()` returns false (`:337-342`). A restarted
  Peon that still has its persist dir resumes from disk state, not from what the supervisor thinks.
- **Kafka end offsets are exclusive; Kinesis's are inclusive.** `isEndOffsetExclusive()`
  (`KafkaIndexTaskRunner.java:204-207`) flips: whether `lastReadOffsets` is pre-seeded (`:565`),
  whether new sequences carry `exclusiveStartPartitions` (`:1744-1752`), and the comparison in
  `canHandle` (`SequenceMetadata.java:214-226`). `isMoreToReadAfterReadingRecord` deliberately
  ignores it (`:1349`).
- **`currOffsets` is "next offset to read", not "last read".** `:747` stores
  `getNextStartOffset(seq)`; `lastReadOffsets` stores the raw offset (`:746`).
- **The committer supplier has a side effect.** Merely calling `committerSupplier.get()` clears
  and repopulates `lastPersistedOffsets` (`:580-581`). `SequenceMetadata.getCommitterSupplier`
  then takes a per-partition **max** of `lastPersistedOffsets` and this sequence's `endOffsets`
  (`SequenceMetadata.java:284-295`) — because one publish covers only a subset of segments.
- **`getCommitterSupplier`'s committer asserts `assignments.isEmpty()`** (`SequenceMetadata.java:272-277`),
  which is why `updateAssignments` must be called before publishing (`:859`, `:1212`).
- **Idempotent publish via `useUniquePath=true`** (`StreamAppenderatorDriver.java:286-288`) plus
  the "already published by another replica" reconciliation in `publishInBackground`
  (`BaseAppenderatorDriver.java:662-689`): if `findPublishedSegments` returns exactly our segment
  IDs, the push is treated as a success and *physically disjoint* copies are `killQuietly`'d
  (`:682-689`). If they are **not** ours, our segments are killed and an `ISE` is thrown (`:696-701`).
- **`publishWaitList`/`handOffWaitList` are plain `ArrayList`s** (`:220-221`) mutated from
  runThread (`:1035`, `:1039`) but read/removed from in `checkPublishAndHandoffFailure` also on
  runThread — safe only because both happen on runThread; the callbacks at `:1113` never touch
  the lists.
- **Empty-publish is a real path.** `supportsEmptyPublish()` returns `true`
  (`SequenceMetadata.java:435-438`), and `publishInBackground` asserts total row count is 0 in
  that case (`BaseAppenderatorDriver.java:604-613`); the publisher then either commits offsets only
  (`SequenceMetadata.java:404-409`) or skips the commit if offsets are unchanged (`:392-398`).
- **`intermediateHandoffPeriod` defaults to effectively infinite** —
  `new Period().withDays(Integer.MAX_VALUE)` (`SeekableStreamIndexTaskTuningConfig.java:122-124`).
  Time-based early publish is off unless configured.
- **Units.** `maxRowsPerSegment`/`maxTotalRows` = rows (`:703-707`); `maxBytesInMemory` = bytes,
  default `0` meaning "derive" (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskTuningConfig.java:102`);
  `handoffConditionTimeout`/`pollTimeout`/`batchAllocationWaitTime` = ms;
  `intermediateHandoffPeriod`/`taskDuration`/`storageSyncRate` = Joda `Period`/`Duration`.
- **The giant lock is per-datasource in 34.0.0.** `GlobalTaskLockbox` holds a
  `ConcurrentHashMap<String, DatasourceLockboxResource>` (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/GlobalTaskLockbox.java:65`) and builds a
  `TaskLockbox` per datasource (`:488`), each with its own `giant` `ReentrantLock`
  (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskLockbox.java:101`). References are counted so a lockbox can be dropped when idle
  (`:598-619`). This is a correction to the older "process-wide giant lock" framing.
- **Batched allocation is the default path.** `SegmentAllocateAction.canPerformAsync` returns
  `toolbox.canBatchSegmentAllocation()` (`:191-194`), and `batchSegmentAllocation` defaults `true`
  (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:34`),
  so `perform()`'s 90-attempt inline loop (`:81`, `:219-220`) is the fallback, not the norm.
- **`batchAllocationWaitTime` defaults to `0`** (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:37`), so `isDue()`
  (`SegmentAllocationQueue.java:660-663`) is true immediately: batching happens only from
  concurrency, not from waiting.
- **Task-context `javaOpts` override the worker's.** `ForkingTaskRunner.java:265-288` appends
  task-context `druid.indexer.runner.javaOpts[Array]` *after* the config ones.

---

## Failure modes

| Failure | Where it surfaces (`path:LINE`) | Detection | Blast radius | Recovery / knob |
|---|---|---|---|---|
| Segment allocation returns not-ok | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskRunner.java:718` | `addResult.isOk() == false` | Task fails immediately | Task retried by supervisor; `ISE` is deliberate ("determinism at risk") |
| Async persist fails | `:733-737` → checked at `:639` | `backgroundThreadException` on persist pool | Task fails at next loop iteration | Disk/IO; `maxPendingPersists`, `numPersistThreads` |
| Publish transaction fails (stale `startMetadata`) | `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentTransactionalInsertAction.java:240`; surfaced `:1024-1028`, `:1107-1111` | `SegmentPublishResult` not success → `ISE` | Segments killed, task fails; offsets unchanged | Supervisor recreates task from stored offsets |
| Task locks revoked | `SegmentTransactionalInsertAction.java:248-253` | `onInvalidLocks` → fail result | Publish fails, task fails | Task priority; avoid concurrent overwrite tasks |
| Handoff never completes | `:883-895` | `TimeoutException` + alert | **Task still succeeds**; segment stays unavailable | `handoffConditionTimeout` (0 = forever) |
| Handoff returns null (recoverable) | `:1077-1088` | `warn` only, `handoffFuture.set(null)` | Silent: task succeeds | Coordinator-side investigation |
| Overlord unreachable during a task action | `RemoteTaskActionClient.java:84-95`; retries in `ServiceClientImpl.java:219-244` | `IOException`/`ChannelException`/5xx | Blocks that action, up to ~12 attempts | `druid.peon.taskActionClient.retry.*` |
| Checkpoint request rejected | `:808-810` | action returns `false` | Task dies ("dying") | Supervisor `checkpointSequences` mismatch |
| Checkpoint not found on supervisor | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:774` | `ISE` in notice handler | Notice alert; task group churn | Supervisor restart / task kill |
| `setEndOffsets` throws | `:1825-1833` | `backgroundThreadException` + `resume()` | Task fails fast, supervisor kills it | — |
| `resume()` not accepted in 5 s | `:1962-1966` | `RuntimeException` | Aborts the caller (Jetty thread) | — |
| Peon JVM dies / non-zero exit | `ForkingTaskRunner.java:411-427` | `process.waitFor()` exit code | Synthetic `TaskStatus.failure` | `status.json` unread; check MM logs |
| MiddleManager closes Peon stdout | `ForkingTaskRunner.java:681-694`; `ExecutorLifecycle.java:151-161` | stdin EOF → `System.exit(2)` | Peon dies without status | `gracefulShutdownTimeout` |
| Overlord crashes between status persist and runner shutdown | `TaskQueue.java:714-737` | none at the time | Orphan process on worker | Next leader's `startPendingTasksOnRunner` shuts down unknown tasks (`:404-405`) |
| Allocation queue full | `SegmentAllocationQueue.java:262-268` | `processingQueue.offer` false | All pending requests in the batch fail | `MAX_QUEUE_SIZE=2000` (`:74`); watch `task/action/batch/runTime` |
| Allocation blocked by partially overlapping segment | `SegmentAllocationQueue.java:508-513`, `:540-546` | request never allocated this round | Requests stall until used segments change | Compaction / `completePendingRequestsWithNull` at `:462` |
| Kafka offset out of range | `KafkaIndexTaskRunner.java:95-104`, `:121-177` | `OffsetOutOfRangeException` | Poll loop spins or supervisor reset | `resetOffsetAutomatically`, `pollRetryMs` |
| Task not ready at Overlord | `TaskQueue.java:437-452` | exception from `isReady` | Immediate `TaskStatus.failure` | `MaxAllowedLocksExceededException` surfaced verbatim |

### Task actions against a failing/failing-over Overlord

`RemoteTaskActionClient.submit` (`:58-97`) POSTs `/druid/indexer/v1/action` through a
`ServiceClient` and does **no retrying itself** — retries live in the client built by
`RemoteTaskActionClientFactory` (`:50-58`):

```java
StandardRetryPolicy.builder()
    .maxAttempts(retryPolicyConfig.getMaxRetryCount() - 1)   // :54  → 12
    .minWaitMillis(retryPolicyConfig.getMinWait()...)        // :55  → 5000
    .maxWaitMillis(retryPolicyConfig.getMaxWait()...)        // :56  → 60000
```

Defaults come from `RetryPolicyConfig` (`minWait PT5S :30`, `maxWait PT1M :33`,
`maxRetryCount 13 :36`) ⇒ **12 attempts**. Backoff is
`max(minWait, min(maxWait, 2^attempt * minWait))`
(`server/src/main/java/org/apache/druid/rpc/ServiceClientImpl.java:490-496`) — 5 s, 10 s, 20 s,
40 s, then 60 s flat; worst case ≈ 11 min per action. Retried: `IOException`, `ChannelException`,
or any cause thereof (`server/src/main/java/org/apache/druid/rpc/StandardRetryPolicy.java:150-155`);
HTTP 502/503/504 **and 500** (`:137-147`, with an explicit comment that 500 shouldn't strictly be
retryable); and "service not available" from the locator when `retryNotAvailable` (default `true`,
`:174`) — that path is `ServiceClientImpl.java:134-148`. Redirects are followed, not retried
(`:193-194`). On exhaustion the failure arrives as `ExecutionException` and
`RemoteTaskActionClient` rewrites `HttpResponseException` into an `IOE` pointing at the Overlord
logs (`:84-93`); `InterruptedException` re-sets the interrupt flag and wraps (`:80-83`).
Net effect during a leader failover: a publish action blocks for minutes and then either succeeds
against the new leader or fails the task — the transaction itself is never partially applied.

On the Overlord, `LocalTaskActionClient.performAction` (`:59-74`) either calls `perform` inline or
`performAsync(...).get(5, TimeUnit.MINUTES)` (`:64`) — the 5-minute cap is the real deadline for a
batched segment allocation. Any `Throwable` becomes a `RuntimeException` (`:71-73`), so allocation
timeouts surface as generic task-action failures.

### Publish failure vs handoff failure

Only `publishFuture` failing propagates into `handoffFuture` (`:1110`), so both lists observe it.
`checkPublishAndHandoffFailure` (`:984-1011`) is what turns a *completed* failed future into a
thrown exception inside the read loop — it calls `get()` on every `isDone()` future. Because it
also removes them (`:997`, `:1010`), a failure that lands *after* the last invocation is only seen
by `Futures.allAsList(publishWaitList).get()` at `:871`. Either way the throw is caught by (1)
`:814-819`, then (2) `:910-936` or (3) `:937-951`, both of which
`Futures.allAsList(...).cancel(true)` on both lists and call `appenderator.closeNow()`
(`:915-918`, `:941-944`). Path (2) is special: if `stopRequested` is set, the interrupt is
**swallowed and the task returns success** (`:931-935`); otherwise the interrupt flag is restored
and rethrown.

`BaseAppenderatorDriver.publishInBackground` wraps the publish in
`RetryUtils.retry(..., predicate, 1)` (`:623`, `:718-724`) — **one** attempt, with an explicit
comment that "the TransactionalSegmentPublisher itself performs required retries" (`:722`). The
predicate only matches the "new start metadata is ahead of last commited end state" message
(`:719-721`), and that specific case also triggers `killQuietly` of the pushed segments
(`:727-730`). Note the two-level killing policy: on an *unknown* outcome nothing is deleted
(`:707-717`, "Must not remove segments here, we aren't sure if our transaction succeeded").

### Persist failure

`driver.persistAsync` failures never throw on runThread; they land in the callback at `:733-737`
which sets `backgroundThreadException`. The loop reads it at `:639` and after the final publish at
`:866-868`. So a persist failure can be delayed by up to one `pollTimeout` + one record batch,
and if it happens during the terminal publish sweep it is checked only at `:866`.

### Graceful shutdown

`SeekableStreamIndexTask.stopGracefully(taskConfig)`
(`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTask.java:169-176`)
branches on `restoreTasksOnRestart`: true ⇒ `runner.stopGracefully()`, false ⇒
`runner.stopForcefully()` (which sets `stopRequested` and interrupts, `:1490-1498`).
`stopGracefully` (`:1500-1544`) sets `stopRequested`, and **if `status == PUBLISHING` it
interrupts immediately and returns** (`:1505-1510`) — publishing does not honour the flag.
Otherwise it tries `pauseLock.tryLock(15 s)` (`LOCK_ACQUIRE_TIMEOUT_SECONDS`,
`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTask.java:64`) to clear `pauseRequested` and signal `shouldResume`
(`:1513-1522`), then the same for `pollRetryLock`/`isAwaitingRetry` (`:1529-1539`); either timeout
falls back to `runThread.interrupt()` (`:1524-1526`, `:1537-1538`).
Because `publishOnStop` is false in this path, the `statusLock` block throws
`InterruptedException("Stopping without publishing")` (`:837`) and catch (2) returns *success*
without publishing (`:931-935`) — the persisted `sequences.json` + appenderator state is what
makes the next attempt resumable.

`SingleTaskBackgroundRunner.stop()` (`:167-240`) calls `stopGracefully` (`:188`),
`waitForCleanupToFinish()` (`:189`), and then waits on the task future for
`gracefulShutdownTimeout` (default `PT5M`,
`indexing-service/src/main/java/org/apache/druid/indexing/common/config/TaskConfig.java:70`)
only when `restoreTasksOnRestart && canRestore()` (`:191-196`); otherwise listeners get a
synthetic failure "Canceled as task execution process stopped" (`:234-238`).
Note `SingleTaskBackgroundRunner.restore()` returns an empty list (`:106-109`) — Peon-side restore
is driven by `BaseRestorableTaskRunner.restore()` on the worker
(`indexing-service/src/main/java/org/apache/druid/indexing/overlord/BaseRestorableTaskRunner.java:86-138`),
which reads `restore.json` (`:57`, `:88-100`) and re-`run(task)`s each task whose directory still
exists and which `canRestore()` (`:121-123`), deleting the directory otherwise (`:125-128`).

### Offset reset propagation

Task side: `sendResetRequestAndWait` (`:1420-1460`) submits `ResetDataSourceMetadataAction`
(`:1433-1443`); on success it emits the alert "Offsets were reset automatically, potential data
duplication or loss" and calls `requestPause()` (`:1446-1452`) — the task parks in
`possiblyPause()` until the supervisor re-drives it; on failure it only alerts (`:1454-1458`)
and keeps going. Kafka reaches this only from `possiblyResetOffsetsOrWait`
(`KafkaIndexTaskRunner.java:121-177`) when `resetOffsetAutomatically` is on **and** the earliest
available offset is beyond what we want (`:131`, `:152-155`); otherwise it waits `pollRetryMs`
on `pollRetryLock`, wakeable by pause/stop (`:165-175`).
Overlord side: `ResetDataSourceMetadataAction.perform` → `SupervisorManager.resetSupervisor`
(`indexing-service/src/main/java/org/apache/druid/indexing/common/actions/ResetDataSourceMetadataAction.java:76-79`).

---

## Optimizations in the current structure

- **Incremental publish while still reading.** `maybePersistAndPublishSequences` (`:1208-1232`)
  publishes closed sequences mid-loop, so one long task emits many segments.
  *Cost:* the read loop does a synchronous `driver.persist` (`:1218`) inline, and each publish adds
  a future the loop must poll (`:643`).
- **Per-batch persist suppression.** `driver.add(..., false)` (`:697`) forbids incremental persist
  until the whole polled batch is indexed, then does one `persistAsync` (`:722`).
  *Cost:* a large poll batch can overshoot `maxRowsInMemory` before any persist starts.
- **`persistAsync` off the read thread** (`:722-740`) keeps polling going during disk writes.
  *Cost:* failures are only observed one iteration later (`:639`), and the persist pool is
  *blocking* with `maxPendingPersists` capacity (`StreamAppenderator.java:1237-1240`) — once full,
  `add()` back-pressures the read loop instead of failing.
- **Single-threaded blocking push/merge executor** (`StreamAppenderator.java:1246-1248`, comment at
  `:1245`: "throttle the input source when write to disk is slow"). *Cost:* segment merges for one
  task never parallelise.
- **Batched segment allocation.** `SegmentAllocationQueue` coalesces requests by
  `AllocateRequestKey` and allocates up to `MAX_BATCH_SIZE = 500` under one `giant`-lock
  acquisition (`:75`, `:578-585`), and `retrieveUsedSegments` is done **once per batch** (`:439`).
  *Cost:* a batch is skipped entirely if another batch for the same datasource is running
  (`:310-313`), and `batchAllocationWaitTime = 0` (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:37`) means most batches are
  size 1 anyway.
- **`batchAllocationReduceMetadataIO = true`** (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:40`) passes a flag into
  `getSegmentTimelineForAllocation` for TIME_CHUNK locking only (`SegmentAllocationQueue.java:472-476`).
  *Cost:* less metadata read, so allocation decisions use a narrower view.
- **Per-datasource lockboxes with reference counting** (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/GlobalTaskLockbox.java:65`, `:477-504`,
  `:598-619`) so unrelated datasources no longer serialise. *Cost:* lock/unlock now goes through a
  `ConcurrentHashMap.compute` on every operation (`:482`).
- **`useUniquePath=true` on push** (`StreamAppenderatorDriver.java:286-288`) makes replica pushes
  non-colliding. *Cost:* orphaned deep-storage objects that must be reclaimed by `killQuietly`
  (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/BaseAppenderatorDriver.java:688`).
- **Status persisted before runner shutdown** (`TaskQueue.java:714-723`) so a crashing Overlord
  never re-runs a completed task. *Cost:* the window at `:723-737` leaves an orphan process.
- **Direct-executor callbacks throughout** (`:739`, `:1033`, `:1091`, `:1113`,
  `indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskQueue.java:822`) avoid a thread hop. *Cost:* `sequences.remove`, `persistSequences()` and
  the metric emit all run on the `-publish` thread, so a slow `sequences.json` write stalls the
  publish executor.
- **`CopyOnWriteArrayList` for `sequences`** (`:246`, `:270`) makes the per-record
  `sequences.stream().filter(canHandle)` (`:674-678`) lock-free. *Cost:* an O(n) copy on every
  add/remove, and a linear scan per record.
- **Notice de-duplication** in the supervisor's `NoticesQueue` (`:887`, comment at `:422`) collapses
  redundant `RunNotice`s. *Cost:* observability — one handled notice may represent many ticks.
- **`TaskQueue` request-coalescing queue** — an `ArrayBlockingQueue<Object>(8)` used purely as a
  signal, cleared after each wake (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskQueue.java:137`, `:320-331`, `:352`). *Cost:* the 100 ms
  floor at `:342` bounds reaction time.

---

## Concurrency & thread model

**Peon**

| Executor / thread | Created at | Runs |
|---|---|---|
| `task-runner-%d-priority-N` | `SingleTaskBackgroundRunner.java:148-156` | all of `runInternal`; = `runThread` (`:429`) |
| Jetty request threads | container | `/pause`, `/resume`, `/offsets/end`, `/stop`, `/status`, `/checkpoints` (`:1546-1942`) |
| `parent-monitor-%d` | `indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java:66` | stdin EOF watch → `System.exit(2)` (`:161`) |
| `RejectionPeriodUpdater-Exec--%d` | `:276`, scheduled `:283-287` | `refreshMinMaxMessageTime` (`:2124-2134`) |
| `[id]-appenderator-persist` | `StreamAppenderator.java:1236-1241` | persists; `numPersistThreads`, bounded by `maxPendingPersists` |
| `[id]-appenderator-merge` | `StreamAppenderator.java:1246-1248` | segment merge/push, blocking single thread |
| `[id]-appenderator-abandon` | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1254` | segment drop after handoff |
| `[id]-publish` | `BaseAppenderatorDriver.java:280-282` | `publishInBackground` + all publish callbacks |

Locks and guarded state in the runner:
- `pauseLock` + `hasPaused`/`shouldResume` (`:189-191`) — the pause state machine; documented at
  `:174-188`. Guards `pauseRequested` transitions and is held across `setEndOffsets`' whole body
  (`:1736`-`:1835`).
- `statusLock` (`:203`) — orders `stopGracefully()`'s flag-set against the `READING → PUBLISHING`
  transition (`:835-841`, `:1505-1510`).
- `pollRetryLock` + `isAwaitingRetry` (`:205-206`) — interruptible poll backoff
  (`KafkaIndexTaskRunner.java:166-175`), signalled from both `pause()` (`:1910-1916`) and
  `stopGracefully()` (`:1529-1535`).
- `SequenceMetadata.lock` (`indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SequenceMetadata.java:68`) — `endOffsets`/`checkpointed`, because
  `setEndOffsets` is called from both the Jetty thread (`:1792`) and runThread (`:854`).
- `synchronized persistSequences()` (`:1138`) — called from the Jetty thread (`:1823`) and from
  publish callbacks on the `-publish` thread (`:1061`). This is the real cross-thread
  contention point on `sequences.json`.
- `volatile` singles: `status`, `startTime`, `driver`, `appenderator`, `pauseRequested`,
  `nextCheckpointTime`, `backgroundThreadException`, `sequences` (`:235-247`).
- Concurrent collections: `currOffsets`, `lastPersistedOffsets` (`ConcurrentHashMap`, `:171-172`);
  `publishingSequences`/`publishedSequences` (`Sets.newConcurrentHashSet`, `:218-219`).
  `lastReadOffsets` is a plain `HashMap` (`:167`) — runThread only (inferred from its only
  writers, `:572` and `:746`).

**Overlord**

| Executor | Created at | Runs | Contention |
|---|---|---|---|
| `TaskQueue-Manager` (1) | `TaskQueue.java:138-142` | `manage()` loop | `startStopLock` read lock (`:378`) |
| `TaskQueue-StorageSync` (1) | `:143-147` | `syncFromStorage()` every `storageSyncRate` | same read lock (`:833`) |
| `TaskQueue-OnComplete-%d` (5) | `:190-193` | `handleStatus`/`notifyStatus`: DB write + remote shutdown | per-task-id `activeTasks` compute lock |
| `SegmentAllocQueue-Manager-%s` (1) | `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentAllocationQueue.java:128` | `processBatchesDue` | `isProcessingScheduled` CAS (`:197`) |
| `SegmentAllocQueue-Worker-%s` (5) | `:129-132` | `runBatchOnWorker` → `taskLockbox.allocateSegments` | per-datasource `giant` lock; `runningDatasources` set (`:98`, `:310-317`) |
| `forking-task-runner-%d` (capacity) | `ForkingTaskRunner.java:157-159` | one thread blocked per running Peon | `synchronized (tasks)` (`:165`, `:215`, `:450`, `:588`) |
| supervisor notice thread (1) | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:938`, loop `:1268-1304` | every `Notice.handle()` | serialises all supervisor state mutation |
| supervisor `-Scheduler-%d` (1) | `:939`, `:1306-1311` | periodic `RunNotice`, min period 1000 ms (`:164`) | — |
| supervisor `-Reporting-%d` (1) | `:940`, `:4457-4480` | lag/metric emission | — |
| supervisor `-Worker-%d` | `:969-975`, sized by `calculateWorkerThreads` (`:1021-1035`) | task HTTP calls, `checkpointTaskGroup` (`:3541-3545`) | keep-alive 2000 ms (`:168`) |

`startStopLock` is a **fair** `ReentrantReadWriteLock` (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskQueue.java:134`): `start`/`stop` take
the write lock, everything else the read lock — so a leadership change quiesces the whole queue.

---

## Config knobs that change execution

| Property | Class field (`path:LINE`) | Default | What it changes |
|---|---|---|---|
| `tuningConfig.handoffConditionTimeout` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskTuningConfig.java:44`, `:58` | 900000 ms | `:880` picks unbounded vs timed `handOffWaitList` wait; timeout only alerts (`:891`) |
| `tuningConfig.intermediateHandoffPeriod` | `SeekableStreamIndexTaskTuningConfig.java:122-124` | `Integer.MAX_VALUE` days | `resetNextCheckpointTime` (`:1846`) → time-based `sequenceToCheckpoint` at `:770-776` |
| `partitionsSpec.maxRowsPerSegment` | used at `:704` | 5,000,000 (DynamicPartitionsSpec) `(unverified)` | `isPushRequired` → early checkpoint (`:708`) |
| `partitionsSpec.maxTotalRows` | `:705-706` | `DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS` | same |
| `tuningConfig.maxRowsInMemory` | `:47`, `:98` | `DEFAULT_MAX_ROWS_IN_MEMORY_REALTIME` | `addResult.isPersistRequired()` → `persistAsync` (`:712-741`) |
| `tuningConfig.maxBytesInMemory` | `:48`, `:102` | `0` (derived) | same |
| `tuningConfig.maxPendingPersists` | `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:1232` | — | persist executor queue depth; when full, `add()` blocks the read loop |
| `tuningConfig.numPersistThreads` | `StreamAppenderator.java:1235-1239` | — | persist parallelism |
| `tuningConfig.resetOffsetAutomatically` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SeekableStreamIndexTaskTuningConfig.java:39` | `false` | gates the whole reset path (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskRunner.java:131`) |
| `tuningConfig.maxParseExceptions` | `:66`, `:130-135` | `TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS` | `ParseExceptionHandler` built at `:405-410`; forced to `0` if `reportParseExceptions` |
| `ioConfig.pollTimeout` | `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskIOConfig.java:88`; `extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/supervisor/KafkaSupervisorIOConfig.java:47` | 100 ms | `recordSupplier.poll(...)` (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTaskRunner.java:93`) ⇒ loop iteration floor |
| `ioConfig.useTransaction` | consumed `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/SequenceMetadata.java:411`, `:425` | `true` | non-transactional branch commits segments with **null** metadata (`:428`) — breaks exactly-once |
| `ioConfig.taskDuration` | `indexing-service/src/main/java/org/apache/druid/indexing/seekablestream/supervisor/SeekableStreamSupervisor.java:3318` | — | task rollover via `checkpointTaskGroup(group, true)` (`:3329`) |
| `ioConfig.completionTimeout` | `:3343` | — | deadline for pending-completion groups |
| `ioConfig.maxAllowedStops` | `:3323` | — | caps concurrent rollovers |
| `ioConfig.period` | `:1271`, `:1309` | — | supervisor tick; floored at `MAX_RUN_FREQUENCY_MILLIS = 1000` (`:164`) |
| `tuningConfig.workerThreads` | `:1021-1035` | `max(2, taskCount/4)` | supervisor `workerExec` size |
| `druid.indexer.queue.maxSize` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskQueueConfig.java:61` | `Integer.MAX_VALUE` | `add()` rejects with CAPACITY_EXCEEDED (`TaskQueue.java:525-532`) |
| `druid.indexer.queue.startDelay` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskQueueConfig.java:63` | PT1M | `manage()` initial sleep (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskQueue.java:361`) |
| `druid.indexer.queue.restartDelay` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskQueueConfig.java:64` | PT30S | sleep after a `manage()` crash (`TaskQueue.java:242-246`) |
| `druid.indexer.queue.storageSyncRate` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskQueueConfig.java:65` | PT1M | `syncFromStorage` period (`TaskQueue.java:255-257`) |
| `druid.indexer.queue.taskCompleteHandlerNumThreads` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskQueueConfig.java:62` | 5 | `TaskQueue-OnComplete-%d` size (`:190-193`) |
| `druid.indexer.tasklock.batchSegmentAllocation` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:34` | `true` | `canPerformAsync` (`SegmentAllocateAction.java:191-194`) vs inline 90-attempt loop |
| `druid.indexer.tasklock.batchAllocationWaitTime` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:37` | 0 ms | `isDue()` (`SegmentAllocationQueue.java:660-663`) and reschedule delay (`:337`) |
| `druid.indexer.tasklock.batchAllocationNumThreads` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:43` | 5 | worker pool + the `runningDatasources` cap at `:306`, `:329` |
| `druid.indexer.tasklock.batchAllocationReduceMetadataIO` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:40` | `true` | `retrieveUsedSegments` narrowing (`SegmentAllocationQueue.java:472-476`) |
| `druid.indexer.tasklock.forceTimeChunkLock` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/TaskLockConfig.java:31` | `true` | stamped into task context at `indexing-service/src/main/java/org/apache/druid/indexing/overlord/TaskQueue.java:509` |
| `druid.peon.taskActionClient.retry.maxRetryCount` | `indexing-service/src/main/java/org/apache/druid/indexing/common/RetryPolicyConfig.java:36` | 13 → 12 attempts | `indexing-service/src/main/java/org/apache/druid/indexing/common/actions/RemoteTaskActionClientFactory.java:54` |
| `druid.peon.taskActionClient.retry.minWait` / `maxWait` | `indexing-service/src/main/java/org/apache/druid/indexing/common/RetryPolicyConfig.java:30`, `:33` | PT5S / PT1M | exponential backoff bounds (`ServiceClientImpl.java:490-496`) |
| `druid.indexer.task.restoreTasksOnRestart` | `indexing-service/src/main/java/org/apache/druid/indexing/common/config/TaskConfig.java:90`, `:143` | `false` | graceful vs forceful stop (`SeekableStreamIndexTask.java:169-176`); restore in `indexing-service/src/main/java/org/apache/druid/indexing/overlord/BaseRestorableTaskRunner.java:121` |
| `druid.indexer.task.gracefulShutdownTimeout` | `indexing-service/src/main/java/org/apache/druid/indexing/common/config/TaskConfig.java:70` | PT5M | `SingleTaskBackgroundRunner.java:193-196`; `ForkingTaskRunner.java:545-551` |
| `druid.indexer.task.directoryLockTimeout` | `indexing-service/src/main/java/org/apache/druid/indexing/common/config/TaskConfig.java:69` | PT10M | Peon task-dir `FileLock` wait (`ExecutorLifecycle.java:124-133`) |
| `druid.indexer.runner.javaOpts[Array]` (+ task context) | `ForkingTaskRunner.java:261-288` | — | Peon JVM flags; task context appended last |
| `druid.worker.capacity` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java:158` | — | concurrent Peons per worker |

---

## Sharp edges

1. **A handoff that never happens is invisible in task status.** `:888-895` alerts and moves on;
   `:1077-1088` downgrades a null handoff to a `warn`. The task reports success (`:976`). Monitor
   the "Timeout waiting for handoff" alert and `handoffWaitMs` in the completion report (`:1171`),
   not task state.
2. **`sequences.json` silently overrides the supervisor.** `initializeSequences` skips
   `getCheckPointsFromContext` entirely when the file exists (`:337`). Manually clearing a Peon's
   persist dir and restarting changes which offsets it reads. Conversely a stale persist dir from
   a previous attempt in the same `taskDir` will be picked up (`ForkingTaskRunner.java:389-391`
   reuses `task.json`, and the persist dir lives under the same `taskWorkDir`).
3. **`stopGracefully` returns success without publishing.** The chain
   `:837` → `:931-935` means a shutdown mid-read yields `TaskStatus.success` with nothing
   published. That is intentional (state is restorable) but it makes "task succeeded" a weak
   signal about data.
4. **`useTransaction: false` quietly disables exactly-once.** `SequenceMetadata.java:425-428`
   passes `null` start/end metadata, so offsets are never written — and in
   `SegmentTransactionalAppendAction.perform` the branch keys off `startMetadata == null`
   (`:183` vs `:190`), not off any "am I supposed to be transactional" flag.
5. **`checkPublishAndHandoffFailure` removes futures from the wait lists** (`:997`, `:1010`).
   If you add code that inspects `publishWaitList`/`handOffWaitList` expecting a complete history,
   it will be wrong.
6. **Calling `committerSupplier.get()` mutates `lastPersistedOffsets`** (`:580-581`). It looks like
   a pure getter. Two calls in a row are not equivalent to one.
7. **`possiblyPause()` can block forever.** `while (pauseRequested) shouldResume.await()` (`:1391-1393`)
   has no timeout — a supervisor that pauses a task and then dies leaves it parked, and the only
   ways out are `/resume`, `stopGracefully` clearing the flag (`:1515-1518`), or an interrupt.
8. **`pause()` can return HTTP 202 without having paused** (`:1922-1928`) after 2 s, while
   `resume()` *throws* after 5 s (`:1962-1966`). Callers must handle the asymmetry;
   `checkpointTaskGroup` treats an error/empty result as "kill the task"
   (`SeekableStreamSupervisor.java:3461-3476`).
9. **`Preconditions.checkArgument` at `:787-794`** makes "checkpoint a non-latest sequence" a task
   failure rather than a no-op. Any change to sequence ordering (e.g. concurrent `setEndOffsets`)
   trips it.
10. **The `-publish` executor runs application logic.** `sequences.remove`, the `synchronized`
    `persistSequences()` (`:1061`) and metric emission (`:1099`) all execute there via
    `directExecutor` (`:1113`). A slow disk on `sequences.json` serialises all publishes for the task.
11. **Non-zero Peon exit discards `status.json`.** `ForkingTaskRunner.java:413-427` only reads the
    status file on exit code 0; a JVM killed by the OOM killer or by `System.exit(2)` from the
    parent monitor always looks like a generic failure.
12. **Orphan-process window in `notifyStatus`.** Status is persisted at `:723` and the runner is
    told to shut down at `:737`; an Overlord crash in between leaves the Peon running. The next
    leader cleans it up only on the next `startPendingTasksOnRunner` pass, via
    `shutdownUnknownTaskOnRunner` (`TaskQueue.java:404-405`, `:474-482`).
13. **`batchAllocationWaitTime = 0` defeats batching.** With `isDue()` immediately true
    (`SegmentAllocationQueue.java:660-663`) most batches contain one request, so you pay the queue's
    overhead without the amortised `retrieveUsedSegments`. Raise it deliberately if
    `task/action/batch/size` is ~1 and `runTime` is high.
14. **Allocation can complete with `null` instead of failing.** After `MAX_ATTEMPTS`
    (`indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentAllocateAction.java:81` = 90) or when used segments are unchanged,
    `completePendingRequestsWithNull()` (`indexing-service/src/main/java/org/apache/druid/indexing/common/actions/SegmentAllocationQueue.java:462`, `:761`) resolves the
    future to `null` — which the task turns into the hard `ISE` at `:718`.
15. **`SequenceMetadata.getEndOffsets()` returns the live mutable map** under a lock that is
    released before the caller uses it (`SequenceMetadata.java:139-148`). Callers such as `:1798`
    read it concurrently with `setEndOffsets` from another thread.
16. **Sequence names are `<baseSequenceName>_<id>` and `baseSequenceName` is supervisor-generated**
    from offsets + schema + tuning config (`:354`, `:1806`). Changing tuning config mid-flight
    changes generated names and confuses checkpoint matching across supervisor restarts.
17. **Confluent fork delta in the hot path.** `parser.parse(..., record.isFiltered())` at `:671`
    plus `StreamChunkParser.java:114-121` and `OrderedPartitionableRecord.isFiltered()` — filtered
    records advance offsets and count as `filtered`, not `thrownAway`. Row-count reconciliation
    against upstream Druid dashboards will differ.

---

## Open questions

- `DynamicPartitionsSpec` default `maxRowsPerSegment` is quoted as 5,000,000 `(unverified)` — the
  constant lives in `indexing-hadoop`/`processing` and was not read here; `maxTotalRows` was read
  only as the symbolic `DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS` (`:706`).
- `lastReadOffsets` being runThread-confined is `(inferred)` from its only two write sites
  (`:572`, `:746`); it is a plain `HashMap` (`:167`) with no documented guard.
- `maxPendingPersists` / `numPersistThreads` defaults were not read (fields are consumed at
  `StreamAppenderator.java:1232-1239`) — `(unverified)`.
- Whether `HttpRemoteTaskRunner` (`overlord/hrtr`) detects Peon death differently from
  `ForkingTaskRunner`'s exit-code path is `(unverified)`; only `ForkingTaskRunner` was read.
