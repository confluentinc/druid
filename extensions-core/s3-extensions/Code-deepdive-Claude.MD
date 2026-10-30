# Code deep dive — druid-s3-extensions

Execution-mechanics companion to `extensions-core/s3-extensions/CLAUDE.md` (read that first for the class
map). This doc traces actual method bodies for the `→ S3 →` hop: an `index_kafka` task's
`StreamAppenderator` pushing a built segment through `S3DataSegmentPusher`, and a Historical later pulling
it back via `S3LoadSpec`/`S3DataSegmentPuller`. It also covers the shared retry/upload/client plumbing that
every other code path in this module (`S3DataSegmentKiller`, `S3InputSource`, `S3TaskLogs`,
`output/S3StorageConnector`) reuses. Class responsibilities are not repeated here except where needed to
explain a mechanism; see `CLAUDE.md` for the inventory.

## Happy path, traced

### (A) Task pushes a built segment

Runs entirely on the Appenderator's single-threaded `pushExecutor`
(`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:191`); the
transform that calls `mergeAndPush` is explicitly submitted on that executor
(`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:848`), and
`mergeAndPush`'s own doc comment says it "must only be run in the single-threaded pushExecutor"
(`StreamAppenderator.java:865-866`).

1. `StreamAppenderator#mergeAndPush` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:874`) merges per-hydrant `QueryableIndex`es
   into one on-disk segment via `IndexMergerV9#mergeQueryableIndex`, then at
   `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:977` calls `dataSegmentPusher.push(mergedFile, segmentToPush, useUniquePath)`.
   Local disk write: `mergedTarget` under the task's working directory (not this module).
2. `S3DataSegmentPusher#push` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusher.java:80`)
   computes `storageDirSuffix = getStorageDir(inSegment, useUniquePath)` (default impl:
   `processing/src/main/java/org/apache/druid/segment/loading/DataSegmentPusher.java:113` `getDefaultStorageDir`,
   `dataSource/interval_start_end/version/partitionNum[/uniqueUuid]`) and delegates to `pushToPath`.
3. `S3DataSegmentPusher#pushToPath` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusher.java:87`):
   - `s3Path = S3Utils.constructSegmentPath(config.getBaseKey(), storageDirSuffix)` →
     `<baseKey>/<storageDir>/index.zip` (`S3Utils.java:214-220`).
   - `zipOutFile = File.createTempFile("druid", "index.zip")` (JVM temp dir, **not** a Druid-configured path)
     then `indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile)` (`S3DataSegmentPusher.java:92-93`).
     This is a **streaming** zip: `CompressionUtils.zip(File, File)` opens a `FileChannel`/`OutputStream`
     directly on `zipOutFile` and streams entries into a `ZipOutputStream`
     (`processing/src/main/java/org/apache/druid/utils/CompressionUtils.java:171-192`) — the whole segment
     is never buffered in heap memory, only a local temp file is materialized.
   - Builds `outSegment = inSegment.withSize(indexSize).withLoadSpec(makeLoadSpec(...)).withBinaryVersion(...)`
     **before** the upload happens (`S3DataSegmentPusher.java:95-97`) — the loadSpec that will be persisted
     to metadata is computed from the local zip, not confirmed against what actually landed in S3.
   - Uploads inside `S3Utils.retryS3Operation(() -> { S3Utils.uploadFileIfPossible(...); return outSegment; })`
     (`S3DataSegmentPusher.java:100-106`); `uploadFileIfPossible` optionally sets an ACL granting the bucket
     owner full control, then calls `service.upload(putObjectRequest)` (`S3Utils.java:348-363`).
   - `finally { zipOutFile.delete(); }` (`S3DataSegmentPusher.java:127-130`) — temp file is cleaned up
     whether or not the upload succeeded.
4. `ServerSideEncryptingAmazonS3#upload` (`ServerSideEncryptingAmazonS3.java:189-197`): if a `TransferManager`
   is configured (`S3TransferConfig.isUseTransferManager()`, default `true`), delegates to
   `transferManager.upload(...)` and blocks on `transfer.waitForCompletion()` on the same `pushExecutor`
   thread; otherwise falls back to a plain synchronous `putObject`.
5. Back in `StreamAppenderator#push` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:773`), the returned `DataSegment` (with S3
   `loadSpec`) is collected and eventually handed to `BaseAppenderatorDriver#publishInBackground` →
   `TransactionalSegmentPublisher#publish` → `IndexerSQLMetadataStorageCoordinator#commitSegmentsAndMetadata`
   (all in `server`/`indexing-service`, out of this module's scope; see `server/CLAUDE.md`).

The returned `loadSpec` map is exactly:
```java
ImmutableMap.of("type", "s3_zip", "bucket", bucket, "key", key, "S3Schema", useS3aSchema ? "s3a" : "s3n")
```
(`S3DataSegmentPusher.java:144-156`). Nothing else about the upload (ETag, part count, SSE mode used) is
recorded in the loadSpec or in `DataSegment` — durability/verification relies entirely on S3's PUT semantics.

### (B) Historical pulls it back

Runs on whichever loading executor `SegmentLoadDropHandler` dispatches to
(`server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:68-69`:
`normalLoadExec`/`turboLoadExec`, selected by `SegmentLoadingMode` at `SegmentLoadDropHandler.java:439-441`)
— not a dedicated S3 thread pool.

1. `SegmentLoadDropHandler#addSegment` (`server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:146`) calls
   `segmentManager.loadSegment(segment)` (`:165`), which (via `SegmentLocalCacheManager`, in `server`)
   deserializes `segment.getLoadSpec()` into an `S3LoadSpec` (Jackson subtype registered at
   `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageDruidModule.java:82`).
2. `S3LoadSpec#loadSegment` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3LoadSpec.java:58-62`)
   delegates straight to `puller.getSegmentFiles(new CloudObjectLocation(bucket, key), outDir)`.
3. `S3DataSegmentPuller#getSegmentFiles` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPuller.java:74`):
   - `isObjectInBucket(s3Coords)` (`:283-296`) does a **HEAD-equivalent existence check** wrapped in
     `S3Utils.retryS3Operation` before touching disk; throws `SegmentLoadingException` immediately if absent
     (`:80-82`) — no local directory is created yet.
   - `FileUtils.mkdirp(outDir)` (`:85`) creates the local cache directory.
   - Builds a lazy `ByteSource` whose `openStream()` calls `buildFileObject(uri).openInputStream()`
     (`:88-105`), which lazily promotes to a full S3 GET (`S3DataSegmentPuller.java:183-186`,
     `s3Client.getObject(bucket, key)`) only when the stream is actually opened.
   - `CompressionUtils.unzip(byteSource, outDir, S3Utils.S3RETRY, false)` (`:106-114`) streams the GET
     response directly into `unzip`, which can **re-open the ByteSource and retry the whole download** if a
     retryable exception occurs mid-stream (the `S3RETRY` predicate is passed straight into
     `CompressionUtils.unzip`, so unzip-level retries reuse the exact same predicate as `S3Utils.retryS3Operation`).
   - On any exception, the `catch` block (`:126-139`) calls `FileUtils.deleteDirectory(outDir)` to clean up a
     partially-written cache dir, then re-throws as `SegmentLoadingException`.
4. Back in `SegmentLoadDropHandler#addSegment`, once `loadSegment` returns, `announcer.announceSegment(segment)`
   (`:173`) makes the segment visible to the Coordinator/Broker's server view (see `server/CLAUDE.md`).

## Nuances

- **Key layout**: `<baseKey>/<dataSource>/<intervalStart_intervalEnd>/<version>/<partitionNum>[/<uuid>]/index.zip`.
  `baseKey` comes from `S3DataSegmentPusherConfig.baseKey` (default `""`, so a key can start directly with the
  datasource); `storageDir` comes from `DataSegmentPusher.getDefaultStorageDir`
  (`processing/src/main/java/org/apache/druid/segment/loading/DataSegmentPusher.java:113-129`); the two are
  joined with `/` and `index.zip` appended by `S3Utils.constructSegmentPath` (`S3Utils.java:214-220`), which
  `skipNulls()`-joins so an empty `baseKey` doesn't produce a leading `//`.
- **`useUniquePath`**: when `true`, `getDefaultStorageDir` appends a random UUID segment
  (`processing/src/main/java/org/apache/druid/segment/loading/DataSegmentPusher.java:127`, `generateUniquePath()` at `:142-145`) so that concurrent/retried task
  attempts never collide on the same S3 key — the interface javadoc
  (`DataSegmentPusher.java:51-65`) spells out the exact replica/retry race this guards against for Kafka
  ingestion. The tradeoff: if a task pushes with a unique path and then fails before publishing to metadata,
  that S3 object is orphaned — nothing in this module cleans it up automatically (killers only act on
  segments recorded in metadata).
- **No `descriptor.json` written on push**: `S3DataSegmentPusher` only uploads `index.zip`; the loadSpec is
  carried in Druid's metadata store, not a companion file. `S3DataSegmentKiller` still best-effort deletes a
  `descriptor.json`-shaped key (`DataSegmentKiller.descriptorPath(path)`,
  `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentKiller.java:208`/`:101`) purely for legacy segments pushed by older Druid versions.
  (documented in `CLAUDE.md`'s Gotchas — repeated here because it directly affects the kill code path below.)
- **Overwrite semantics**: `putObject`/multipart upload to an existing key **silently overwrites** it (S3 has
  no put-if-absent in the SDK calls used here); the only protection against clobbering a previously-published
  segment is `useUniquePath`, not any conditional-write logic in `S3Utils.uploadFileIfPossible`
  (`S3Utils.java:348-363`).
- **Path-style vs virtual-host addressing**: controlled by `AWSClientConfig.isEnablePathStyleAccess()`, wired
  in at `S3StorageDruidModule#getServerSideEncryptingAmazonS3Builder`
  (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageDruidModule.java:143`,
  `.withPathStyleAccessEnabled(...)`) — needed for S3-compatible endpoints (e.g. MinIO) that don't support
  virtual-hosted-style bucket URLs.
- **`s3a`/`s3n` schema is informational only**: `S3DataSegmentPusherConfig.useS3aSchema` (default `false`,
  i.e. `s3n` for backward compatibility, `S3DataSegmentPusherConfig.java:43-44`) only changes
  `getPathForHadoop()` (`S3DataSegmentPusher.java:57-64`) and the `S3Schema` string stamped into the loadSpec
  (`S3DataSegmentPusher.java:153-154`) for Hadoop-based batch jobs to consume; the actual push/pull in this
  module always talks to S3 through the AWS SDK `AmazonS3` client, never through a Hadoop `s3a://`/`s3n://`
  filesystem.
- **Units**: `S3TransferConfig.minimumUploadPartSize`/`multipartUploadThreshold` default to `20 * 1024 * 1024L`
  bytes = 20 MiB (`S3TransferConfig.java:35,39`); `S3OutputConfig.chunkSize` (durable storage / export, a
  different code path) defaults to `"100MiB"` via `HumanReadableBytes` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/output/S3OutputConfig.java:49`) and is
  clamped to `[5MiB, 5GiB]` (`S3OutputConfig.java:35-36`, validated at `:97-113`) — these two chunking configs
  are independent and apply to different upload mechanisms (TransferManager vs `RetryableS3OutputStream`'s
  manual multipart, see Optimizations below).

## Failure modes

| Failure | Where it surfaces | Detection | Blast radius | Recovery / knob |
|---|---|---|---|---|
| Transient 5xx / throttling (`SlowDown`, `503`, `RequestTimeout`, clock skew) | Any `S3Utils.retryS3Operation`-wrapped call: push (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusher.java:100`), pull existence check (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPuller.java:286`), metadata fetch (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3Utils.java:252`), listing (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/ObjectSummaryIterator.java:126`), delete (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3Utils.java:333`, `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentKiller.java:156`) | `S3Utils.S3RETRY` predicate (`S3Utils.java:73-108`) → `AWSClientUtil.isClientExceptionRecoverable` (`cloud/aws-common/src/main/java/org/apache/druid/common/aws/AWSClientUtil.java:74-110`) | Single operation delayed; task/Historical thread blocked while sleeping | Retried up to `RetryUtils.DEFAULT_MAX_TRIES`=10 (`processing/src/main/java/org/apache/druid/java/util/common/RetryUtils.java:36`) with fuzzy exponential backoff (see below) |
| 403 `AccessDenied` / other 4xx service errors | Same call sites | `S3RETRY` falls through to `apply(e.getCause())`/`isClientExceptionRecoverable` returning `false` for non-listed error codes | Immediate `RuntimeException`/`SegmentLoadingException`/`IOException`, task or load attempt fails outright | Not retried by design (`RetryUtils.java` doc: "Service-level exceptions (access denied, file not found, etc) are not retried" — comment on `S3Utils.retryS3Operation`, `S3Utils.java:110-113`); fix IAM policy |
| Object > 5 GB single PUT (`EntityTooLarge`) | `S3DataSegmentPusher#pushToPath` catch block | `S3Utils.getS3ErrorCode(e).equals(ERROR_ENTITY_TOO_LARGE)` (`S3DataSegmentPusher.java:108-121`) | Task fails with a user-facing `DruidException` | User must lower target segment size; no automatic multipart fallback for this single `putObject`-style call (TransferManager may still choose multipart under the 20 MiB threshold, but a >5GB whole-file `PutObjectRequest` object metadata check can still trip this depending on SDK/method path) `(inferred exact trigger conditions)` |
| Local disk full while zipping (`CompressionUtils.zip`) or unzipping (`CompressionUtils.unzip`) | `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusher.java:93` / `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPuller.java:107` | Underlying `IOException` from the `FileChannel`/`ZipOutputStream` or unzip write | Push/pull attempt fails; on pull, `outDir` is deleted in the `catch` (`S3DataSegmentPuller.java:126-139`); on push, `zipOutFile.delete()` still runs in `finally` (`S3DataSegmentPusher.java:128-130`) | No S3-side knob; operator must ensure task/Historical local disk (JVM temp dir for push, cache `StorageLocation` for pull) has headroom |
| Object missing on pull | `S3DataSegmentPuller#getSegmentFiles` | `isObjectInBucket` returns `false` (`S3DataSegmentPuller.java:80-82`), checked **before** any retry-eligible GET | Immediate `SegmentLoadingException("IndexFile[%s] does not exist.")`, no retries | Indicates a real data-loss/misconfiguration bug (wrong bucket/key, deleted segment still in metadata); not retried since a 404 isn't in `S3RETRY`'s recoverable set |
| Partial/corrupted download (dropped connection mid-GET) | `S3DataSegmentPuller#buildFileObject#openInputStream` | `SdkClientException` with message `"Data read has a different length than the expected"` — explicitly special-cased (`S3Utils.java:86-91`, citing apache/druid#11941) | Unzip fails partway, `outDir` deleted (`S3DataSegmentPuller.java:126-139`) | Retried like any other `S3RETRY`-recoverable error, because `CompressionUtils.unzip` is called with the `S3RETRY` predicate (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPuller.java:110`) so it can re-open the `ByteSource` (fresh GET) and retry |
| Credential expiry mid-transfer | Any SDK call | `SdkClientException`/`AmazonServiceException` from STS/credentials provider; message-matched case for `"Unable to find a region via the region provider chain"` (`S3Utils.java:95-98`) is explicitly retried since it "can happen sometimes when AWS isn't able to obtain the credentials" | Operation fails/retries; if credentials are truly expired (not transient), all `DEFAULT_MAX_TRIES` retries exhaust and it surfaces as a hard failure | No automatic credential refresh logic in this module; relies on the injected `AWSCredentialsProvider` chain (`cloud/aws-common`) refreshing under the hood |
| Multi-object delete partial failure | `S3DataSegmentKiller#kill(List)` / `#deleteKeysForBucket` | `MultiObjectDeleteException` caught per-chunk (`S3DataSegmentKiller.java:164-180`); each `DeleteError`'s code/message logged, batch continues | Only the failed keys in that `DeleteObjectsRequest` are left undeleted; the overall `kill(List)` call throws `SegmentLoadingException` at the end if *any* chunk had an issue (`S3DataSegmentKiller.java:105-121`), even though other chunks succeeded | Caller (killer duty) can retry the whole `kill` — `deleteKeysForBucket` re-checks `doesObjectExist` implicitly via S3's delete-is-idempotent semantics on the next attempt |
| Object already gone on kill | `S3DataSegmentKiller#kill(DataSegment)` | Explicit `s3Client.doesObjectExist(...)` guard before each `deleteObject` call (`S3DataSegmentKiller.java:211,217`) | None — treated as already-successful, no error | N/A, this is the designed idempotent-kill behavior |

**Retry predicate and attempt counts, precisely**: `S3Utils.S3RETRY` (`S3Utils.java:73-108`) recurses through
`IOException` causes (always retryable if wrapped), special-cases two `SdkClientException` message substrings,
clears the interrupt flag and refuses to retry on `InterruptedException` (`:99-101`), and for any
`AmazonClientException` defers to `AWSClientUtil.isClientExceptionRecoverable`
(`cloud/aws-common/src/main/java/org/apache/druid/common/aws/AWSClientUtil.java:74-110`), which retries:
IOException-caused client exceptions, `RequestTimeout` (explicit `AmazonServiceException` error-code check),
any `RetryUtils.isRetryableServiceException` (AWS SDK's own 5xx classification), any
`RetryUtils.isThrottlingException`, any `RetryUtils.isClockSkewError`, and for `MultiObjectDeleteException`,
any per-key error whose code is in the 24-entry `RECOVERABLE_ERROR_CODES` set (`AWSClientUtil.java:41-67`,
includes `"SlowDown"`, `"RequestThrottled"`, `"ServiceUnavailable"`, `"InternalError"`, etc.). Everything else
(e.g. `AccessDenied`, `NoSuchKey`, `InvalidBucketName`) returns `false` and fails fast.

`RetryUtils.retry` (`processing/src/main/java/org/apache/druid/java/util/common/RetryUtils.java:112-149`) is
the engine: default `maxTries = RetryUtils.DEFAULT_MAX_TRIES = 10` (`:36`, used by
`S3Utils.retryS3Operation(Task)` at `S3Utils.java:114-117`; `S3DataSegmentKiller` explicitly overrides this to
`3` for its `deleteObjects` calls, `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentKiller.java:161`). Backoff, per
`RetryUtils.nextRetrySleepMillis` (`RetryUtils.java:226-235`):
```java
sleepMillis = min(60_000, 1000 * 2^(nTry-1)) * fuzzyMultiplier   // fuzzyMultiplier in [0,2], Gaussian(mean 1, sd 0.2)
```
i.e. attempt 1 waits up to ~2s, attempt 2 up to ~4s, ... capping at 60s from attempt 7 onward — so a fully
exhausted 10-try retry loop can sleep for several minutes total before finally propagating the last exception
(`RetryUtils.java:139-142`, `Throwables.propagateIfInstanceOf`).

## Optimizations in the current structure

- **TransferManager-based multipart/parallel upload** (push path): `ServerSideEncryptingAmazonS3` constructs a
  `TransferManager` when `S3TransferConfig.isUseTransferManager()` is `true` (default)
  (`ServerSideEncryptingAmazonS3.java:73-86`), so `upload()` (`:189-197`) automatically splits large `index.zip`
  files into parts above `multipartUploadThreshold` (20 MiB default) and uploads them with parallelism
  internal to the SDK's `TransferManager`, rather than one blocking single-part PUT. Cost: an extra thread
  pool inside `TransferManager` per `ServerSideEncryptingAmazonS3` instance, and `waitForCompletion()` still
  blocks the calling (`pushExecutor`) thread until all parts finish.
- **Streaming zip instead of buffering** (push path): `CompressionUtils.zip(File, File)` streams directly to a
  local temp file via `FileChannel`/`ZipOutputStream` (`processing/.../CompressionUtils.java:148-158`) rather
  than building the zip in memory — bounds heap usage regardless of segment size, at the cost of needing local
  disk headroom for the temp `index.zip` (see Failure modes: disk full).
  ```java
  // S3DataSegmentPusher.java:92-93
  final File zipOutFile = File.createTempFile("druid", "index.zip");
  final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);
  ```
- **Retry-with-backoff instead of failing the task on transient errors**: every S3 call in the push/pull/kill
  paths is wrapped in `S3Utils.retryS3Operation`, converting a large class of transient AWS errors into
  bounded latency rather than task failure — see Failure modes table above. Cost: a stuck retry loop can hold
  the single `pushExecutor` thread (push side) for minutes, delaying all other pending pushes for that task
  since it's single-threaded (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:191`).
- **Batch deletes** (kill path): `S3DataSegmentKiller#kill(List<DataSegment>)` groups segments by bucket and
  issues one `DeleteObjectsRequest` per up-to-1000-key chunk
  (`S3DataSegmentKiller.java:52,142-145`,`MAX_MULTI_OBJECT_DELETE_SIZE`) instead of one HTTP DELETE per
  segment, cutting request count by ~1000x for large drop batches at the cost of coarser error granularity
  (a whole chunk's `MultiObjectDeleteException` must be unpacked per-key to find which ones actually failed,
  `S3DataSegmentKiller.java:164-180`).
- **Single shared, lazily-built S3 client**: `S3StorageDruidModule` binds `ServerSideEncryptingAmazonS3` as a
  `@LazySingleton` (`S3StorageDruidModule.java:164-171`) and separately exposes a memoized `Supplier` for
  killers/movers/archivers (`:177-184`, `Suppliers.memoize`) so that a single AWS SDK client (and its internal
  connection pool / `TransferManager` thread pool) is reused across all pushes/pulls/kills in a process,
  instead of constructing a new client per operation. Cost: config validation (credentials, endpoint) for
  processes where S3 isn't actually the active deep storage is deferred via the `Supplier` indirection
  specifically to avoid paying this cost when unused (`S3DataSegmentKiller.java:56-64` javadoc).
- **Existence check before GET on pull**: `isObjectInBucket` (`S3DataSegmentPuller.java:283-296`) does a
  metadata-only check before opening a full object stream, so a missing/misconfigured segment fails fast with
  a cheap call rather than after a large partial GET.

## Concurrency & thread model

- **Push**: always on the task's single-threaded `pushExecutor`
  (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:191`, an
  executor created per Appenderator instance). `StreamAppenderator#push` submits `mergeAndPush` work onto it
  via `Futures.transform(persistAll(committer), ..., pushExecutor)` (`StreamAppenderator.java:844-849`), and a
  `pushBarrier()` helper (`:857-862`) exists specifically to synchronize with "all pending pushes have
  finished" — implying pushes for a single task are strictly serialized, never concurrent with each other.
  `S3DataSegmentPusher` itself has no internal thread pool of its own; concurrency for a single push comes
  only from the `TransferManager`'s internal part-upload threads (blocked-on via `waitForCompletion()`).
- **Pull**: dispatched by the Historical's `SegmentLoadDropHandler` onto either `normalLoadExec` (a
  `ScheduledExecutorService`) or `turboLoadExec` (a `ThreadPoolExecutor`), selected per `SegmentLoadingMode`
  (`server/src/main/java/org/apache/druid/server/coordination/SegmentLoadDropHandler.java:68-69,439-441`) —
  i.e. **multiple segment loads can run concurrently** on a Historical, each on its own thread from one of
  these pools, each independently calling into `S3DataSegmentPuller#getSegmentFiles` against the shared
  `ServerSideEncryptingAmazonS3` client.
- **Client thread-safety**: `ServerSideEncryptingAmazonS3` wraps AWS's `AmazonS3Client`, which is documented
  by AWS as thread-safe; the module relies on this to share one instance across concurrent pulls (Historical)
  and the (serialized) pushes (task) without any additional locking in this module's code.
- **Kill**: `S3DataSegmentKiller#kill(List)` iterates buckets and chunks sequentially within the calling
  thread (`S3DataSegmentKiller.java:106-113`) — no internal parallelism; the caller (a Coordinator kill duty
  or CLI tool, outside this module) determines whether/how kill calls for different segment batches run
  concurrently.

## Config knobs that change execution

| Property | Class field | Default | What it changes |
|---|---|---|---|
| `druid.storage.bucket` | `S3DataSegmentPusherConfig.bucket` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusherConfig.java:31`) | `""` | Target bucket for `pushToPath`'s `PutObjectRequest`/`makeLoadSpec` |
| `druid.storage.baseKey` | `S3DataSegmentPusherConfig.baseKey` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusherConfig.java:34`) | `""` | Key prefix joined by `S3Utils.constructSegmentPath` (`S3Utils.java:214-220`) |
| `druid.storage.disableAcl` | `S3DataSegmentPusherConfig.disableAcl` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusherConfig.java:37`) | `false` | If `false`, `S3Utils.uploadFileIfPossible` fetches the bucket ACL and grants the bucket owner full control before every upload (`S3Utils.java:358-360`) — an extra `getBucketAcl` call per push |
| `druid.storage.useS3aSchema` | `S3DataSegmentPusherConfig.useS3aSchema` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusherConfig.java:44`) | `false` | Only `getPathForHadoop()`/loadSpec `S3Schema` string (see Nuances) |
| `druid.storage.maxListingLength` | `S3DataSegmentPusherConfig.maxListingLength` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusherConfig.java:41`) / `S3InputDataConfig.maxListingLength` | `1024` | Page size for `ListObjectsV2Request` in `ObjectSummaryIterator`/`killAll` |
| `druid.storage.sse.type` (+`.kms.*`/`.custom.*`) | `S3StorageConfig.serverSideEncryption` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageConfig.java:37`) | `NoopServerSideEncryption` | Decorates every `PutObjectRequest`/`GetObjectRequest`/multipart request via `ServerSideEncryption.decorate(...)` inside `ServerSideEncryptingAmazonS3` (`ServerSideEncryptingAmazonS3.java:121-175`) |
| `druid.storage.transfer.useTransferManager` | `S3TransferConfig.useTransferManager` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3TransferConfig.java:31`) | `true` | Whether `upload()` uses multipart `TransferManager` or a single blocking `putObject` (`ServerSideEncryptingAmazonS3.java:189-197`) |
| `druid.storage.transfer.minimumUploadPartSize`/`multipartUploadThreshold` | `S3TransferConfig.java:35,39` | 20 MiB / 20 MiB | Part-size and size threshold for `TransferManagerBuilder` (`ServerSideEncryptingAmazonS3.java:78-82`) |
| `druid.s3.accessKey`/`secretKey`/etc. | `AWSCredentialsConfig` (`cloud/aws-common`) | — | Feeds `AWSCredentialsProvider` injected into `S3StorageDruidModule#getServerSideEncryptingAmazonS3Builder` (`S3StorageDruidModule.java:128-141`) |
| `druid.s3.endpoint.url`/`.signingRegion` | `AWSEndpointConfig` | — | Non-empty URL triggers `amazonS3ClientBuilder.setEndpointConfiguration(...)` (`S3StorageDruidModule.java:146-150`) instead of default AWS region resolution |
| `druid.s3.proxy.*` | `AWSProxyConfig` | — | Applied via `S3Utils.setProxyConfig` onto the `ClientConfiguration` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageDruidModule.java:141`, `S3Utils.java:398-413`) |
| `druid.s3.enablePathStyleAccess` | `AWSClientConfig.isEnablePathStyleAccess()` | — | `.withPathStyleAccessEnabled(...)` on the client builder (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageDruidModule.java:143`) — needed for non-AWS S3-compatible endpoints |
| `druid.s3.disableChunkedEncoding` | `AWSClientConfig.isDisableChunkedEncoding()` | — | `.withChunkedEncodingDisabled(...)` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageDruidModule.java:142`) |
| output/export `chunkSize` | `S3OutputConfig.chunkSize` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/output/S3OutputConfig.java:49`) | `100MiB`, clamped `[5MiB,5GiB]` | Local chunk size before each part is queued for manual multipart upload in `RetryableS3OutputStream` (durable-storage/export path, separate from segment push) |
| output/export `maxRetry` | `S3OutputConfig.maxRetry` (`extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/output/S3OutputConfig.java:55`) | `RetryUtils.DEFAULT_MAX_TRIES`=10 | Retry budget for `RetryableS3OutputStream`'s per-chunk uploads |
| — (fixed) | `RetryUtils.DEFAULT_MAX_TRIES` (`processing/src/main/java/org/apache/druid/java/util/common/RetryUtils.java:36`) | `10` | Global default max attempts for every `S3Utils.retryS3Operation(Task)` call site that doesn't pass an explicit count |

## Sharp edges

- **IAM permissions actually required, per code path**: push needs `s3:PutObject` (+ `s3:GetBucketAcl` when
  `disableAcl=false`, `S3Utils.java:358-360`) on the target bucket/prefix; pull needs `s3:GetObject` and
  effectively `s3:ListBucket`/head access (`doesObjectExist` under the hood calls `getObjectMetadata`,
  `ServerSideEncryptingAmazonS3.java:93-109`, which needs `s3:GetObject` or `s3:ListBucket` depending on
  bucket policy); kill needs `s3:DeleteObject` and `s3:DeleteObjects` (batch); `killAll`/`S3InputSource`
  prefix listing needs `s3:ListBucket`. A common misconfiguration is granting only `PutObject`/`GetObject`
  without `ListBucket`, which breaks `doesObjectExist`'s 404-vs-403 disambiguation
  (`S3Utils.isObjectInBucketIgnoringPermission`, `S3Utils.java:141-158`, which specifically treats a 403 as
  "object exists but inaccessible" — a permissions gap here can silently mask a genuinely missing object).
- **Eventual consistency is not specially handled**: this module assumes S3's strong read-after-write
  consistency (current AWS S3 behavior); there is no explicit wait/backoff for "just-written object not yet
  visible to GET" beyond the generic `S3RETRY` predicate, which does not treat a 404 immediately following a
  successful PUT as retryable. On S3-compatible stores with weaker consistency, a pull immediately after push
  could observe `isObjectInBucket` returning `false` and fail hard `(inferred risk, not verified against
  actual weakly-consistent backends)`.
- **`useUniquePath` orphan risk**: as noted in Nuances, a segment pushed with a unique path whose subsequent
  metadata publish fails leaves a permanently orphaned S3 object; nothing in this module scans for or reaps
  unreferenced unique-path objects.
- **Single-threaded push executor is a serialization point**: because `pushExecutor` is single-threaded per
  Appenderator (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:191`), a slow/retrying S3 upload for one segment blocks all other
  segment pushes for that same task, potentially delaying handoff for unrelated intervals within the same
  task instance.
- **`descriptor.json` deletion in `S3DataSegmentKiller#kill` is dead weight for new segments** — every kill
  call still issues a `doesObjectExist`/`deleteObject` pair against a `descriptor.json` key that current
  pushers never write (`S3DataSegmentKiller.java:215-220`); harmless but doubles the round-trips per
  single-segment kill for no benefit on modern segments.
- **`RetryableS3OutputStream`/`S3OutputConfig` (durable storage & export) and `S3TransferConfig` (segment
  push) are two independent chunking/retry configurations** that happen to share the same underlying
  `ServerSideEncryptingAmazonS3`/`S3Utils.retryS3Operation` plumbing but are tuned and sized completely
  separately — changing one does not affect the other, which is easy to assume incorrectly when tuning upload
  throughput.
