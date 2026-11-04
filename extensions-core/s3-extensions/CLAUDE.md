# druid-s3-extensions

Deep-storage backend for S3 (push/pull/kill/move/archive of segments), the `S3InputSource` for batch ingestion, and S3-backed task logs / durable-storage export. Loaded by every process type: **Peon/Indexer** (pushes segments it builds, e.g. from `index_kafka`), **Historical** (pulls segments into local cache), **Overlord/Coordinator/Peon** (kill segments via `DataSegmentKiller`, move/archive via rules), and any process writing task logs to S3.

> **Execution mechanics live in [`Code-deepdive-Claude.MD`](Code-deepdive-Claude.MD)** —
> the push/pull traces and the exact S3 retry predicate and backoff formula, traced at method-body granularity with thread ownership, failure modes and config knobs.
> This file is the class map; that one is the runtime behaviour.

## Position in the ingestion path

- `index_kafka` task's `StreamAppenderator` persists+merges a segment, then calls the injected `DataSegmentPusher.push()` — bound to `S3DataSegmentPusher` when `druid.storage.type=s3`.
- Pusher zips the segment dir, uploads to `s3://<bucket>/<baseKey>/<storageDir>/index.zip`, and stamps the segment's `loadSpec` with `type=s3_zip, bucket, key, S3Schema`.
- The returned `DataSegment` (with loadSpec) is published to metadata storage by the task; that's what Coordinator hands to Historicals.
- Historical's `SegmentLocalCacheManager` deserializes `loadSpec` into a `LoadSpec` (Jackson subtype `s3_zip` → `S3LoadSpec`) and calls `loadSegment()`, which delegates to `S3DataSegmentPuller.getSegmentFiles()` to download+unzip into the local segment cache dir.
- On drop/kill, `S3DataSegmentKiller` deletes the zip (and legacy `descriptor.json`) at the loadSpec's bucket/key.

## Key classes

Push:
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusher.java:40` — implements `DataSegmentPusher`. `push()`/`pushToPath()` (line 87) zip `indexFilesDir`, call `S3Utils.retryS3Operation` + `S3Utils.uploadFileIfPossible` (line 102), build loadSpec via `makeLoadSpec()` (line 144: `type=s3_zip`, `bucket`, `key`, `S3Schema`). `getPathForHadoop()` (line 58) returns `s3a://` or `s3n://` per `useS3aSchema`.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPusherConfig.java:28` — `bucket`, `baseKey`, `disableAcl`, `maxListingLength`, `useS3aSchema`.

Pull & load:
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3LoadSpec.java:37` — `@JsonTypeName("s3_zip")`, implements `LoadSpec`; `loadSegment(File outDir)` (line 59) delegates to puller.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentPuller.java:59` — implements `URIDataPuller`. `getSegmentFiles()` (line 74) checks object existence, then `CompressionUtils.unzip`/`gunzip` from an S3 `ByteSource`; retry predicate is `S3Utils.S3RETRY`.

Delete & move:
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentKiller.java:47` — implements `DataSegmentKiller`. `kill(DataSegment)` (line 202) deletes zip+descriptor; `kill(List)` (line 81) batches via `DeleteObjectsRequest` (max 1000/request); `killAll()` (line 228) wipes `bucket`/`baseKey` prefix via `S3Utils.deleteObjectsInPath`.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentMover.java:48` / `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3DataSegmentArchiver.java:34` (extends Mover) — implement `DataSegmentMover`/`DataSegmentArchiver`; copy object to a new bucket/key and rewrite `loadSpec`.

S3 client & config:
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/ServerSideEncryptingAmazonS3.java:62` — wraps `AmazonS3`, applies configured `ServerSideEncryption` on every `putObject`/multipart call; built via nested `Builder` in `S3StorageDruidModule`.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageConfig.java:28` — holds `sse` (`ServerSideEncryption`: Noop/S3/Kms/Custom) and `transfer` (`S3TransferConfig`).
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3TransferConfig.java:28` — `minimumUploadPartSize` (20MiB default), `multipartUploadThreshold` (20MiB default).
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3Utils.java:62` — `S3RETRY` predicate (line 73), `retryS3Operation` (lines 114/124, uses `RetryUtils`), `constructSegmentPath` (line 214), `uploadFileIfPossible` (line 348, chooses simple vs multipart upload).
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3StorageDruidModule.java:53` — Guice bindings: registers `S3LoadSpec` Jackson subtype, binds killer/mover/archiver/pusher to scheme `s3`/`s3_zip`, binds `druid.storage.*` config classes, constructs `ServerSideEncryptingAmazonS3`.

Input source & task logs:
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/data/input/s3/S3InputSource.java:69` — extends `CloudObjectInputSource`; `createEntity()` (line 326) wraps objects as `S3Entity`.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/data/input/s3/S3InputSourceConfig.java:36` — per-ingestion-spec credential overrides: `accessKeyId`/`secretAccessKey` (`PasswordProvider`), `assumeRoleArn`, `assumeRoleExternalId`.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3InputDataConfig.java:33` — `maxListingLength` for input-source object listing.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/S3TaskLogs.java:42` — implements `TaskLogs`; push/stream task logs, reports, status, payload to S3 (`pushTaskLog` line 159, `streamTaskLog` line 66).
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/output/S3OutputConfig.java:33` — durable-storage/export config: `bucket`, `prefix`, `tempDir`, `chunkSize` (default 100MiB), `maxRetry`.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/output/S3StorageConnector.java:52` — extends `ChunkingStorageConnector`, used for durable shuffle storage / query results export.
- `extensions-core/s3-extensions/src/main/java/org/apache/druid/storage/s3/output/S3ExportStorageProvider.java:44` — `ExportStorageProvider` for MSQ `EXTERN` export to S3 (`bucket`, `prefix`).

## Critical flows

(a) Task pushes a built segment to S3:
1. `StreamAppenderator#mergeAndPush` (`server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:977`) calls `dataSegmentPusher.push(mergedFile, segmentToPush, useUniquePath)`.
2. `S3DataSegmentPusher#push` → `#pushToPath` — zips dir, uploads via `S3Utils#uploadFileIfPossible`, returns segment with `loadSpec` from `#makeLoadSpec`.
3. Task publishes the returned `DataSegment` (with S3 loadSpec) to the metadata store.

(b) Historical loads that segment from S3:
1. `SegmentLocalCacheManager#getSegmentFiles` (`server/src/main/java/org/apache/druid/segment/loading/SegmentLocalCacheManager.java:357`) → `#loadSegmentWithRetry` (line 385) → converts `segment.getLoadSpec()` map to `LoadSpec` via Jackson (line 483) → `S3LoadSpec#loadSegment`.
2. `S3LoadSpec#loadSegment` → `S3DataSegmentPuller#getSegmentFiles` — checks existence, streams+unzips into the local cache dir.

## Configuration reference

| Property | Config class / field |
|---|---|
| `druid.storage.type=s3` | selects this module's `DataSegmentPusher` binding (scheme `s3`) |
| `druid.storage.bucket` | `S3DataSegmentPusherConfig.bucket` |
| `druid.storage.baseKey` | `S3DataSegmentPusherConfig.baseKey` |
| `druid.storage.disableAcl` | `S3DataSegmentPusherConfig.disableAcl` |
| `druid.storage.useS3aSchema` | `S3DataSegmentPusherConfig.useS3aSchema` |
| `druid.storage.maxListingLength` | `S3DataSegmentPusherConfig.maxListingLength` / `S3InputDataConfig.maxListingLength` |
| `druid.storage.archiveBucket`/`archiveBaseKey` | `S3DataSegmentArchiverConfig` |
| `druid.storage.sse.type`, `.kms.*`, `.custom.*` | `S3StorageConfig.serverSideEncryption`, `S3SSEKmsConfig`, `S3SSECustomConfig` |
| `druid.storage.transfer.minimumUploadPartSize`/`multipartUploadThreshold` | `S3TransferConfig` |
| `druid.indexer.logs.*` (type=s3) | `S3TaskLogsConfig` |
| `druid.s3.accessKey`/`secretKey`/etc. | `AWSCredentialsConfig` (in `druid-aws-common`, not this module) |
| `druid.s3.proxy.*`, `druid.s3.endpoint.*`, `druid.s3.protocol` etc. | `AWSProxyConfig`/`AWSEndpointConfig`/`AWSClientConfig` (aws-common), consumed in `S3StorageDruidModule#getServerSideEncryptingAmazonS3Builder` |
| output/export `bucket`/`prefix`/`chunkSize`/`maxRetry`/`tempDir` (per-config, not global property) | `S3OutputConfig` |

## Cross-module pointers

- `DataSegmentPusher` interface: `processing/src/main/java/org/apache/druid/segment/loading/DataSegmentPusher.java`
- `LoadSpec` interface: `processing/src/main/java/org/apache/druid/segment/loading/LoadSpec.java`
- `DataSegmentKiller` interface: `processing/src/main/java/org/apache/druid/segment/loading/DataSegmentKiller.java`
- `SegmentCacheManager` interface: `server/src/main/java/org/apache/druid/segment/loading/SegmentCacheManager.java` (implemented by `SegmentLocalCacheManager`, the Historical-side caller of `LoadSpec#loadSegment`)
- Task-side push call sites: `server/src/main/java/org/apache/druid/segment/realtime/appenderator/StreamAppenderator.java:977` (streaming ingestion, e.g. `index_kafka`) and `BatchAppenderator.java` (batch); also `indexing-service/.../batch/parallel/PartialSegmentMergeTask.java` and `indexing-service/.../worker/shuffle/DeepStorageIntermediaryDataManager.java` for intermediate shuffle data.

## Gotchas

- `S3DataSegmentPusherConfig.useS3aSchema` only affects `getPathForHadoop()` and the `S3Schema` field in loadSpec (informational for Hadoop jobs); the actual push/pull always talks to S3 via the AWS SDK, not s3a/s3n Hadoop FS.
- Segment object key layout is `<baseKey>/<storageDir>` where `storageDir` mirrors `DataSegmentPusher#getStorageDir` (datasource/interval/version/partition); `S3Utils#constructSegmentPath` joins `baseKey` and `storageDir`.
- Killers/movers/archivers are Guice-instantiated at startup for every ingestion job if the extension is loaded, even when S3 isn't the active deep storage — hence they take a `Supplier<ServerSideEncryptingAmazonS3>` (lazy) instead of the client directly, deferring credential/config validation until actually used.
- Multi-object delete is capped at 1000 keys per `DeleteObjectsRequest` (`S3DataSegmentKiller.MAX_MULTI_OBJECT_DELETE_SIZE`); larger batches are chunked.
- `S3DataSegmentPusher#pushToPath` maps AWS `EntityTooLarge` errors to a user-facing `DruidException` suggesting smaller segments (S3 5GB single-PUT limit); large uploads instead rely on `S3TransferConfig`/`S3Utils#uploadFileIfPossible` multipart thresholds.
- All read paths (`S3DataSegmentPuller`, `S3Utils.getSingleObjectMetadata`, etc.) route through `S3Utils.S3RETRY` + `RetryUtils.retry` for transient/eventual-consistency style AWS errors.
- `descriptor.json` deletion in `S3DataSegmentKiller#kill` is legacy/best-effort — current pusher no longer writes a separate descriptor file, load/kill metadata all lives in the `loadSpec` persisted in Druid's metadata store.
