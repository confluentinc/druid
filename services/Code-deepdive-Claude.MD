# Code deep dive — druid-services

This is an execution-mechanics companion to `services/CLAUDE.md` — read that first for the
process→`NodeRole` map, per-process module list, and Router class inventory. This document traces
actual method bodies for two things `services/` uniquely owns: (1) the `Lifecycle`-driven process
bootstrap/shutdown sequence, and (2) the Peon-JVM lifecycle and Router request-forwarding
internals, at `path:LINE` granularity with threads named.

Where the real logic lives outside `services/` — `ForkingTaskRunner`, `ExecutorLifecycle`,
`SingleTaskBackgroundRunner`, `ThreadingTaskRunner` in `indexing-service/`; Jetty bootstrap in
`server/` — that is called out explicitly. All line numbers verified with `grep -n` against branch
`34.0.0-confluent` (Druid 34.0.0).

---

## Happy path, traced

### (A) Query → Router → Broker → response streamed back

1. Jetty acceptor/selector thread
   (`server/src/main/java/org/apache/druid/server/initialization/jetty/JettyServerModule.java:198-239`)
   hands the request to a `QueuedThreadPool` worker (`druid.server.http.numThreads`, `:492-497`).
2. That worker runs `AsyncQueryForwardingServlet#service`
   (`services/src/main/java/org/apache/druid/server/AsyncQueryForwardingServlet.java:211`),
   branching on URI/method (`:224-228`).
3. Native POST: the same thread deserializes the *entire* `Query` body (`:255`), then calls
   `hostFinder.pickServer` (`:257`).
4. `QueryHostFinder#pickServer`
   (`services/src/main/java/org/apache/druid/server/router/QueryHostFinder.java:105`) →
   `#findServer` (`:55`) → `TieredBrokerHostSelector#select`
   (`services/src/main/java/org/apache/druid/server/router/TieredBrokerHostSelector.java:185`):
   rule-manager/started check (`:188-191`) → strategy chain (`:195-201`, default
   `[TimeBoundary, Priority(0,1)]`,
   `services/src/main/java/org/apache/druid/server/router/TieredBrokerConfig.java:58-61`) →
   cached `CoordinatorRuleManager.getRulesWithDefault`
   (`services/src/main/java/org/apache/druid/server/router/CoordinatorRuleManager.java:157`,
   a lock-free `AtomicReference` read, refreshed every `druid.router.pollPeriod` = `PT1M` on a
   separate `CoordinatorRuleManager-Exec` thread) → default-tier fallback (`:237-249`) →
   `NodesHolder#pick()` round robin (`:358-367`).
5. `QueryHostFinder#findServerInner` (`:128-160`) falls back to the `serverBackup` cache, then
   throws `ISE` only if nothing at all is found.
6. `sendProxyRequest` (`AsyncQueryForwardingServlet.java:429-479`) sets the per-request timeout and
   idle timeout from `druid.router.http.readTimeout` (default `PT15M` = 900000 ms, `:436-437`),
   and re-serializes the body (`:481-493`).
7. The Router's own `@Router` `HttpClient` (`services/src/main/java/org/apache/druid/cli/CliRouter.java:95`)
   sends the request on its own I/O threads — **the original Jetty worker is released here**
   (the async proxy thread handoff).
8. The response streams back on `HttpClient` I/O threads; `MetricsEmittingProxyResponseListener`
   (`:711-923`) emits metrics/logs and completes the client's `AsyncContext`.

### (B) Peon JVM starts, runs `index_kafka`, reports status

1. The MiddleManager's `forking-task-runner-N` pool (sized `druid.worker.capacity`,
   `indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java:157-159`)
   runs `ForkingTaskRunner#run` (`:163`): picks a storage slot (`:177`), allocates ports
   (`:196-201`), and builds
   `java ... org.apache.druid.cli.Main internal peon <taskDir> <attemptId>` (`:367-371`).
2. `runTaskProcess` (`:498-505`) forks a real OS process; `waitForTaskProcessToComplete`
   (`:508-529`) blocks that pool thread on `process.waitFor()` (`:519`) for the task's entire life.
3. New JVM: `Main#main` (`services/src/main/java/org/apache/druid/cli/Main.java:50`) → `CliPeon`
   (registered `services/src/main/java/org/apache/druid/cli/Main.java:94`). `CliPeon` extends `GuiceRunnable`, **not** `ServerRunnable`;
   `run()` (`services/src/main/java/org/apache/druid/cli/CliPeon.java:385`) builds its own
   injector (`:388`).
4. `CliPeon#getModules` (`:213-381`) binds `TaskRunner` → `SingleTaskBackgroundRunner` (`:273-274`),
   a `readTask` provider (`:311-325`, which pulls from deep storage via `TaskPayloadManager` if
   `task.json` is missing), and binds `ExecutorLifecycle` at `ManageLifecycle` (`:253-257`).
5. `initLifecycle` (`services/src/main/java/org/apache/druid/cli/GuiceRunnable.java:92-149`) →
   `Lifecycle#start()` (`processing/src/main/java/org/apache/druid/java/util/common/lifecycle/Lifecycle.java:327`)
   starts `ExecutorLifecycle`.
6. `ExecutorLifecycle#start`
   (`indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java:90`):
   reads `task.json` (`:97`); if `isParentStreamDefined()` (false only in k8s mode,
   `CliPeon.java:260-263`) takes a `FileLock` (`:110-140`) and spawns a `parent-monitor-%d` thread
   (`:66`, `:148-163`) blocked on stdin — EOF → `System.exit(2)` (`:161`, a hard exit with no
   `Lifecycle.stop()`).
7. `task.isReady` check (`:169-175`); then
   `statusFuture = Futures.transform(taskRunner.run(task), ...)` (`:177-204`) —
   `SingleTaskBackgroundRunner#run`
   (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/SingleTaskBackgroundRunner.java:266`)
   submits to a fresh single-thread `task-runner-0-priority-N` executor (`:148-156`, `:280-282`).
8. That thread runs `SingleTaskBackgroundRunnerCallable#call` (`:463-504`) → `task.run(toolbox)`
   (`:478`) → `KafkaIndexTask`/`SeekableStreamIndexTaskRunner` (see
   `indexing-service/Code-deepdive-Claude.MD`).
9. On completion the `Futures.transform` callback (`ExecutorLifecycle.java:181-201`, via
   `MoreExecutors.directExecutor()` — so the same thread) writes `status.json` (`:194`).
10. `CliPeon#run`'s main thread (`:398`) unblocks from `ExecutorLifecycle#join` →
    `statusFuture.get()` (`:207-215`), calls `lifecycle.stop()` (`:409`), and the JVM exits.
11. The MiddleManager's blocked `process.waitFor()` (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java:519`) returns;
    exit 0 → read `status.json` (`:413-416`); non-zero → synthesize `TaskStatus.failure`
    **without reading `status.json`** (`:417-427`).

---

## Nuances

- **`Lifecycle.Stage` order `INIT < NORMAL < SERVER < ANNOUNCEMENTS`**
  (`processing/src/main/java/org/apache/druid/java/util/common/lifecycle/Lifecycle.java:77-83`)
  is load-bearing: `ServerRunnable` registers the Jetty `Server` at `SERVER` and node announcement
  at `ANNOUNCEMENTS` (`services/src/main/java/org/apache/druid/cli/ServerRunnable.java:173-199`)
  so a node is never advertised before it is listening. `stop()` reverses the order, un-announcing
  (`:186-196`) before the server stops.
- **Startup failure is fatal, with no rollback.** `GuiceRunnable#initLifecycle`
  (`GuiceRunnable.java:136-142`) treats any `lifecycle.start()` `Throwable` as fatal —
  `System.exit(1)`.
- **The Router is stateless per request** beyond two in-memory caches:
  `TieredBrokerHostSelector.servers` and `QueryHostFinder.serverBackup`
  (`services/src/main/java/org/apache/druid/server/router/QueryHostFinder.java:43`, `:156`).
- **The Router parses the entire native `Query` body** (`services/src/main/java/org/apache/druid/server/AsyncQueryForwardingServlet.java:255`)
  just to read the datasource/intervals. SQL is only strategy-routed when
  `druid.router.sql.enable=true` (default `false`, `:106-107`, `:176-178`); otherwise
  `pickDefaultServer()` runs with zero inspection.
- **Units.** All `DruidHttpClientConfig` timeouts are Joda `Period`/`Duration` but are consumed in
  **milliseconds** at the call sites (`:436-437`). `TaskConfig.gracefulShutdownTimeout` /
  `directoryLockTimeout` default `PT5M` / `PT10M`
  (`indexing-service/src/main/java/org/apache/druid/indexing/common/config/TaskConfig.java:69-70`).

---

## Failure modes

| Failure | Where it surfaces (`path:LINE`) | Detection | Blast radius | Recovery / knob |
|---|---|---|---|---|
| Broker down / empty tier | `TieredBrokerHostSelector.java:362-364` (`pick()` → null) | `QueryHostFinder.java:137-153` falls back to `serverBackup` | 1 query fails (`ISE`, `:172-173`) or routes stale | Populate every tier in `druid.router.tierToBrokerMap` |
| Router proxy timeout | `AsyncQueryForwardingServlet.java:436-437` | `onFailure` (`:830-923`) `TimeoutException` | Client errors; the Broker keeps working (it is **not** cancelled) | `druid.router.http.readTimeout` (ms) |
| Peon OOM / killed | `ForkingTaskRunner.java:411-428` | non-zero exit, no `status.json` | `TaskStatus.failure`; slot freed (`:467-473`) | `druid.indexer.runner.javaOpts` / task `context.javaOpts` |
| Parent MM dies, orphaned Peon | `ExecutorLifecycle.java:148-162` | stdin EOF | `System.exit(2)` (`:161`), no graceful stop | Disable via k8s mode (`CliPeon.java:260-263`) |
| Port exhaustion | `ForkingTaskRunner.java:196-201` | `PortFinder` throws | New task launches fail on that MM | `druid.indexer.runner.startPort`/`endPort`/`ports` |
| Task JSON deserialize failure | `CliPeon.java:311-325`, `indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java:97` | `IOException` → `RuntimeException` | Peon fails to start; task marked failed | Operator fixes the task.json source |
| Lifecycle stage startup failure | `GuiceRunnable.java:136-142` | caught `Throwable` | `System.exit(1)`, whole process dies | Fix config; no partial recovery |

The Router **never health-checks Brokers proactively** — an unresponsive-but-still-discovered
Broker stays in rotation until a proxy call actually times out.

Peon OOM/kill and parent-death both skip graceful shutdown (no `taskLockFileLock.release()`,
`ExecutorLifecycle.java:222-230`), which is exactly why the task-directory lock and
`directoryLockTimeout` exist.

---

## Optimizations in the current structure

- **Async non-blocking proxy** (`AsyncProxyServlet` base, `services/src/main/java/org/apache/druid/server/AsyncQueryForwardingServlet.java:90`)
  releases the Jetty worker thread once the Broker call is dispatched.
  *Cost:* query bodies are still fully buffered and deserialized once for routing
  (`:255`, `:278`, `:481-493`).
- **Connection pooling to Brokers** via the shared `@Router` `HttpClient` (`services/src/main/java/org/apache/druid/cli/CliRouter.java:95`,
  `druid.router.http.numConnections` = 20).
  *Cost:* bursts queue behind `numRequestsQueued` = 1024.
- **Process isolation vs shared JVM.** `ForkingTaskRunner` forks a JVM per task
  (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java:501`); `ThreadingTaskRunner` runs tasks in-process
  (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/ThreadingTaskRunner.java:151`,
  `:240`) with zero fork cost and a shared processing pool.
  *Cost:* isolation pays JVM-boot latency per task; the shared JVM pays cross-task blast radius on
  OOM or thread starvation.

---

## Concurrency & thread model

- **Router/Broker Jetty:** 8 acceptor + selector threads per connector
  (`JettyServerModule.java:492-497`) plus a shared `QueuedThreadPool` sized
  `druid.server.http.numThreads` (default `max(10, cores*17/16+2)+30`,
  `server/src/main/java/org/apache/druid/server/initialization/ServerConfig.java:401-403`).
- **Router proxy:** parsing/routing on the Jetty worker thread; the outbound call and response
  callback on the separate `@Router` `HttpClient`'s threads (sized via
  `druid.router.http.numMaxThreads`,
  `services/src/main/java/org/apache/druid/cli/RouterJettyServerInitializer.java:174`).
- **Peon threads:** main (blocks in `ExecutorLifecycle#join`), `parent-monitor-%d` (blocked on
  stdin), and `task-runner-0-priority-N` (runs `task.run`). The status write is inline via
  `directExecutor()` (`indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java:203`).
- **`ThreadingTaskRunner`:** two pools sized `druid.worker.capacity` —
  `threading-task-runner-executor-%d` runs tasks (`:127-129`, `:240`), and
  `threading-task-runner-control-%d` runs graceful shutdowns concurrently (`:130-132`, `:345-386`).
- **`ForkingTaskRunner`:** the `forking-task-runner-%d` pool is sized `druid.worker.capacity`
  (`:157-159`), and each thread is blocked in `process.waitFor()` (`:519`) for the task's full
  duration — so it bounds concurrent JVMs, not CPU.

---

## Config knobs that change execution

| Property | Class field (`path:LINE`) | Default | What it changes |
|---|---|---|---|
| `druid.router.http.readTimeout` | `server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:47` | PT15M (900000 ms) | Router→Broker proxy + idle timeout |
| `druid.router.http.numConnections` | `server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:44` | 20 | Pooled connections per Broker |
| `druid.router.http.numMaxThreads` | `server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:51` | `max(10, cores*17/16+2)+30` | Router outbound HttpClient pool size |
| `druid.router.http.numRequestsQueued` | `server/src/main/java/org/apache/druid/guice/http/DruidHttpClientConfig.java:55` | 1024 | Proxy backlog before rejection |
| `druid.router.pollPeriod` | `services/src/main/java/org/apache/druid/server/router/TieredBrokerConfig.java:54` | PT1M | Load-rule cache refresh interval |
| `druid.router.tierToBrokerMap` | `services/src/main/java/org/apache/druid/server/router/TieredBrokerConfig.java:46`, `:66-71` | `{DEFAULT_TIER: defaultBrokerServiceName}` | Whether tiering is active at all |
| `druid.router.strategies` | `TieredBrokerConfig.java:58-61` | `[TimeBoundary, Priority(0,1)]` | Strategy chain tried before rule fallback |
| `druid.router.sql.enable` | `AsyncQueryForwardingServlet.java:106-107`, `:176-178` | `false` | SQL strategy-routing on/off |
| `druid.server.http.numThreads` | `server/src/main/java/org/apache/druid/server/initialization/ServerConfig.java:115`, `:401-403` | `max(10, cores*17/16+2)+30` | Inbound Jetty pool on all server-type Clis |
| `druid.worker.capacity` | `server/src/main/java/org/apache/druid/indexing/worker/config/WorkerConfig.java:45` | `max(1, cores-1)` | Concurrent Peon JVMs / in-thread tasks |
| `druid.indexer.runner.javaOpts` / `javaOptsArray` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/ForkingTaskRunnerConfig.java:50`, `:54` | `""` / `[]` | Forked Peon JVM flags |
| `druid.indexer.runner.startPort` / `endPort` / `ports` | `indexing-service/src/main/java/org/apache/druid/indexing/overlord/config/ForkingTaskRunnerConfig.java:63`, `:68`, `:76` | 8100 / 65535 / `[]` | Peon port allocation range |
| `druid.indexer.task.gracefulShutdownTimeout` | `indexing-service/src/main/java/org/apache/druid/indexing/common/config/TaskConfig.java:93`, `:70` | PT5M | Runner shutdown grace period |
| `druid.indexer.task.directoryLockTimeout` | `indexing-service/src/main/java/org/apache/druid/indexing/common/config/TaskConfig.java:96`, `:69` | PT10M | Task-dir lock wait on Peon start |

---

## Sharp edges

1. **Non-zero Peon exit never reads `status.json`**, even if the file is valid and complete
   (`ForkingTaskRunner.java:417-427`). Every hard kill looks like a generic failure.
2. **Parent-death `System.exit(2)`** (`indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java:161`) bypasses `Lifecycle.stop()`
   entirely — no un-announcement, no lock release.
3. **`TieredBrokerHostSelector#getDefaultLookup`** (`:308-312`) can NPE if
   `defaultBrokerServiceName` is not also a value in `tierToBrokerMap`.
4. **`CliPeon` has no Jetty or announcement by default**; only `--loadBroadcastDatasourceMode`
   (`CliPeon.java:292-298`) makes it discoverable/queryable.
5. **`ForkingTaskRunner`'s pool thread is blocked for a task's whole lifetime** (`:519`), so
   `druid.worker.capacity` is a hard process ceiling, not a throughput knob.
