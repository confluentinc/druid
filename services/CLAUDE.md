# druid-services

Role: this module (`services/pom.xml` artifactId `druid-services`) is the **process bootstrap module** for every
Druid server type (`org.apache.druid.cli.*`) AND the concrete implementation of the **Router** process
(`org.apache.druid.server.router.*`, plus `org.apache.druid.server.AsyncQueryForwardingServlet`, which despite its
`org.apache.druid.server` package physically lives here, not in `server/`). `server/` supplies the generic
query-serving/coordination building blocks; `services/` wires them into runnable JVM entry points and owns Router
routing logic end-to-end.

> **Execution mechanics live in [`Code-deepdive-Claude.MD`](Code-deepdive-Claude.MD)** —
> Lifecycle bootstrap ordering, the Peon JVM lifecycle, and Router request forwarding, traced at method-body granularity with thread ownership, failure modes and config knobs.
> This file is the class map; that one is the runtime behaviour.

Scope of this doc: process bootstrap (`cli` package), Router internals, and the two pipeline paths given —
ingestion (`index_kafka` via Indexer/MiddleManager+Peon) and query (Router -> Broker -> Historical). Not covered:
Hadoop-related CLIs (`CliHadoopIndexer`, `CliInternalHadoopIndexer`), `DumpSegment`, `PullDependencies`, etc.

## Process -> entry point -> NodeRole map

| Process | Cli class (`path:line`) | NodeRole | Notable installed modules |
|---|---|---|---|
| Coordinator | `services/src/main/java/org/apache/druid/cli/CliCoordinator.java:153` | `COORDINATOR` (+`OVERLORD` if `druid.coordinator.asOverlord.enabled=true`, checked at `CliCoordinator.java:156-157`) | `MetadataManagerModule`, `SegmentSchemaCacheModule`, `QueryableModule`, `LookupSerdeModule`, `SupervisorCleanupModule` (overlord mode) |
| Overlord | `services/src/main/java/org/apache/druid/cli/CliOverlord.java:159` | `OVERLORD` (`CliOverlord.java:177-179`) | `MetadataManagerModule` (standalone), `IndexingServiceInputSourceModule`, `IndexingServiceTaskLogsModule`, `IndexingServiceTuningConfigModule`, `SupervisorModule`, `LookupSerdeModule`, `SamplerModule` |
| Indexer | `services/src/main/java/org/apache/druid/cli/CliIndexer.java:98` | `INDEXER` (`CliIndexer.java:118-120`) | `DruidProcessingModule`, `QueryableModule`, `IndexerServiceModule`, `ShuffleModule`, `IndexingServiceInputSourceModule`, `IndexingServiceTaskLogsModule`, `IndexingServiceTuningConfigModule`, `QueryablePeonModule` |
| MiddleManager | `services/src/main/java/org/apache/druid/cli/CliMiddleManager.java:100` | `MIDDLE_MANAGER` (`CliMiddleManager.java:118-120`) | `MiddleManagerServiceModule`, `ShuffleModule`, `IndexingServiceInputSourceModule`, `IndexingServiceTaskLogsModule`, `IndexingServiceTuningConfigModule` |
| Peon | `services/src/main/java/org/apache/druid/cli/CliPeon.java:151` | `PEON` (`services/src/main/java/org/apache/druid/cli/CliPeon.java:388`, also bound `services/src/main/java/org/apache/druid/cli/CliPeon.java:490`) — extends `GuiceRunnable`, not `ServerRunnable` | `PeonProcessingModule`, `QueryableModule`, `IndexingServiceTaskLogsModule`, `SingleTaskBackgroundRunner`-backed `TaskRunner` binding (`services/src/main/java/org/apache/druid/cli/CliPeon.java:273`), `QueryablePeonModule`, `IndexingServiceInputSourceModule`, `IndexingServiceTuningConfigModule`, `ChatHandlerServerModule` |
| Historical | `services/src/main/java/org/apache/druid/cli/CliHistorical.java:74` | `HISTORICAL` (`CliHistorical.java:91-94`) | `DruidProcessingModule`, `QueryableModule`, `HistoricalServiceModule` (`services/src/main/java/org/apache/druid/guice/HistoricalServiceModule.java:29`), `CacheModule` |
| Broker | `services/src/main/java/org/apache/druid/cli/CliBroker.java:95` | `BROKER` (`CliBroker.java:112-115`) | `BrokerProcessingModule`, `QueryableModule`, `BrokerServiceModule`, `SqlModule`, `LookupModule`, `CacheModule` |
| Router | `services/src/main/java/org/apache/druid/cli/CliRouter.java:73` | `ROUTER` (`CliRouter.java:82-85`) | `RouterProcessingModule`, `QueryableModule`, `QueryRunnerFactoryModule`, `JettyHttpClientModule("druid.router.http", ...)`, `LookupSerdeModule`, anonymous binder wiring `TieredBrokerHostSelector`/`QueryHostFinder`/`AsyncQueryForwardingServlet` |

All server-type CLIs (except Peon) are registered in `services/src/main/java/org/apache/druid/cli/Main.java:58-66`
under the `server` command group; `CliPeon` is registered separately under the `internal` group
(`services/src/main/java/org/apache/druid/cli/Main.java:91-94`), reflecting that it's not meant to be launched by a
human — only forked by a task runner.

## Router (query path entry)

- `services/src/main/java/org/apache/druid/server/AsyncQueryForwardingServlet.java:90` — `AsyncProxyServlet`
  subclass; the actual HTTP entry point for all Router traffic (native `/druid/v2`, SQL `/druid/v2/sql`, Avatica
  JDBC). Key method `service()` (`:211`) branches on request type, calls `hostFinder.pickServer(inputQuery)` for
  native POST queries (`:257`), `hostFinder.findServerSql(...)`/`pickDefaultServer()` for SQL (`:276-285`, gated by
  `druid.router.sql.enable`, default `false` — see `PROPERTY_SQL_ENABLE_DEFAULT` at `:107`), and
  `hostFinder.findServerAvatica(connectionId)` for JDBC (`:230-246`). Sets `HOST_ATTRIBUTE`/`SCHEME_ATTRIBUTE`
  request attributes (`:301-302`) then delegates to Jetty's `AsyncProxyServlet` machinery
  (`doService`/`rewriteTarget`/`sendProxyRequest`, `:420-538`) to actually forward the HTTP request byte-for-byte to
  the chosen Broker.
- `services/src/main/java/org/apache/druid/server/router/QueryHostFinder.java:36` — thin facade over
  `TieredBrokerHostSelector`; `findServer`/`pickServer` (`:55-116`) call `hostSelector.select(query)`, cache the last
  good server per service name in `serverBackup` as a fallback (`findServerInner`, `:128-160`).
- `services/src/main/java/org/apache/druid/server/router/TieredBrokerHostSelector.java:54` — holds live Broker
  membership per tier (`servers: Map<brokerServiceName, NodesHolder>`), fed by `DruidNodeDiscoveryProvider
  .getForNodeRole(NodeRole.BROKER)` (`:131`). `select(query)` (`:185-250`): if the `TieredBrokerConfig` polling
  hasn't started or `CoordinatorRuleManager` isn't started yet, falls back to `getDefaultLookup()` (`:187-191`).
  Otherwise it runs the configured `TieredBrokerSelectorStrategy` list (`:195-201`) in order; first strategy to
  return a name wins. If none match, it consults load rules via `CoordinatorRuleManager.getRulesWithDefault(...)`
  (`:205`) to find the highest-priority `LoadRule` tier applicable to the query's intervals, then maps that tier to
  a broker service name (`:224-234`). Final fallback is `tierConfig.getDefaultBrokerServiceName()` (`:246`).
  Server-within-tier pick is simple round robin (`NodesHolder.pick()`, `:358-367`).
- `services/src/main/java/org/apache/druid/server/router/TieredBrokerConfig.java:35` — JSON-bound config
  (`druid.router.*`). `defaultBrokerServiceName` defaults to `"druid/broker"` (`:38,43`); `tierToBrokerMap` maps
  tier name -> broker service name, defaulting to a single entry `{DruidServer.DEFAULT_TIER: defaultBrokerServiceName}`
  (`:64-71`) if unset — i.e. with no explicit tiering config, everything routes to `druid/broker`. Default
  `strategies` list is `[TimeBoundaryTieredBrokerSelectorStrategy, PriorityTieredBrokerSelectorStrategy(0, 1)]`
  (`:58-61`).
- Strategy impls, all in `services/src/main/java/org/apache/druid/server/router/`:
  `services/src/main/java/org/apache/druid/server/router/ManualTieredBrokerSelectorStrategy.java:46` (honors an explicit broker service name set in the query/SQL
  context), `services/src/main/java/org/apache/druid/server/router/PriorityTieredBrokerSelectorStrategy.java:29` (routes by a `priority` query-context value/range),
  `services/src/main/java/org/apache/druid/server/router/TimeBoundaryTieredBrokerSelectorStrategy.java:29` (routes `timeBoundary`-type queries to a dedicated tier), and
  `services/src/main/java/org/apache/druid/server/router/JavaScriptTieredBrokerSelectorStrategy.java:31` (user-supplied JS function decides the tier).

## Task execution processes

- `services/src/main/java/org/apache/druid/cli/CliMiddleManager.java:100` (`middleManager` command) — long-lived
  process that accepts task assignments and forks a JVM per task via `ForkingTaskRunner`
  (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java`).
- `services/src/main/java/org/apache/druid/cli/CliPeon.java:151` (`internal peon` command, registered
  `services/src/main/java/org/apache/druid/cli/Main.java:94`) — the single-task worker JVM. Takes `taskDirPath` and `attemptId` as CLI args
  (`CliPeon.java:154-156`); binds `TaskRunner` to `SingleTaskBackgroundRunner`
  (`services/src/main/java/org/apache/druid/cli/CliPeon.java:273`, in `indexing-service/src/main/java/org/apache/druid/indexing/overlord/SingleTaskBackgroundRunner.java:72`,
  which also implements `QuerySegmentWalker` so the task's own segments are queryable while running). Provides the
  `Task` object via a `readTask` provider (`CliPeon.java:311-323`) that reads `task.json` from the task dir, pulling
  it from deep storage first if missing (`TaskPayloadManager`). Installs `IndexingServiceTaskLogsModule` for
  pushing logs, and conditionally `BroadcastSegmentLoadingModule` (`services/src/main/java/org/apache/druid/cli/CliPeon.java:297`) when
  `--loadBroadcastDatasourceMode` requires broadcast segments (used by queryable/streaming tasks like Kafka
  ingestion).
- `services/src/main/java/org/apache/druid/cli/CliIndexer.java:98` (`indexer` command) — alternative execution
  model: a single long-lived process that runs each task **in a thread**, not a forked JVM (see its `@Command`
  description at `CliIndexer.java:95-96`). Installs `IndexerServiceModule`, `ShuffleModule`,
  `IndexingServiceTaskLogsModule` directly; there is no separate Peon step in this model.
- Which model this cluster uses is not directly determinable from this module alone (unverified); the repo's
  `examples/conf/druid/**` sample configs only ship `middleManager/` directories (no `indexer/`), suggesting
  MiddleManager+Peon is the example/default topology, while the task's stated pipeline explicitly names "Indexer".
  Both are documented above; verify actual deployment via the cluster's `common.runtime.properties`
  /`druid.indexer.runner.type` if precision is required.

## Critical flows

**(a) Query enters Router, forwarded to a Broker**
1. `AsyncQueryForwardingServlet#service` (`services/src/main/java/org/apache/druid/server/AsyncQueryForwardingServlet.java:211`)
   parses the request by endpoint (native/SQL/Avatica).
2. `QueryHostFinder#pickServer` / `#findServerSql` / `#findServerAvatica`
   (`services/src/main/java/org/apache/druid/server/router/QueryHostFinder.java:105,92,74`)
3. `TieredBrokerHostSelector#select` / `#selectForSql`
   (`services/src/main/java/org/apache/druid/server/router/TieredBrokerHostSelector.java:185,272`) runs
   `TieredBrokerSelectorStrategy#getBrokerServiceName` implementations, falling back to `CoordinatorRuleManager`
   load-rule lookup, then to `TieredBrokerConfig#getDefaultBrokerServiceName`.
4. `TieredBrokerHostSelector.NodesHolder#pick` (`:358`) round-robins a live Broker `Server` for the resolved
   service name (membership sourced from `DruidNodeDiscoveryProvider` ZK/HTTP discovery, `:131`).
5. `AsyncQueryForwardingServlet#service` stores host/scheme on the request (`:301-302`) then calls
   `#doService` -> Jetty `AsyncProxyServlet#service` -> `#rewriteTarget`/`#sendProxyRequest` (`:509-538`) to proxy
   the HTTP call to the chosen Broker, which then executes the query against Historicals.

**(b) Peon JVM starts and runs an `index_kafka` task**
1. Overlord assigns the task to a MiddleManager; `ForkingTaskRunner#run`
   (`indexing-service/src/main/java/org/apache/druid/indexing/overlord/ForkingTaskRunner.java:161`) builds a
   `ProcessBuilder` command ending in `org.apache.druid.cli.Main internal peon <taskDir> <attemptId>`
   (`ForkingTaskRunner.java:367-371`) and forks a new JVM.
2. `Main#main` (`services/src/main/java/org/apache/druid/cli/Main.java:50`) dispatches to `CliPeon`
   (registered `services/src/main/java/org/apache/druid/cli/Main.java:94`).
3. `CliPeon#getModules` / Guice injector build (`services/src/main/java/org/apache/druid/cli/CliPeon.java:213-388`)
   binds `NodeRole.PEON` (`:388,490`), wires `TaskRunner` to `SingleTaskBackgroundRunner`, and provides the `Task`
   via the `readTask` provider (`:311-323`), which for a `index_kafka` task deserializes it as a `KafkaIndexTask`
   (`extensions-core/kafka-indexing-service/src/main/java/org/apache/druid/indexing/kafka/KafkaIndexTask.java`).
4. `ExecutorLifecycle` (`indexing-service/src/main/java/org/apache/druid/indexing/worker/executor/ExecutorLifecycle.java`)
   starts the task, ultimately invoking `SingleTaskBackgroundRunner#run` ->
   `KafkaIndexTaskRunner` (`extensions-core/kafka-indexing-service/.../KafkaIndexTaskRunner.java`) to pull records
   from Kafka, index them, hand off segments to deep storage (S3), and periodically commit consumer offsets.

## Cross-module pointers

- `server/`: generic building blocks reused by every Cli class — `QueryableModule`, `DruidProcessingModule`,
  `ServerConfig`, `RequestLogger`, `ServiceEmitter`, `AuthenticatorMapper`, `NodeRole`/`DruidNodeDiscoveryProvider`,
  `LoadRule`/`Rule` (used by `TieredBrokerHostSelector`), `BrokerServerView`/`CachingClusteredClient`/
  `ClientQuerySegmentWalker` (bound in `CliBroker.java:139-156`), `ZkCoordinator`/`SegmentManager`/`ServerManager`
  (bound in `CliHistorical.java` and `services/src/main/java/org/apache/druid/guice/HistoricalServiceModule.java`).
- `indexing-service/`: `ForkingTaskRunner` (forks Peon JVMs), `SingleTaskBackgroundRunner` (Peon's in-process
  `TaskRunner`), `ExecutorLifecycle`/`ExecutorLifecycleConfig` (drives task execution inside the Peon/Indexer),
  `TaskStorageDirTracker`, `SupervisorManager`/`SupervisorModule` (wired by `CliOverlord`).
- `extensions-core/kafka-indexing-service/`: `KafkaIndexTask`, `KafkaIndexTaskRunner`,
  `KafkaIndexTaskClientFactory`, `KafkaIndexTaskModule` — the actual `index_kafka` task type executed inside a Peon
  (or Indexer thread), consuming from Kafka and writing segments that eventually land in S3 deep storage.

## Gotchas

- With no explicit `druid.router.tierToBrokerMap` configured, `TieredBrokerConfig` defaults every tier to a single
  `DruidServer.DEFAULT_TIER -> "druid/broker"` mapping (`TieredBrokerConfig.java:64-71`), i.e. Router tiering is a
  no-op unless deliberately configured.
- Router SQL routing-by-strategy is opt-in: `AsyncQueryForwardingServlet` only calls
  `hostFinder.findServerSql(...)` when `druid.router.sql.enable=true` in the strategy sense — actually the relevant
  flag gating strategy-based SQL routing is `routeSqlByStrategy` (constructed from `druid.router.sql.enable`,
  default `"false"`, `AsyncQueryForwardingServlet.java:106-107,176-178`); otherwise all SQL goes to
  `pickDefaultServer()` (`:283-285`).
- `CliPeon` extends `GuiceRunnable` directly, not `ServerRunnable` like every other server Cli class — it has no
  Jetty server initializer wiring of its own by default; a query-serving task (e.g. streaming ingestion) has to
  opt in via `ChatHandlerServerModule`/broadcast-segment bindings.
- Indexer vs MiddleManager+Peon trade-off (from `@Command` descriptions): the Indexer (`CliIndexer.java:95-96`)
  runs each task **in a thread** inside one long-lived JVM (lower per-task startup overhead, shared JVM
  heap/GC domain across tasks); MiddleManager+Peon (`CliMiddleManager.java:97-98`,
  `CliPeon.java:146-149`) forks a **separate JVM per task**, isolating task failures/memory at the cost of JVM
  startup latency per task.
