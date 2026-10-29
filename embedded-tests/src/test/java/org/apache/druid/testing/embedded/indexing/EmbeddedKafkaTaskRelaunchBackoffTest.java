/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Runs a Kafka supervisor whose tasks always fail at startup, on a cluster with one Indexer, so that worker
 * blacklisting cannot stop the relaunch loop. Compares the relaunch rate with and without
 * {@code druid.supervisor.taskRelaunchBackoff.enabled}.
 */
public class EmbeddedKafkaTaskRelaunchBackoffTest
{
  private static final Logger log = new Logger(EmbeddedKafkaTaskRelaunchBackoffTest.class);
  private static final long OBSERVATION_MILLIS = 45_000;

  @Nested
  class WithBackoff extends FailingTasksTestBase
  {
    WithBackoff()
    {
      super(true);
    }

    @Test
    @Timeout(180)
    public void test_failingTasksAreRelaunchedWithBackoff_andSuspendResumeRelaunchesAtOnce() throws Exception
    {
      final KafkaSupervisorSpec spec = startSupervisorWithFailingTasks();
      Thread.sleep(OBSERVATION_MILLIS);
      final List<Long> gaps = getLaunchGapsMillis("with backoff");

      // A failure is seen about 5 s after the launch, and the delay (1, 1.6, 2.6, 4.1 s...) starts then.
      // Each delay is longer than the previous one even with jitter, so the gaps grow.
      // Without a wake-up at the end of each delay, launches would wait for the 30 s supervisor period.
      Assertions.assertTrue(gaps.size() >= 4 && gaps.size() <= 7, "gaps: " + gaps);
      for (int i = 1; i < gaps.size(); i++) {
        Assertions.assertTrue(gaps.get(i) > gaps.get(i - 1), "gaps: " + gaps);
      }
      Assertions.assertTrue(gaps.get(gaps.size() - 1) >= 8_000, "gaps: " + gaps);
      Assertions.assertTrue(gaps.get(gaps.size() - 1) < 30_000, "gaps: " + gaps);

      makeTaskDirectoriesWritable(true);
      cluster.callApi().onLeaderOverlord(o -> o.postSupervisor(spec.createSuspendedSpec()));
      cluster.callApi().onLeaderOverlord(o -> o.postSupervisor(spec));
      waitForRunningTask(10_000);
    }
  }

  @Nested
  class WithoutBackoff extends FailingTasksTestBase
  {
    WithoutBackoff()
    {
      super(false);
    }

    @Test
    @Timeout(180)
    public void test_failingTasksAreRelaunchedAtOnce() throws Exception
    {
      startSupervisorWithFailingTasks();
      Thread.sleep(OBSERVATION_MILLIS);
      final List<Long> gaps = getLaunchGapsMillis("without backoff");

      // The supervisor run that follows each launch by 5 s sees the failure and relaunches at once
      Assertions.assertTrue(gaps.size() >= 6, "gaps: " + gaps);
      gaps.forEach(gap -> Assertions.assertTrue(gap < 6_500, "gaps: " + gaps));
    }
  }

  abstract static class FailingTasksTestBase extends EmbeddedClusterTestBase
  {
    private final boolean backoffEnabled;
    private final EmbeddedOverlord overlord = new EmbeddedOverlord();
    private final EmbeddedIndexer indexer = new EmbeddedIndexer();
    private final File baseTaskDir;
    private final String topic = EmbeddedClusterApis.createTestDatasourceName();
    private KafkaResource kafkaServer;

    FailingTasksTestBase(boolean backoffEnabled)
    {
      this.backoffEnabled = backoffEnabled;
      this.baseTaskDir = FileUtils.createTempDir("relaunch-backoff-tasks");
    }

    @Override
    public EmbeddedDruidCluster createCluster()
    {
      final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
      kafkaServer = new KafkaResource()
      {
        @Override
        public void start()
        {
          super.start();
          createTopicWithPartitions(topic, 1);
        }
      };

      overlord.addProperty("druid.supervisor.taskRelaunchBackoff.enabled", String.valueOf(backoffEnabled));
      indexer.addProperty("druid.worker.capacity", "2")
             .addProperty("druid.indexer.task.baseTaskDir", baseTaskDir.getAbsolutePath());

      return cluster.addExtension(KafkaIndexTaskModule.class)
                    .addResource(kafkaServer)
                    .addServer(new EmbeddedCoordinator())
                    .addServer(overlord)
                    .addServer(indexer);
    }

    /**
     * Makes the Indexer task slot directories read-only, so that every new task fails when it creates its
     * directory, then submits the supervisor.
     */
    KafkaSupervisorSpec startSupervisorWithFailingTasks()
    {
      makeTaskDirectoriesWritable(false);
      final KafkaSupervisorSpec spec = createSupervisorSpec();
      cluster.callApi().onLeaderOverlord(o -> o.postSupervisor(spec));
      return spec;
    }

    void makeTaskDirectoriesWritable(boolean writable)
    {
      final File[] slotDirs = baseTaskDir.listFiles(file -> file.isDirectory() && file.getName().startsWith("slot"));
      Assertions.assertNotNull(slotDirs);
      Assertions.assertTrue(slotDirs.length > 0);
      for (File slotDir : slotDirs) {
        Assertions.assertTrue(slotDir.setWritable(writable));
      }
    }

    List<TaskStatusPlus> getTasksByCreatedTime() throws IOException
    {
      final List<TaskStatusPlus> tasks = new ArrayList<>();
      try (CloseableIterator<TaskStatusPlus> iterator =
               cluster.callApi().onLeaderOverlord(o -> o.taskStatuses(null, dataSource, null))) {
        iterator.forEachRemaining(tasks::add);
      }
      tasks.sort(Comparator.comparing(TaskStatusPlus::getCreatedTime));
      return tasks;
    }

    List<Long> getLaunchGapsMillis(String label) throws IOException
    {
      final List<TaskStatusPlus> tasks = getTasksByCreatedTime();
      final List<Long> gapsMillis = new ArrayList<>();
      for (int i = 1; i < tasks.size(); i++) {
        gapsMillis.add(tasks.get(i).getCreatedTime().getMillis() - tasks.get(i - 1).getCreatedTime().getMillis());
      }
      log.info(
          "Relaunch loop %s: [%d] launches, states%s, gaps between launches (ms)%s",
          label,
          tasks.size(),
          tasks.stream().map(TaskStatusPlus::getStatusCode).collect(Collectors.toList()),
          gapsMillis
      );
      return gapsMillis;
    }

    void waitForRunningTask(long timeoutMillis) throws Exception
    {
      final long deadline = System.currentTimeMillis() + timeoutMillis;
      while (System.currentTimeMillis() < deadline) {
        final boolean running = getTasksByCreatedTime().stream()
                                                       .anyMatch(task -> task.getStatusCode() == TaskState.RUNNING);
        if (running) {
          return;
        }
        Thread.sleep(200);
      }
      Assertions.fail("No task started within " + timeoutMillis + " ms");
    }

    private KafkaSupervisorSpec createSupervisorSpec()
    {
      return new KafkaSupervisorSpec(
          dataSource + "_supe",
          null,
          DataSchema.builder()
                    .withDataSource(dataSource)
                    .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                    .withGranularity(new UniformGranularitySpec(Granularities.HOUR, null, null))
                    .withDimensions(DimensionsSpec.EMPTY)
                    .build(),
          null,
          new KafkaSupervisorIOConfig(
              topic,
              null,
              new JsonInputFormat(null, null, null, null, null),
              null,
              1,
              Period.hours(1),
              kafkaServer.consumerProperties(),
              null, null, null,
              Period.millis(10),
              Period.seconds(30),
              true,
              Period.minutes(30),
              null, null, null, null, null, null, null, null
          ),
          null, null, null, null, null, null, null, null, null, null, null
      );
    }
  }
}
