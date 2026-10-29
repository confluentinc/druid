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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

/**
 * Delays task creation for a task group whose tasks keep failing, using exponential backoff with jitter.
 * The first failure is not delayed. The backoff of a group is reset once one of its tasks succeeds or has been
 * running for {@link #HEALTHY_RUN_MILLIS}.
 * <p>
 * Not thread-safe: {@link SeekableStreamSupervisor} uses it only from its notice-handling thread.
 */
class TaskGroupRelaunchBackoff
{
  static final long INITIAL_DELAY_MILLIS = 1_000;
  static final double MULTIPLIER = 1.6;
  static final long MAX_DELAY_MILLIS = 300_000;
  static final double JITTER = 0.2;
  static final long HEALTHY_RUN_MILLIS = 300_000;

  private static final Logger log = new Logger(TaskGroupRelaunchBackoff.class);

  private final String supervisorId;
  private final boolean enabled;
  private final LongSupplier clock;
  private final DoubleSupplier random;
  private final Map<Integer, GroupBackoff> groupBackoffs = new HashMap<>();
  private final Map<String, Integer> launchedTaskGroupIds = new HashMap<>();

  TaskGroupRelaunchBackoff(String supervisorId, boolean enabled)
  {
    this(supervisorId, enabled, System::currentTimeMillis, () -> ThreadLocalRandom.current().nextDouble());
  }

  @VisibleForTesting
  TaskGroupRelaunchBackoff(String supervisorId, boolean enabled, LongSupplier clock, DoubleSupplier random)
  {
    this.supervisorId = supervisorId;
    this.enabled = enabled;
    this.clock = clock;
    this.random = random;
  }

  void recordFailure(int groupId)
  {
    if (!enabled) {
      return;
    }

    final GroupBackoff backoff = groupBackoffs.computeIfAbsent(groupId, id -> new GroupBackoff());
    backoff.consecutiveFailures++;
    final long delayMillis = computeDelayMillis(backoff.consecutiveFailures);
    backoff.nextLaunchTimeMillis = clock.getAsLong() + delayMillis;

    if (delayMillis > 0) {
      log.warn(
          "Tasks of taskGroup[%d] for supervisor[%s] failed [%d] times in a row. Delaying the next launch by [%d] ms.",
          groupId,
          supervisorId,
          backoff.consecutiveFailures,
          delayMillis
      );
    }
  }

  void recordLaunch(int groupId, List<String> taskIds)
  {
    if (enabled) {
      taskIds.forEach(taskId -> launchedTaskGroupIds.put(taskId, groupId));
    }
  }

  /**
   * Stops tracking all launched tasks and returns those that are no longer active, mapped to their task group.
   * The supervisor discovers active tasks itself; the returned ones may have completed before it could.
   */
  Map<String, Integer> removeLaunchedTasks(Set<String> activeTaskIds)
  {
    final Map<String, Integer> completedTasks = new HashMap<>(launchedTaskGroupIds);
    completedTasks.keySet().removeAll(activeTaskIds);
    launchedTaskGroupIds.clear();
    return completedTasks;
  }

  boolean hasGroupsInBackoff()
  {
    return groupBackoffs.values().stream().anyMatch(backoff -> backoff.consecutiveFailures > 1);
  }

  void reset(int groupId)
  {
    groupBackoffs.remove(groupId);
    launchedTaskGroupIds.values().removeIf(id -> id == groupId);
  }

  void clear()
  {
    groupBackoffs.clear();
    launchedTaskGroupIds.clear();
  }

  boolean isHealthy(@Nullable DateTime taskStartTime)
  {
    return taskStartTime != null && clock.getAsLong() - taskStartTime.getMillis() >= HEALTHY_RUN_MILLIS;
  }

  long getRemainingDelayMillis(int groupId)
  {
    final GroupBackoff backoff = groupBackoffs.get(groupId);
    return backoff == null ? 0 : Math.max(0, backoff.nextLaunchTimeMillis - clock.getAsLong());
  }

  private long computeDelayMillis(int consecutiveFailures)
  {
    if (consecutiveFailures < 2) {
      return 0;
    }

    final double jitterFactor = 1 - JITTER + 2 * JITTER * random.getAsDouble();
    final double delayMillis = INITIAL_DELAY_MILLIS * Math.pow(MULTIPLIER, consecutiveFailures - 2) * jitterFactor;
    return (long) Math.min(MAX_DELAY_MILLIS, delayMillis);
  }

  private static class GroupBackoff
  {
    int consecutiveFailures;
    long nextLaunchTimeMillis;
  }
}
