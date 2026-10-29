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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Delays task creation for a task group whose tasks keep failing, using exponential backoff with jitter.
 * The first failure is not delayed. The backoff of a group is reset once one of its tasks succeeds, or once a task
 * that started after the last failure has been running for {@link #HEALTHY_RUN_MILLIS}.
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
  static final int MAX_STATUS_CHECKS = 3;

  private static final Logger log = new Logger(TaskGroupRelaunchBackoff.class);

  private final String supervisorId;
  private final boolean enabled;
  private final LongSupplier clock;
  private final DoubleSupplier random;
  private final Map<Integer, GroupBackoff> groupBackoffs = new HashMap<>();
  private final Map<String, LaunchedTask> launchedTasks = new HashMap<>();

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

  boolean isEnabled()
  {
    return enabled;
  }

  void recordFailure(int groupId)
  {
    if (!enabled) {
      return;
    }

    final long nowMillis = clock.getAsLong();
    final GroupBackoff backoff = groupBackoffs.computeIfAbsent(groupId, id -> new GroupBackoff());
    backoff.consecutiveFailures++;
    backoff.lastFailureMillis = nowMillis;
    final long delayMillis = computeDelayMillis(backoff.consecutiveFailures);
    backoff.nextLaunchTimeMillis = nowMillis + delayMillis;

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
      taskIds.forEach(taskId -> launchedTasks.put(taskId, new LaunchedTask(groupId)));
    }
  }

  /**
   * Returns the tracked launched tasks, mapped to their task group, so that the supervisor can find the ones that
   * complete before it discovers them. A task stays tracked until {@link #forgetLaunchedTask} is called for it, once
   * the supervisor has discovered the task or found its final status, but it is returned at most
   * {@link #MAX_STATUS_CHECKS} times.
   */
  Map<String, Integer> pollLaunchedTasks()
  {
    final Map<String, Integer> tasks = new HashMap<>();
    final Iterator<Map.Entry<String, LaunchedTask>> iterator = launchedTasks.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<String, LaunchedTask> entry = iterator.next();
      final LaunchedTask task = entry.getValue();
      if (task.statusChecks >= MAX_STATUS_CHECKS) {
        iterator.remove();
      } else {
        task.statusChecks++;
        tasks.put(entry.getKey(), task.groupId);
      }
    }
    return tasks;
  }

  void forgetLaunchedTask(String taskId)
  {
    launchedTasks.remove(taskId);
  }

  /**
   * Stops tracking the launched tasks of the group, for example because the supervisor killed them.
   */
  void forgetLaunchedTasks(int groupId)
  {
    launchedTasks.values().removeIf(task -> task.groupId == groupId);
  }

  void reset(int groupId)
  {
    groupBackoffs.remove(groupId);
    forgetLaunchedTasks(groupId);
  }

  void clear()
  {
    groupBackoffs.clear();
    launchedTasks.clear();
  }

  /**
   * Moves the backoff of each task group to the groups that get its partitions after a task count change, so that the
   * change does not relaunch failing partitions at once. A group that gets the partitions of several groups gets the
   * longest backoff among them. Launched tasks are no longer tracked, since the supervisor stops all tasks for the
   * change.
   *
   * @param newGroupIds maps the ID of a task group to the IDs of the task groups that get its partitions
   */
  void remapTaskGroups(Function<Integer, Set<Integer>> newGroupIds)
  {
    final Map<Integer, GroupBackoff> remapped = new HashMap<>();
    groupBackoffs.forEach(
        (groupId, backoff) -> newGroupIds.apply(groupId).forEach(
            newGroupId -> remapped.merge(newGroupId, backoff.copy(), GroupBackoff::keepLongest)
        )
    );
    groupBackoffs.clear();
    groupBackoffs.putAll(remapped);
    launchedTasks.clear();
  }

  /**
   * Whether a task of the group shows that the group has recovered: the task started after the last failure of the
   * group and has been running for {@link #HEALTHY_RUN_MILLIS}. A task that started earlier, such as a replica that
   * kept running while another replica failed, does not count.
   */
  boolean hasRecovered(int groupId, @Nullable DateTime taskStartTime)
  {
    final GroupBackoff backoff = groupBackoffs.get(groupId);
    return backoff != null
           && taskStartTime != null
           && taskStartTime.getMillis() > backoff.lastFailureMillis
           && clock.getAsLong() - taskStartTime.getMillis() >= HEALTHY_RUN_MILLIS;
  }

  /**
   * Returns the task groups whose next launch is delayed, mapped to the remaining delay. The supervisor takes one
   * snapshot per run, so that a delay that ends during the run does not change how the run treats the group.
   */
  Map<Integer, Long> getRemainingDelaysMillis()
  {
    final long nowMillis = clock.getAsLong();
    final Map<Integer, Long> delaysMillis = new HashMap<>();
    groupBackoffs.forEach((groupId, backoff) -> {
      if (backoff.nextLaunchTimeMillis > nowMillis) {
        delaysMillis.put(groupId, backoff.nextLaunchTimeMillis - nowMillis);
      }
    });
    return delaysMillis;
  }

  private long computeDelayMillis(int consecutiveFailures)
  {
    if (consecutiveFailures < 2) {
      return 0;
    }

    // The cap applies before the jitter, so that delays at the cap are still spread out. It leaves room for the
    // jitter, so that no delay exceeds MAX_DELAY_MILLIS.
    final double delayMillis = Math.min(
        MAX_DELAY_MILLIS / (1 + JITTER),
        INITIAL_DELAY_MILLIS * Math.pow(MULTIPLIER, consecutiveFailures - 2)
    );
    final double jitterFactor = 1 - JITTER + 2 * JITTER * random.getAsDouble();
    return Math.round(delayMillis * jitterFactor);
  }

  private static class GroupBackoff
  {
    int consecutiveFailures;
    long lastFailureMillis;
    long nextLaunchTimeMillis;

    GroupBackoff copy()
    {
      final GroupBackoff copy = new GroupBackoff();
      copy.consecutiveFailures = consecutiveFailures;
      copy.lastFailureMillis = lastFailureMillis;
      copy.nextLaunchTimeMillis = nextLaunchTimeMillis;
      return copy;
    }

    GroupBackoff keepLongest(GroupBackoff other)
    {
      consecutiveFailures = Math.max(consecutiveFailures, other.consecutiveFailures);
      lastFailureMillis = Math.max(lastFailureMillis, other.lastFailureMillis);
      nextLaunchTimeMillis = Math.max(nextLaunchTimeMillis, other.nextLaunchTimeMillis);
      return this;
    }
  }

  private static class LaunchedTask
  {
    final int groupId;
    int statusChecks;

    LaunchedTask(int groupId)
    {
      this.groupId = groupId;
    }
  }
}
