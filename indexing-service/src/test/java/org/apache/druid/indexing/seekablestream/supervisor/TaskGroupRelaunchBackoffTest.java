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

import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class TaskGroupRelaunchBackoffTest
{
  private static final int GROUP_ID = 0;
  private static final double NO_JITTER = 0.5;

  private final AtomicLong now = new AtomicLong(1_000_000L);

  @Test
  public void testFirstFailureIsNotDelayed()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);

    backoff.recordFailure(GROUP_ID);

    Assert.assertEquals(0, getRemainingDelayMillis(backoff, GROUP_ID));
  }

  @Test
  public void testDelayGrowsExponentiallyUpToCap()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);

    final long[] expectedDelays = {1000, 1600, 2560, 4096, 6554};
    for (long expectedDelay : expectedDelays) {
      backoff.recordFailure(GROUP_ID);
      Assert.assertEquals(expectedDelay, getRemainingDelayMillis(backoff, GROUP_ID), 1.0);
    }

    // The cap leaves room for the jitter
    for (int i = 0; i < 20; i++) {
      backoff.recordFailure(GROUP_ID);
    }
    Assert.assertEquals(250_000, getRemainingDelayMillis(backoff, GROUP_ID));
  }

  @Test
  public void testJitterBounds()
  {
    final TaskGroupRelaunchBackoff lowest = newBackoff(0.0);
    lowest.recordFailure(GROUP_ID);
    lowest.recordFailure(GROUP_ID);
    Assert.assertEquals(800, getRemainingDelayMillis(lowest, GROUP_ID), 1.0);

    final TaskGroupRelaunchBackoff highest = newBackoff(1.0);
    highest.recordFailure(GROUP_ID);
    highest.recordFailure(GROUP_ID);
    Assert.assertEquals(1200, getRemainingDelayMillis(highest, GROUP_ID), 1.0);
  }

  @Test
  public void testJitterSpreadsDelaysAtCapWithoutExceedingMaxDelay()
  {
    final TaskGroupRelaunchBackoff lowest = newBackoff(0.0);
    final TaskGroupRelaunchBackoff highest = newBackoff(1.0);
    for (int i = 0; i < 30; i++) {
      lowest.recordFailure(GROUP_ID);
      highest.recordFailure(GROUP_ID);
    }

    Assert.assertEquals(200_000, getRemainingDelayMillis(lowest, GROUP_ID));
    Assert.assertEquals(TaskGroupRelaunchBackoff.MAX_DELAY_MILLIS, getRemainingDelayMillis(highest, GROUP_ID));
  }

  @Test
  public void testRemainingDelayDecreasesWithTime()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    backoff.recordFailure(GROUP_ID);

    now.addAndGet(400);
    Assert.assertEquals(600, getRemainingDelayMillis(backoff, GROUP_ID), 1.0);

    now.addAndGet(600);
    Assert.assertEquals(0, getRemainingDelayMillis(backoff, GROUP_ID));
    Assert.assertEquals(Map.of(), backoff.getRemainingDelaysMillis());
  }

  @Test
  public void testGroupsAreIndependent()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    backoff.recordFailure(GROUP_ID);

    Assert.assertTrue(getRemainingDelayMillis(backoff, GROUP_ID) > 0);
    Assert.assertEquals(0, getRemainingDelayMillis(backoff, GROUP_ID + 1));
  }

  @Test
  public void testResetStartsOverFromFirstFailure()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    backoff.recordFailure(GROUP_ID);

    backoff.reset(GROUP_ID);
    Assert.assertEquals(0, getRemainingDelayMillis(backoff, GROUP_ID));

    backoff.recordFailure(GROUP_ID);
    Assert.assertEquals(0, getRemainingDelayMillis(backoff, GROUP_ID));
  }

  @Test
  public void testClearResetsAllGroups()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    for (int groupId = 0; groupId < 3; groupId++) {
      backoff.recordFailure(groupId);
      backoff.recordFailure(groupId);
    }

    backoff.clear();

    Assert.assertEquals(Map.of(), backoff.getRemainingDelaysMillis());
  }

  @Test
  public void testHasRecoveredOnlyWithTaskStartedAfterLastFailure()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    final long failureMillis = now.get();

    // No failure, so nothing to recover from
    Assert.assertFalse(backoff.hasRecovered(GROUP_ID, DateTimes.utc(failureMillis - 1)));

    backoff.recordFailure(GROUP_ID);
    now.addAndGet(2 * TaskGroupRelaunchBackoff.HEALTHY_RUN_MILLIS);

    // A replica that was already running when the failure happened
    Assert.assertFalse(backoff.hasRecovered(GROUP_ID, DateTimes.utc(failureMillis - 1)));
    Assert.assertFalse(backoff.hasRecovered(GROUP_ID, null));

    final long startMillis = failureMillis + 1;
    now.set(startMillis + TaskGroupRelaunchBackoff.HEALTHY_RUN_MILLIS - 1);
    Assert.assertFalse(backoff.hasRecovered(GROUP_ID, DateTimes.utc(startMillis)));
    now.set(startMillis + TaskGroupRelaunchBackoff.HEALTHY_RUN_MILLIS);
    Assert.assertTrue(backoff.hasRecovered(GROUP_ID, DateTimes.utc(startMillis)));
    Assert.assertFalse(backoff.hasRecovered(GROUP_ID + 1, DateTimes.utc(startMillis)));
  }

  @Test
  public void testPollLaunchedTasksReturnsTasksUntilForgottenOrChecked()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordLaunch(GROUP_ID, List.of("task-a", "task-b"));
    backoff.recordLaunch(GROUP_ID + 1, List.of("task-c"));

    Assert.assertEquals(
        Map.of("task-a", GROUP_ID, "task-b", GROUP_ID, "task-c", GROUP_ID + 1),
        backoff.pollLaunchedTasks()
    );

    backoff.forgetLaunchedTask("task-a");
    for (int i = 1; i < TaskGroupRelaunchBackoff.MAX_STATUS_CHECKS; i++) {
      Assert.assertEquals(Map.of("task-b", GROUP_ID, "task-c", GROUP_ID + 1), backoff.pollLaunchedTasks());
    }
    Assert.assertEquals(Map.of(), backoff.pollLaunchedTasks());
  }

  @Test
  public void testResetAndClearForgetLaunchedTasks()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordLaunch(GROUP_ID, List.of("task-a"));
    backoff.recordLaunch(GROUP_ID + 1, List.of("task-b"));

    backoff.reset(GROUP_ID);
    Assert.assertEquals(Map.of("task-b", GROUP_ID + 1), backoff.pollLaunchedTasks());

    backoff.recordLaunch(GROUP_ID, List.of("task-c"));
    backoff.clear();
    Assert.assertEquals(Map.of(), backoff.pollLaunchedTasks());
  }

  @Test
  public void testForgetLaunchedTasksKeepsBackoff()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    backoff.recordFailure(GROUP_ID);
    backoff.recordLaunch(GROUP_ID, List.of("task-a"));
    backoff.recordLaunch(GROUP_ID + 1, List.of("task-b"));

    backoff.forgetLaunchedTasks(GROUP_ID);

    Assert.assertTrue(getRemainingDelayMillis(backoff, GROUP_ID) > 0);
    Assert.assertEquals(Map.of("task-b", GROUP_ID + 1), backoff.pollLaunchedTasks());
  }

  @Test
  public void testRemapTaskGroupsMovesBackoffWithPartitions()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(0);
    backoff.recordFailure(0);
    backoff.recordLaunch(0, List.of("task-a"));

    // The task count goes from 2 to 4: the partitions of group 0 go to groups 0 and 2, those of group 1 to 1 and 3
    backoff.remapTaskGroups(groupId -> Set.of(groupId, groupId + 2));

    Assert.assertEquals(Map.of(0, 1000L, 2, 1000L), backoff.getRemainingDelaysMillis());
    Assert.assertEquals(Map.of(), backoff.pollLaunchedTasks());

    // The new groups count failures on from the old group, independently of each other
    backoff.recordFailure(0);
    Assert.assertEquals(Map.of(0, 1600L, 2, 1000L), backoff.getRemainingDelaysMillis());
  }

  @Test
  public void testRemapTaskGroupsKeepsLongestBackoff()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(0);
    backoff.recordFailure(0);
    for (int i = 0; i < 3; i++) {
      backoff.recordFailure(1);
    }

    // The task count goes from 2 to 1
    backoff.remapTaskGroups(groupId -> Set.of(0));

    Assert.assertEquals(Map.of(0, 1600L), backoff.getRemainingDelaysMillis());
    backoff.recordFailure(0);
    Assert.assertEquals(2560, getRemainingDelayMillis(backoff, 0));
  }

  @Test
  public void testDisabledNeverDelays()
  {
    final TaskGroupRelaunchBackoff backoff =
        new TaskGroupRelaunchBackoff("supervisor", false, now::get, () -> NO_JITTER);
    for (int i = 0; i < 10; i++) {
      backoff.recordFailure(GROUP_ID);
    }
    backoff.recordLaunch(GROUP_ID, List.of("task-a"));

    Assert.assertFalse(backoff.isEnabled());
    Assert.assertEquals(Map.of(), backoff.getRemainingDelaysMillis());
    Assert.assertEquals(Map.of(), backoff.pollLaunchedTasks());
  }

  private TaskGroupRelaunchBackoff newBackoff(double random)
  {
    return new TaskGroupRelaunchBackoff("supervisor", true, now::get, () -> random);
  }

  private static long getRemainingDelayMillis(TaskGroupRelaunchBackoff backoff, int groupId)
  {
    return backoff.getRemainingDelaysMillis().getOrDefault(groupId, 0L);
  }
}
