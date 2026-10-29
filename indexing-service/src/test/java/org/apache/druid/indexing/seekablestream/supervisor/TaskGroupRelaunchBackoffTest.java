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

    Assert.assertEquals(0, backoff.getRemainingDelayMillis(GROUP_ID));
  }

  @Test
  public void testDelayGrowsExponentiallyUpToMax()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);

    final long[] expectedDelays = {1000, 1600, 2560, 4096, 6553};
    for (long expectedDelay : expectedDelays) {
      backoff.recordFailure(GROUP_ID);
      Assert.assertEquals(expectedDelay, backoff.getRemainingDelayMillis(GROUP_ID), 1.0);
    }

    for (int i = 0; i < 20; i++) {
      backoff.recordFailure(GROUP_ID);
    }
    Assert.assertEquals(TaskGroupRelaunchBackoff.MAX_DELAY_MILLIS, backoff.getRemainingDelayMillis(GROUP_ID));
  }

  @Test
  public void testJitterBounds()
  {
    final TaskGroupRelaunchBackoff lowest = newBackoff(0.0);
    lowest.recordFailure(GROUP_ID);
    lowest.recordFailure(GROUP_ID);
    Assert.assertEquals(800, lowest.getRemainingDelayMillis(GROUP_ID), 1.0);

    final TaskGroupRelaunchBackoff highest = newBackoff(1.0);
    highest.recordFailure(GROUP_ID);
    highest.recordFailure(GROUP_ID);
    Assert.assertEquals(1200, highest.getRemainingDelayMillis(GROUP_ID), 1.0);
  }

  @Test
  public void testJitterDoesNotExceedMaxDelay()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(1.0);
    for (int i = 0; i < 30; i++) {
      backoff.recordFailure(GROUP_ID);
    }

    Assert.assertEquals(TaskGroupRelaunchBackoff.MAX_DELAY_MILLIS, backoff.getRemainingDelayMillis(GROUP_ID));
  }

  @Test
  public void testRemainingDelayDecreasesWithTime()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    backoff.recordFailure(GROUP_ID);

    now.addAndGet(400);
    Assert.assertEquals(600, backoff.getRemainingDelayMillis(GROUP_ID), 1.0);

    now.addAndGet(600);
    Assert.assertEquals(0, backoff.getRemainingDelayMillis(GROUP_ID));
  }

  @Test
  public void testGroupsAreIndependent()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    backoff.recordFailure(GROUP_ID);

    Assert.assertTrue(backoff.getRemainingDelayMillis(GROUP_ID) > 0);
    Assert.assertEquals(0, backoff.getRemainingDelayMillis(GROUP_ID + 1));
  }

  @Test
  public void testResetStartsOverFromFirstFailure()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    backoff.recordFailure(GROUP_ID);

    backoff.reset(GROUP_ID);
    Assert.assertEquals(0, backoff.getRemainingDelayMillis(GROUP_ID));

    backoff.recordFailure(GROUP_ID);
    Assert.assertEquals(0, backoff.getRemainingDelayMillis(GROUP_ID));
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

    for (int groupId = 0; groupId < 3; groupId++) {
      Assert.assertEquals(0, backoff.getRemainingDelayMillis(groupId));
    }
  }

  @Test
  public void testIsHealthy()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);

    Assert.assertFalse(backoff.isHealthy(null));
    Assert.assertFalse(backoff.isHealthy(DateTimes.utc(now.get() - TaskGroupRelaunchBackoff.HEALTHY_RUN_MILLIS + 1)));
    Assert.assertTrue(backoff.isHealthy(DateTimes.utc(now.get() - TaskGroupRelaunchBackoff.HEALTHY_RUN_MILLIS)));
  }

  @Test
  public void testRemoveLaunchedTasksReturnsOnlyInactiveTasks()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordLaunch(GROUP_ID, List.of("task-a", "task-b"));
    backoff.recordLaunch(GROUP_ID + 1, List.of("task-c"));

    Assert.assertEquals(
        Map.of("task-a", GROUP_ID, "task-c", GROUP_ID + 1),
        backoff.removeLaunchedTasks(Set.of("task-b"))
    );
    Assert.assertEquals(Map.of(), backoff.removeLaunchedTasks(Set.of()));
  }

  @Test
  public void testResetAndClearForgetLaunchedTasks()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordLaunch(GROUP_ID, List.of("task-a"));
    backoff.recordLaunch(GROUP_ID + 1, List.of("task-b"));

    backoff.reset(GROUP_ID);
    Assert.assertEquals(Map.of("task-b", GROUP_ID + 1), backoff.removeLaunchedTasks(Set.of()));

    backoff.recordLaunch(GROUP_ID, List.of("task-c"));
    backoff.clear();
    Assert.assertEquals(Map.of(), backoff.removeLaunchedTasks(Set.of()));
  }

  @Test
  public void testHasGroupsInBackoffAfterSecondFailure()
  {
    final TaskGroupRelaunchBackoff backoff = newBackoff(NO_JITTER);
    backoff.recordFailure(GROUP_ID);
    Assert.assertFalse(backoff.hasGroupsInBackoff());

    backoff.recordFailure(GROUP_ID);
    Assert.assertTrue(backoff.hasGroupsInBackoff());

    now.addAndGet(TaskGroupRelaunchBackoff.MAX_DELAY_MILLIS);
    Assert.assertTrue(backoff.hasGroupsInBackoff());

    backoff.reset(GROUP_ID);
    Assert.assertFalse(backoff.hasGroupsInBackoff());
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

    Assert.assertEquals(0, backoff.getRemainingDelayMillis(GROUP_ID));
    Assert.assertFalse(backoff.hasGroupsInBackoff());
    Assert.assertEquals(Map.of(), backoff.removeLaunchedTasks(Set.of()));
  }

  private TaskGroupRelaunchBackoff newBackoff(double random)
  {
    return new TaskGroupRelaunchBackoff("supervisor", true, now::get, () -> random);
  }
}
