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

package org.apache.druid.indexing.common;

import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class TaskRealtimeMetricsMonitorBuilderTest
{
  private static final String SUPERVISOR_ID = "supervisor-1";

  @Mock(answer = Answers.RETURNS_MOCKS)
  private RowIngestionMeters rowIngestionMeters;
  @Mock
  private ServiceEmitter emitter;

  private SegmentGenerationMetrics segmentGenerationMetrics;
  private Map<String, ServiceMetricEvent> emittedEvents;

  @Before
  public void setUp()
  {
    emittedEvents = new HashMap<>();
    segmentGenerationMetrics = new SegmentGenerationMetrics();
    Mockito.doCallRealMethod().when(emitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito
        .doAnswer(invocation -> {
          ServiceMetricEvent e = invocation.getArgument(0);
          emittedEvents.put(e.getMetric(), e);
          return null;
        })
        .when(emitter).emit(ArgumentMatchers.any(Event.class));
  }

  @Test
  public void testBuildForStreamingTaskEmitsSupervisorId()
  {
    final SeekableStreamIndexTask<?, ?, ?> task = Mockito.mock(SeekableStreamIndexTask.class);
    Mockito.when(task.getDataSource()).thenReturn("wikipedia");
    Mockito.when(task.getId()).thenReturn("task-1");
    Mockito.when(task.getType()).thenReturn("index_kafka");
    Mockito.when(task.getGroupId()).thenReturn("index_kafka_wikipedia");
    Mockito.when(task.getSupervisorId()).thenReturn(SUPERVISOR_ID);

    final TaskRealtimeMetricsMonitor monitor =
        TaskRealtimeMetricsMonitorBuilder.build(task, segmentGenerationMetrics, rowIngestionMeters);
    monitor.doMonitor(emitter);

    Assert.assertFalse(emittedEvents.isEmpty());
    for (ServiceMetricEvent sme : emittedEvents.values()) {
      // A String[] dimension value is stored as a List by ServiceMetricEvent.
      Assert.assertEquals(
          Collections.singletonList(SUPERVISOR_ID),
          sme.getUserDims().get(DruidMetrics.SUPERVISOR_ID)
      );
    }
  }

  @Test
  public void testBuildForNonStreamingTaskDoesNotEmitSupervisorId()
  {
    final Task task = NoopTask.create();

    final TaskRealtimeMetricsMonitor monitor =
        TaskRealtimeMetricsMonitorBuilder.build(task, segmentGenerationMetrics, rowIngestionMeters);
    monitor.doMonitor(emitter);

    Assert.assertFalse(emittedEvents.isEmpty());
    for (ServiceMetricEvent sme : emittedEvents.values()) {
      Assert.assertFalse(sme.getUserDims().containsKey(DruidMetrics.SUPERVISOR_ID));
    }
  }
}
