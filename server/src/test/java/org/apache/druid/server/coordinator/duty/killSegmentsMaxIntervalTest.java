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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class killSegmentsMaxIntervalTest
{

  private static final int MAX_SEGMENTS_TO_KILL = 10;
  private static final Period MAX_KILL_INTERVAL = Period.days(30);
  private static final Duration COORDINATOR_KILL_PERIOD = Duration.standardMinutes(2);
  private static final Duration DURATION_TO_RETAIN = Duration.standardDays(1);
  private static final Duration INDEXING_PERIOD = Duration.standardMinutes(1);

  @Mock
  private SegmentsMetadataManager segmentsMetadataManager;
  @Mock
  private IndexingServiceClient indexingServiceClient;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private DruidCoordinatorConfig config;

  @Mock
  private DruidCoordinatorRuntimeParams params;
  @Mock
  private CoordinatorDynamicConfig coordinatorDynamicConfig;

  private DataSegment monthOldSegment;
  private DataSegment fivedayOldSegment;
  private DataSegment fifteenDayOldSegment;

  private KillUnusedSegments target;

  @Before
  public void setup()
  {
    Mockito.doReturn(coordinatorDynamicConfig).when(params).getCoordinatorDynamicConfig();
    Mockito.doReturn(COORDINATOR_KILL_PERIOD).when(config).getCoordinatorKillPeriod();
    Mockito.doReturn(DURATION_TO_RETAIN).when(config).getCoordinatorKillDurationToRetain();
    Mockito.doReturn(INDEXING_PERIOD).when(config).getCoordinatorIndexingPeriod();
    Mockito.doReturn(MAX_SEGMENTS_TO_KILL).when(config).getCoordinatorKillMaxSegments();
    Mockito.doReturn(MAX_KILL_INTERVAL).when(config).getCoordinatorKillMaxInterval();


    Mockito.doReturn(Collections.singleton("DS1"))
            .when(coordinatorDynamicConfig).getSpecificDataSourcesToKillUnusedSegmentsIn();

    final DateTime now = DateTimes.nowUtc();

    monthOldSegment = createSegmentWithEnd(now.minusDays(30));
    fifteenDayOldSegment = createSegmentWithEnd(now.minusDays(15));
    fivedayOldSegment = createSegmentWithEnd(now.minusDays(5));

    final List<DataSegment> unusedSegments = ImmutableList.of(
            fifteenDayOldSegment,
            fivedayOldSegment
    );

    Mockito.when(
        segmentsMetadataManager.getUnusedSegmentIntervals(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.any(),
                ArgumentMatchers.anyInt()
        )
    ).thenAnswer(invocation -> {
      DateTime maxEndTime = invocation.getArgument(1);
      List<Interval> unusedIntervals =
               unusedSegments.stream()
                .map(DataSegment::getInterval)
                .filter(i -> i.getEnd().isBefore(maxEndTime))
                .collect(Collectors.toList());
      int limit = invocation.getArgument(2);
      return unusedIntervals.size() <= limit ? unusedIntervals : unusedIntervals.subList(0, limit);
    });

    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);
  }

  @Test
  public void testRestrictKillQueryToMaxInterval()
  {
    Mockito.doReturn(Duration.standardHours(6)).when(config).getCoordinatorKillDurationToRetain();
    Mockito.doReturn(Period.days(20)).when(config).getCoordinatorKillMaxInterval();
    Mockito.doReturn(2).when(config).getCoordinatorKillMaxSegments();

    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);
    target.datasourceToLastKillIntervalEnd.put("DS1", monthOldSegment.getInterval().getEnd());

    target.run(params);

    runAndVerifyKillInterval(fifteenDayOldSegment.getInterval());
  }

  @Test
  public void testDurationToRetainOverridesMaxKillInterval()
  {
    Mockito.doReturn(Period.days(3).toStandardDuration()).when(config).getCoordinatorKillDurationToRetain();
    Mockito.doReturn(Period.days(28)).when(config).getCoordinatorKillMaxInterval();

    target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);
    target.datasourceToLastKillIntervalEnd.put("DS1", monthOldSegment.getInterval().getEnd());

    target.run(params);

    runAndVerifyKillInterval(new Interval(fifteenDayOldSegment.getInterval().getStart(), fivedayOldSegment.getInterval().getEnd()));
  }

  private void runAndVerifyKillInterval(Interval expectedKillInterval)
  {
    target.run(params);
    Mockito.verify(indexingServiceClient, Mockito.times(1)).killUnusedSegments(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.eq("DS1"),
            ArgumentMatchers.eq(expectedKillInterval)
    );
  }

  private DataSegment createSegmentWithEnd(DateTime endTime)
  {
    return new DataSegment(
            "DS1",
            new Interval(Period.days(1), endTime),
            DateTimes.nowUtc().toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            1,
            0
    );
  }
}
