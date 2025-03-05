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
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class killSegmentsMaxIntervalTest {

    private static final int MAX_SEGMENTS_TO_KILL = 10;
    private static final Period MAX_KILL_INTERVAL = Period.days(30);
    private static final Duration COORDINATOR_KILL_PERIOD = Duration.standardMinutes(2);
    private static final Duration DURATION_TO_RETAIN = Duration.standardDays(1);
    private static final Duration INDEXING_PERIOD = Duration.standardMinutes(1);
    private static final String DS1 = "DS1";
    private static final String VERSION = "v1";
    private static final DateTime NOW = DateTimes.nowUtc();
    private static final Interval FIFTEEN_DAY_OLD = new Interval(Period.days(1), NOW.minusDays(15));
    private static final Interval DAY_OLD = new Interval(Period.days(1), NOW.minusDays(1));


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

    private DataSegment yearOldSegment;
    private DataSegment monthOldSegment;
    private DataSegment dayOldSegment;
    private DataSegment fifteenDayOldSegment;
    private DataSegment hourOldSegment;
    private DataSegment nextDaySegment;
    private DataSegment nextMonthSegment;

    private KillUnusedSegments target;

    @Before
    public void setup() {
        Mockito.doReturn(coordinatorDynamicConfig).when(params).getCoordinatorDynamicConfig();
        Mockito.doReturn(COORDINATOR_KILL_PERIOD).when(config).getCoordinatorKillPeriod();
        Mockito.doReturn(DURATION_TO_RETAIN).when(config).getCoordinatorKillDurationToRetain();
        Mockito.doReturn(INDEXING_PERIOD).when(config).getCoordinatorIndexingPeriod();
        Mockito.doReturn(MAX_SEGMENTS_TO_KILL).when(config).getCoordinatorKillMaxSegments();
        Mockito.doReturn(MAX_KILL_INTERVAL).when(config).getCoordinatorKillMaxInterval();


        Mockito.doReturn(Collections.singleton("DS1"))
                .when(coordinatorDynamicConfig).getSpecificDataSourcesToKillUnusedSegmentsIn();

        final DateTime now = DateTimes.nowUtc();

        yearOldSegment = createSegmentWithEnd(now.minusDays(365));
        monthOldSegment = createSegmentWithEnd(now.minusDays(30));
        fifteenDayOldSegment = createSegmentWithEnd(now.minusDays(15));
        dayOldSegment = createSegmentWithEnd(now.minusDays(1));
        hourOldSegment = createSegmentWithEnd(now.minusHours(1));
        nextDaySegment = createSegmentWithEnd(now.plusDays(1));
        nextMonthSegment = createSegmentWithEnd(now.plusDays(30));

        final List<DataSegment> unusedSegments = ImmutableList.of(
                yearOldSegment,
                monthOldSegment,
                fifteenDayOldSegment,
                dayOldSegment,
                hourOldSegment,
                nextDaySegment,
                nextMonthSegment
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
    public void testMaxIntervalToKillWithMinStartTime() {
        // Set up the configuration to have maxIntervalToKill less than retainDuration
        Mockito.doReturn(Duration.standardDays(30)).when(config).getCoordinatorKillDurationToRetain();
        Mockito.doReturn(Period.days(10)).when(config).getCoordinatorKillMaxInterval();
        target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);

        // Create segments with specific intervals
        DateTime now = DateTimes.nowUtc();
        DataSegment segmentWithinMaxInterval = createSegmentWithEnd(now.minusDays(5));
        DataSegment segmentOutsideMaxInterval = createSegmentWithEnd(now.minusDays(20));

        List<DataSegment> unusedSegments = ImmutableList.of(segmentWithinMaxInterval, segmentOutsideMaxInterval);

        // Mock the behavior of segmentsMetadataManager to return these segments
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

        target.datasourceToLastKillIntervalEnd.put("DS1", segmentWithinMaxInterval.getInterval().getEnd());
        // Run the KillUnusedSegments duty for the first time
        target.run(params);


        // Verify that only the segment within the maxIntervalToKill range is considered for killing
        Interval expectedKillInterval = segmentWithinMaxInterval.getInterval();
        Mockito.verify(indexingServiceClient, Mockito.times(1)).killUnusedSegments(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq("DS1"),
                ArgumentMatchers.eq(expectedKillInterval)
        );

        // Initialize minStartTime to the end of the first killed segment

        // Run the KillUnusedSegments duty again
        target.run(params);


        // Verify that the segment outside the maxIntervalToKill range is not considered for killing
        Mockito.verify(indexingServiceClient, Mockito.never()).killUnusedSegments(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.eq("DS1"),
                ArgumentMatchers.eq(segmentOutsideMaxInterval.getInterval())
        );
    }

    @Test
    public void testRestrictKillQueryToMaxInterval()
    {
        Mockito.doReturn(Duration.standardHours(6)).when(config).getCoordinatorKillDurationToRetain();
        Mockito.doReturn(Period.days(20)).when(config).getCoordinatorKillMaxInterval();
        Mockito.doReturn(2).when(config).getCoordinatorKillMaxSegments();

        target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);
        target.run(params);

        runAndVerifyKillInterval(new Interval(yearOldSegment.getInterval().getStart(), monthOldSegment.getInterval().getEnd()));

        target = new KillUnusedSegments(segmentsMetadataManager, indexingServiceClient, config);

        target.run(params);

        runAndVerifyKillInterval(fifteenDayOldSegment.getInterval());

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