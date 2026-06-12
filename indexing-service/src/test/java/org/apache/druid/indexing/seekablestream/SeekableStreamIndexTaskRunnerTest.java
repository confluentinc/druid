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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderator;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionValueSetShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@RunWith(MockitoJUnitRunner.class)
public class SeekableStreamIndexTaskRunnerTest
{
  @Mock
  private InputRow row;

  @Mock
  private SeekableStreamIndexTask task;

  @Test
  public void testWithinMinMaxTime()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    DateTime now = DateTimes.nowUtc();

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(120L);
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(DateTimes.nowUtc().plusHours(2));
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(DateTimes.nowUtc().minusHours(2));
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);

    TestasbleSeekableStreamIndexTaskRunner runner = new TestasbleSeekableStreamIndexTaskRunner(task, null,
                                                                                               LockGranularity.TIME_CHUNK);

    Mockito.when(row.getTimestamp()).thenReturn(now);
    Assert.assertTrue(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assert.assertFalse(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assert.assertFalse(runner.withinMinMaxRecordTime(row));
  }

  @Test
  public void testWithinMinMaxTimeNotPopulated()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    DateTime now = DateTimes.nowUtc();

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(null);
    // min max time not populated.
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(null);
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(null);
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);
    TestasbleSeekableStreamIndexTaskRunner runner = new TestasbleSeekableStreamIndexTaskRunner(task, null,
                                                                                               LockGranularity.TIME_CHUNK);

    Assert.assertTrue(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assert.assertTrue(runner.withinMinMaxRecordTime(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assert.assertTrue(runner.withinMinMaxRecordTime(row));
  }

  @Test
  public void testGetSupervisorId()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(null);
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);

    Mockito.when(task.getSupervisorId()).thenReturn("supervisorId");
    TestasbleSeekableStreamIndexTaskRunner runner = new TestasbleSeekableStreamIndexTaskRunner(task, null,
                                                                                               LockGranularity.TIME_CHUNK);
    Assert.assertEquals("supervisorId", runner.getSupervisorId());

    // Setup the task to return a RecordSupplier, StreamAppenderatorDriver, Appenderator
    final RecordSupplier<?, ?, ?> recordSupplier = Mockito.mock(RecordSupplier.class);
    Mockito.when(task.newTaskRecordSupplier(any()))
           .thenReturn(recordSupplier);

    final StreamAppenderator appenderator = Mockito.mock(StreamAppenderator.class);
    Mockito.when(task.newAppenderator(any(), any(), any(), any()))
           .thenReturn(appenderator);

    final List<DataSegment> segment = CreateDataSegments
        .ofDatasource(schema.getDataSource())
        .withNumPartitions(10)
        .withNumRows(1_000)
        .eachOfSizeInMb(500);
    final SegmentsAndCommitMetadata commitMetadata =
        new SegmentsAndCommitMetadata(segment, "offset-100").withWasPublished(true);

    final StreamAppenderatorDriver driver = Mockito.mock(StreamAppenderatorDriver.class);
    Mockito.when(task.newDriver(any(), any(), any()))
           .thenReturn(driver);
    // publishAndRegisterHandoff calls the 4-arg publish overload (with the shard-spec annotator function).
    Mockito.when(driver.publish(any(), any(), any(), any()))
           .thenReturn(Futures.immediateFuture(commitMetadata));
    Mockito.when(driver.registerHandoff(any()))
           .thenReturn(Futures.immediateFuture(commitMetadata));

    Mockito.doAnswer(invocation -> {
      final String metricName = invocation.getArgument(1);
      final Number value = invocation.getArgument(2);
      emitter.emit(ServiceMetricEvent.builder().setMetric(metricName, value).build("test", "localhost"));
      return null;
    }).when(task).emitMetric(any(), any(), any());

    runner.run(createTaskToolbox());
    emitter.verifyValue("ingest/segments/count", 10);
    emitter.verifyValue("ingest/rows/published", 10_000L);
  }

  @Test
  public void testAnnotateSegmentStampsDimensionValueSetShardSpecForObservedValues() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        Map.of("partition", "0"),
        Map.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();
    // Observe out of order; the published values must come back sorted.
    observe(runner, lookupKey, "tenant", "tenant_c", "tenant_a", "tenant_b");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(
        "A segment created during the current run with observed values should get a DimensionValueSetShardSpec",
        annotated.getShardSpec() instanceof DimensionValueSetShardSpec
    );
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    Assert.assertEquals(
        Arrays.asList("tenant_a", "tenant_b", "tenant_c"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * A segment that spans a task restart has incomplete observed values, so it must NOT declare any partition filters
   * (no pruning), to avoid wrongly pruning pre-restart rows. It is still stamped with an empty-filter
   * {@link DimensionValueSetShardSpec} (not a bare {@link NumberedShardSpec}) so that all segments in an interval keep a
   * uniform shard-spec class for {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper}, which
   * rejects a publish batch mixing shard-spec classes within an interval.
   */
  @Test
  public void testRestartSpannedSegmentGetsEmptyFilterDimensionValueSetShardSpec() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    // Post-restart, only tenant_c is observed; tenant_a/tenant_b live only in pre-restart hydrants.
    observe(runner, lookupKey, "tenant", "tenant_c");
    // The runner marks this segment as restored-from-disk (spans a restart).
    markRestartSpanned(runner, lookupKey);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(
        "A restart-spanned segment must be stamped with a DimensionValueSetShardSpec (class-uniform with freshly-stamped "
        + "segments in the same interval) so SegmentPublisherHelper does not reject the publish",
        annotated.getShardSpec() instanceof DimensionValueSetShardSpec
    );
    Assert.assertTrue(
        "Its filters must be empty (no pruning) so incompletely-observed pre-restart rows are never pruned away",
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
  }

  /**
   * A restart batch mixes a restart-spanned partition (empty-filter fallback) with a freshly-observed one in the same
   * interval. Both must keep a uniform shard-spec class so the publish isn't rejected.
   */
  @Test
  public void testRestartBatchMixingFallbackAndObservedSegmentsPublishesWithDimensionValueSetShardSpec()
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    // Two partitions in one interval: partition 0 was restored from disk across a restart, partition 1 created after.
    final List<DataSegment> sameIntervalPartitions = CreateDataSegments
        .ofDatasource(DATA_SOURCE)
        .startingAt("2025-01-01")
        .forIntervals(1, Granularities.DAY)
        .withNumPartitions(2)
        .eachOfSizeInMb(500);
    final DataSegment restartSpanned = sameIntervalPartitions.get(0);
    final DataSegment freshlyObserved = sameIntervalPartitions.get(1);

    markRestartSpanned(runner, restartSpanned.getId());
    observe(runner, restartSpanned.getId(), "tenant", "tenant_c");
    observe(runner, freshlyObserved.getId(), "tenant", "tenant_a");

    final DataSegment annotatedRestartSpanned = runner.annotateSegmentWithPartitionDimensionValues(restartSpanned);
    final DataSegment annotatedFreshlyObserved = runner.annotateSegmentWithPartitionDimensionValues(freshlyObserved);

    Assert.assertEquals(
        annotatedRestartSpanned.getShardSpec().getClass(),
        annotatedFreshlyObserved.getShardSpec().getClass()
    );
    Assert.assertTrue(annotatedRestartSpanned.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assert.assertTrue(
        ((DimensionValueSetShardSpec) annotatedRestartSpanned.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
    Assert.assertEquals(
        List.of("tenant_a"),
        ((DimensionValueSetShardSpec) annotatedFreshlyObserved.getShardSpec()).getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * A dimension that ingested a null/missing value declares null (as a null list element) alongside its non-null
   * values, so {@code IS NULL} queries are not pruned. Here tenant saw tenant_a and a null; region saw only us-west.
   */
  @Test
  public void testNullValuedDimensionDeclaresNullInPartitionDimensionValues() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant", "region")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    // tenant saw a non-null value and (in another row) a null/missing value; region only saw non-null values.
    observe(runner, lookupKey, "tenant", "tenant_a", null);
    observe(runner, lookupKey, "region", "us-west");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(
        annotated.getShardSpec() instanceof DimensionValueSetShardSpec
    );
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    // tenant declares both its non-null value AND null, so IS NULL queries are not pruned.
    Assert.assertEquals(
        Arrays.asList(null, "tenant_a"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
    Assert.assertEquals(
        ImmutableSet.of("us-west"),
        ImmutableSet.copyOf(shardSpec.getPartitionDimensionValues().get("region"))
    );
  }

  /**
   * A dimension that ingested only a null value declares {@code [null]} — pruned for concrete-value queries but never
   * for {@code IS NULL}.
   */
  @Test
  public void testOnlyNullValuedDimensionDeclaresNull() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    observe(runner, lookupKey, "tenant", (String) null);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    Assert.assertEquals(
        Collections.singletonList(null),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * Feature on, but a segment ingested no values for any tracked dimension (nothing recorded under its key). It still
   * gets an empty-filter {@link DimensionValueSetShardSpec} rather than being returned as a bare {@link NumberedShardSpec},
   * so it stays class-uniform with its interval siblings for
   * {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper}.
   */
  @Test
  public void testSegmentWithNoObservedValuesGetsEmptyFilterDimensionValueSetShardSpec() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    // No observe(...) call: nothing was recorded for this segment.
    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(createSingleSegment());

    Assert.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assert.assertTrue(
        "A segment with no observed values declares no filters (no pruning) but stays a DimensionValueSetShardSpec",
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
  }

  /**
   * Feature off (no streamingPartitionsSpec): the segment is returned completely unchanged, retaining its original
   * shard spec.
   */
  @Test
  public void testFeatureOffReturnsSegmentUnchanged() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec()).thenReturn(null);

    final DataSegment segment = createSingleSegment();
    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertSame("With the feature off the segment must be returned unchanged", segment, annotated);
  }

  private static DataSegment createSingleSegment()
  {
    return CreateDataSegments
        .ofDatasource(DATA_SOURCE)
        .startingAt("2025-01-01")
        .forIntervals(1, Granularities.DAY)
        .withNumPartitions(1)
        .eachOfSizeInMb(500)
        .get(0);
  }

  private static void observe(
      SeekableStreamIndexTaskRunner runner,
      SegmentId segmentId,
      String dimension,
      String... values
  )
  {
    for (String value : values) {
      runner.recordObservedDimensionValueForTest(segmentId, dimension, value);
    }
  }

  private static void markRestartSpanned(SeekableStreamIndexTaskRunner runner, SegmentId segmentId)
  {
    runner.markSegmentRestartSpannedForTest(segmentId);
  }

  private TaskToolbox createTaskToolbox()
  {
    public TestasbleSeekableStreamIndexTaskRunner(
        SeekableStreamIndexTask task,
        @Nullable InputRowParser parser,
        LockGranularity lockGranularityToUse
    )
    {
      super(task, parser, lockGranularityToUse);
    }

    @Override
    protected boolean isEndOfShard(Object seqNum)
    {
      return false;
    }

    @Nullable
    @Override
    protected TreeMap<Integer, Map> getCheckPointsFromContext(TaskToolbox toolbox, String checkpointsString)
    {
      return null;
    }

    @Override
    protected Object getNextStartOffset(Object sequenceNumber)
    {
      return null;
    }

    @Override
    protected SeekableStreamEndSequenceNumbers deserializePartitionsFromMetadata(ObjectMapper mapper, Object object)
    {
      return null;
    }

    @Override
    protected List<OrderedPartitionableRecord> getRecords(RecordSupplier recordSupplier, TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    protected SeekableStreamDataSourceMetadata createDataSourceMetadata(SeekableStreamSequenceNumbers partitions)
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber createSequenceNumber(Object sequenceNumber)
    {
      return null;
    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
      return false;
    }

    @Override
    protected TypeReference<List<SequenceMetadata>> getSequenceMetadataTypeReference()
    {
      return null;
    }

    @Override
    protected void possiblyResetDataSourceMetadata(TaskToolbox toolbox, RecordSupplier recordSupplier, Set assignment)
    {

    }
  }
}
