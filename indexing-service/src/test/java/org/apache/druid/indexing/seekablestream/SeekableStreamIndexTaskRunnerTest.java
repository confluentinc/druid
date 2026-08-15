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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
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
  }

  // ------------------------------------------------------------------------------------------------
  // streamingPartitionsSpec / DimensionValueSetShardSpec stamping (apache/druid#19571 and #19596).
  //
  // Backport note: upstream covers this in the same test class, but its versions of these tests are built on
  // scaffolding (createTaskToolbox/TestSeekableStreamIndexTaskRunner) that post-dates 34.0.0. These are the same
  // assertions rewritten against this branch's existing TestasbleSeekableStreamIndexTaskRunner mocks, which is all
  // annotateSegmentWithPartitionDimensionValues() needs.
  // ------------------------------------------------------------------------------------------------

  /**
   * Builds a runner whose tuningConfig reports the given spec. Stubs are lenient because the class-level
   * MockitoJUnitRunner is strict and not every stub is exercised by every test below.
   */
  private TestasbleSeekableStreamIndexTaskRunner createRunner(@Nullable StreamingPartitionsSpec partitionsSpec)
  {
    final DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(new DimensionsSpec(Arrays.asList(
                      new StringDimensionSchema("tenant"),
                      new StringDimensionSchema("region")
                  )))
                  .withGranularity(new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null))
                  .build();

    final SeekableStreamIndexTaskTuningConfig tuningConfig =
        Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    final SeekableStreamIndexTaskIOConfig<String, String> ioConfig =
        Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    final SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers =
        Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    final SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers =
        Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    Mockito.lenient().when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(null);
    Mockito.lenient().when(ioConfig.getInputFormat())
           .thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.lenient().when(ioConfig.getStartSequenceNumbers()).thenReturn(startSequenceNumbers);
    Mockito.lenient().when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);
    Mockito.lenient().when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.lenient().when(startSequenceNumbers.getStream()).thenReturn("test");

    Mockito.lenient().when(tuningConfig.getStreamingPartitionsSpec()).thenReturn(partitionsSpec);
    Mockito.lenient().when(task.getDataSchema()).thenReturn(schema);
    Mockito.lenient().when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.lenient().when(task.getTuningConfig()).thenReturn(tuningConfig);

    return new TestasbleSeekableStreamIndexTaskRunner(task, null, LockGranularity.TIME_CHUNK);
  }

  private static DataSegment createSegment(int partitionNum, int numCorePartitions)
  {
    return DataSegment.builder()
                      .dataSource("datasource")
                      .interval(Intervals.of("2024-01-01/2024-01-02"))
                      .version("v1")
                      .shardSpec(new NumberedShardSpec(partitionNum, numCorePartitions))
                      .size(1L)
                      .build();
  }

  private static DataSegment createSingleSegment()
  {
    return createSegment(0, 1);
  }

  private static void observe(
      TestasbleSeekableStreamIndexTaskRunner runner,
      SegmentId segmentId,
      String dimension,
      String... values
  )
  {
    for (String value : values) {
      runner.recordObservedDimensionValueForTest(segmentId, dimension, value);
    }
  }

  @Test
  public void testAnnotateSegmentStampsDimensionValueSetShardSpecForObservedValues()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    // Observe out of order; the published values must come back sorted.
    observe(runner, segment.getId(), "tenant", "tenant_c", "tenant_a", "tenant_b");

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
    // Partitioning identity must be carried over from the original shard spec.
    Assert.assertEquals(0, shardSpec.getPartitionNum());
    Assert.assertEquals(1, shardSpec.getNumCorePartitions());
  }

  /**
   * A segment spanning a task restart has incomplete observed values, so it must not declare any filters (no pruning),
   * but is still stamped with an empty-filter DimensionValueSetShardSpec so all segments in an interval keep a uniform
   * shard-spec class for SegmentPublisherHelper.
   */
  @Test
  public void testRestartSpannedSegmentGetsEmptyFilterDimensionValueSetShardSpec()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    observe(runner, segment.getId(), "tenant", "tenant_a");
    runner.markSegmentRestartSpannedForTest(segment.getId());

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assert.assertTrue(
        "restart-spanned segments must not declare partition filters",
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
  }

  @Test
  public void testRestartBatchMixingFallbackAndObservedSegmentsPublishesWithDimensionValueSetShardSpec()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment restartSpanned = createSegment(0, 2);
    final DataSegment observedOnly = createSegment(1, 2);
    observe(runner, restartSpanned.getId(), "tenant", "tenant_a");
    runner.markSegmentRestartSpannedForTest(restartSpanned.getId());
    observe(runner, observedOnly.getId(), "tenant", "tenant_b");

    final DataSegment annotatedSpanned = runner.annotateSegmentWithPartitionDimensionValues(restartSpanned);
    final DataSegment annotatedObserved = runner.annotateSegmentWithPartitionDimensionValues(observedOnly);

    // Both must be the same shard-spec class, or SegmentPublisherHelper rejects the batch.
    Assert.assertTrue(annotatedSpanned.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assert.assertTrue(annotatedObserved.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assert.assertTrue(
        ((DimensionValueSetShardSpec) annotatedSpanned.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
    Assert.assertEquals(
        List.of("tenant_b"),
        ((DimensionValueSetShardSpec) annotatedObserved.getShardSpec()).getPartitionDimensionValues().get("tenant")
    );
  }

  @Test
  public void testNullValuedDimensionDeclaresNullInPartitionDimensionValues()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    runner.recordObservedDimensionValueForTest(segment.getId(), "tenant", "tenant_a");
    runner.recordObservedDimensionValueForTest(segment.getId(), "tenant", null);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();

    // null is carried through (distinct from "") and sorts first, so IS NULL queries are not pruned away.
    Assert.assertEquals(
        Arrays.asList(null, "tenant_a"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  @Test
  public void testOnlyNullValuedDimensionDeclaresNull()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    runner.recordObservedDimensionValueForTest(segment.getId(), "tenant", null);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();

    Assert.assertEquals(
        Collections.singletonList(null),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  @Test
  public void testSegmentWithNoObservedValuesGetsEmptyFilterDimensionValueSetShardSpec()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant")));

    // Nothing observed for this segment at all.
    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(createSingleSegment());

    Assert.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assert.assertTrue(
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
  }

  @Test
  public void testFeatureOffReturnsSegmentUnchanged()
  {
    final DataSegment segment = createSingleSegment();

    // Spec absent entirely.
    Assert.assertSame(segment, createRunner(null).annotateSegmentWithPartitionDimensionValues(segment));
    // Spec present but with no partitionDimensions.
    Assert.assertSame(
        segment,
        createRunner(new StreamingPartitionsSpec(List.of())).annotateSegmentWithPartitionDimensionValues(segment)
    );
    Assert.assertTrue(segment.getShardSpec() instanceof NumberedShardSpec);
  }

  @Test
  public void testCapAtBoundaryStampsValuesNormally()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant"), 3));

    final DataSegment segment = createSingleSegment();
    observe(runner, segment.getId(), "tenant", "a", "b", "c");

    final DimensionValueSetShardSpec shardSpec =
        (DimensionValueSetShardSpec) runner.annotateSegmentWithPartitionDimensionValues(segment).getShardSpec();

    // Exactly at the cap is still stamped; only strictly-greater is dropped.
    Assert.assertEquals(Arrays.asList("a", "b", "c"), shardSpec.getPartitionDimensionValues().get("tenant"));
  }

  @Test
  public void testCapExceededOmitsDimensionFromFilterMap()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant"), 2));

    final DataSegment segment = createSingleSegment();
    observe(runner, segment.getId(), "tenant", "a", "b", "c");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();

    // Still a DimensionValueSetShardSpec (class-uniformity), but the over-cap dimension is absent, which
    // possibleInDomain() treats as unconstrained -> no pruning on that dimension for this segment.
    Assert.assertFalse(shardSpec.getPartitionDimensionValues().containsKey("tenant"));
    Assert.assertTrue(shardSpec.getPartitionDimensionValues().isEmpty());
  }

  @Test
  public void testCapEnforcedPerDimensionIndependently()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(Arrays.asList("tenant", "region"), 2));

    final DataSegment segment = createSingleSegment();
    observe(runner, segment.getId(), "tenant", "a", "b", "c");   // over cap
    observe(runner, segment.getId(), "region", "us", "eu");      // at cap

    final DimensionValueSetShardSpec shardSpec =
        (DimensionValueSetShardSpec) runner.annotateSegmentWithPartitionDimensionValues(segment).getShardSpec();

    Assert.assertFalse(shardSpec.getPartitionDimensionValues().containsKey("tenant"));
    Assert.assertEquals(Arrays.asList("eu", "us"), shardSpec.getPartitionDimensionValues().get("region"));
  }

  @Test
  public void testNullCountsTowardCap()
  {
    final TestasbleSeekableStreamIndexTaskRunner runner =
        createRunner(new StreamingPartitionsSpec(List.of("tenant"), 2));

    final DataSegment segment = createSingleSegment();
    runner.recordObservedDimensionValueForTest(segment.getId(), "tenant", "a");
    runner.recordObservedDimensionValueForTest(segment.getId(), "tenant", "b");
    runner.recordObservedDimensionValueForTest(segment.getId(), "tenant", null);

    final DimensionValueSetShardSpec shardSpec =
        (DimensionValueSetShardSpec) runner.annotateSegmentWithPartitionDimensionValues(segment).getShardSpec();

    // 3 distinct observed values (including null) against a cap of 2 -> dimension omitted.
    Assert.assertFalse(shardSpec.getPartitionDimensionValues().containsKey("tenant"));
  }

  static class TestasbleSeekableStreamIndexTaskRunner extends SeekableStreamIndexTaskRunner
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
