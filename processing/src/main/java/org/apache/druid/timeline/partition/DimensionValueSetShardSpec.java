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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link NumberedShardSpec} that additionally declares, per dimension, the set of values a streaming segment
 * contains ({@link #partitionDimensionValues}), letting the broker prune segments whose values cannot match a query filter
 * before compaction. A dimension absent from {@link #partitionDimensionValues} is not pruned on.
 */
public class DimensionValueSetShardSpec extends NumberedShardSpec
{
  private static final Range<String> NULL_RANGE = Range.lessThan("");

  /**
   * Maps dimension name → exhaustive list of values that can appear in this shard for that dimension.
   * An absent dimension means "all values possible" (no pruning on that dimension).
   */
  private final Map<String, List<String>> partitionDimensionValues;

  private final List<String> domainDimensions;

  @Nullable
  private volatile Boolean valuesSortedAndDistinct;

  @JsonCreator
  public DimensionValueSetShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionDimensionValues") @Nullable Map<String, List<String>> partitionDimensionValues
  )
  {
    super(partitionNum, partitions);
    this.partitionDimensionValues = partitionDimensionValues == null ? Collections.emptyMap() : partitionDimensionValues;
    this.domainDimensions = ImmutableList.copyOf(this.partitionDimensionValues.keySet());
  }

  @JsonProperty("partitionDimensionValues")
  public Map<String, List<String>> getPartitionDimensionValues()
  {
    return partitionDimensionValues;
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return domainDimensions;
  }

  /**
   * Returns false only when the query filter explicitly constrains a dimension that this shard declares,
   * and none of this shard's allowed values for that dimension fall within the filter domain.
   *
   * <p>A null entry in a dimension's allowed-values list denotes a row whose value was null/missing. Druid encodes a
   * null match in the query domain as the range {@code (-inf, "")} (see e.g. {@code NullFilter}), so a declared null
   * is tested against the domain as {@link Range#lessThan} {@code ""} rather than as a point value. Every other value
   * (including the empty string {@code ""}) is tested as a singleton point, keeping null and {@code ""} distinct.
   *
   * @return true if segment needs to be considered for query, false if it can be pruned
   */
  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    if (partitionDimensionValues.isEmpty()) {
      return true;
    }

    final boolean sorted = hasSortedValues();
    for (Map.Entry<String, List<String>> entry : partitionDimensionValues.entrySet()) {
      final RangeSet<String> domainRangeSet = domain.get(entry.getKey());
      if (domainRangeSet == null || domainRangeSet.isEmpty()) {
        // Query doesn't constrain this dimension — cannot prune on it.
        continue;
      }
      final boolean anyMatch = sorted
                               ? anySortedValueInDomain(entry.getValue(), domainRangeSet)
                               : anyValueInDomain(entry.getValue(), domainRangeSet);
      if (!anyMatch) {
        return false;
      }
    }

    return true;
  }

  // Checked once per segment; the race is benign because every thread computes the same answer.
  @VisibleForTesting
  boolean hasSortedValues()
  {
    Boolean sorted = valuesSortedAndDistinct;
    if (sorted == null) {
      sorted = partitionDimensionValues.values().stream().allMatch(DimensionValueSetShardSpec::isSortedAndDistinct);
      valuesSortedAndDistinct = sorted;
    }
    return sorted;
  }

  // The publisher's layout: at most one leading null, then strictly ascending values.
  private static boolean isSortedAndDistinct(List<String> values)
  {
    final int start = firstNonNullIndex(values);
    for (int i = start; i < values.size(); i++) {
      final String value = values.get(i);
      if (value == null || (i > start && values.get(i - 1).compareTo(value) >= 0)) {
        return false;
      }
    }
    return true;
  }

  private static int firstNonNullIndex(List<String> values)
  {
    return !values.isEmpty() && values.get(0) == null ? 1 : 0;
  }

  private static boolean anySortedValueInDomain(List<String> values, RangeSet<String> domain)
  {
    final int start = firstNonNullIndex(values);
    if (start == 1 && domain.intersects(NULL_RANGE)) {
      return true;
    }
    // Loop over the smaller side: O(min(r, n) * log(max(r, n))).
    final Set<Range<String>> ranges = domain.asRanges();
    if (ranges.size() > values.size() - start) {
      for (int i = start; i < values.size(); i++) {
        if (domain.contains(values.get(i))) {
          return true;
        }
      }
      return false;
    }
    for (Range<String> range : ranges) {
      final int index = range.hasLowerBound()
                        ? ceilingIndex(values, start, range.lowerEndpoint(), range.lowerBoundType())
                        : start;
      if (index == values.size()) {
        // asRanges() is ascending, so no later range can contain a value either.
        break;
      }
      if (range.contains(values.get(index))) {
        return true;
      }
    }
    return false;
  }

  private static int ceilingIndex(List<String> values, int start, String endpoint, BoundType boundType)
  {
    int low = start;
    int high = values.size();
    while (low < high) {
      final int mid = (low + high) >>> 1;
      final int cmp = values.get(mid).compareTo(endpoint);
      if (cmp < 0 || (cmp == 0 && boundType == BoundType.OPEN)) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  private static boolean anyValueInDomain(List<String> values, RangeSet<String> domain)
  {
    for (String value : values) {
      // Null is represented in the domain as the range (-inf, ""); any other value as a singleton point.
      final Range<String> valueRange = value == null ? NULL_RANGE : Range.singleton(value);
      if (!domain.subRangeSet(valueRange).isEmpty()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getType()
  {
    return Type.DIM_VALUE_SET;
  }

  // NOTE (34.0.0-confluent backport): upstream also overrides ShardSpec#withPartitionNum and
  // ShardSpec#withCorePartitions here. Those base-class methods arrived with apache/druid#19059
  // (Overlord-based MSQ minor compaction), which is not part of this branch, and nothing on the
  // streaming publish path calls them -- IndexerSQLMetadataStorageCoordinator#getUpgradedSegmentShardSpec
  // constructs DimensionValueSetShardSpec directly. Re-add both overrides if #19059 is ever backported.

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DimensionValueSetShardSpec that = (DimensionValueSetShardSpec) o;
    return Objects.equals(partitionDimensionValues, that.partitionDimensionValues);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), partitionDimensionValues);
  }

  @Override
  public String toString()
  {
    return "DimensionValueSetShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getNumCorePartitions() +
           ", partitionDimensionValues=" + partitionDimensionValues +
           '}';
  }
}
