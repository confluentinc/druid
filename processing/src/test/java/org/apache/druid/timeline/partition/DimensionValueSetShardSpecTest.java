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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DimensionValueSetShardSpecTest
{
  private static final String TENANT = "tenant";
  private static final String REGION = "region";

  private static final List<String> UNIVERSE =
      List.of("", "a", "aa", "ab", "b", "ba", "bb", "c", "lkc-1", "lkc-10", "lkc-2");

  private static final List<String> ENDPOINTS =
      List.of("", "a", "a0", "aa", "ab", "az", "b", "ba", "bb", "bz", "c", "lkc-1", "lkc-10", "lkc-2", "z");

  private static DimensionValueSetShardSpec spec(Map<String, List<String>> filters)
  {
    return new DimensionValueSetShardSpec(0, 1, filters);
  }

  private static RangeSet<String> points(String... values)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    for (String v : values) {
      rangeSet.add(Range.singleton(v));
    }
    return rangeSet;
  }

  private static Map<String, RangeSet<String>> domain(String dimension, String... values)
  {
    return ImmutableMap.of(dimension, points(values));
  }

  private static Map<String, RangeSet<String>> rangeFilter(String dimension, String lower, String upper)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed(lower, upper));
    return ImmutableMap.of(dimension, rangeSet);
  }

  /**
   * The query domain Druid builds for an {@code IS NULL} filter: a null match is encoded as the range {@code (-inf, "")}
   * (see e.g. {@code NullFilter#getDimensionRangeSet}).
   */
  private static Map<String, RangeSet<String>> nullDomain(String dimension)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.lessThan(""));
    return ImmutableMap.of(dimension, rangeSet);
  }

  @Test
  public void testNoFilters_alwaysTrue()
  {
    final DimensionValueSetShardSpec s = spec(Collections.emptyMap());
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertTrue(s.possibleInDomain(Collections.emptyMap()));
  }

  @Test
  public void testSingleFilter_matchingValue_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
  }

  @Test
  public void testSingleFilter_nonMatchingValue_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testSingleFilter_domainHasMultipleValues_matchIncluded_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a", "tenant_b")));
  }

  @Test
  public void testSingleFilter_domainHasMultipleValues_noMatch_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b", "tenant_c")));
  }

  @Test
  public void testMultipleAllowedValues_matchOne_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testMultipleAllowedValues_noMatch_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_c")));
  }

  @Test
  public void testDeclaredDimension_notInQueryDomain_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(Collections.emptyMap()));
  }

  @Test
  public void testDeclaredDimension_queryFiltersOnOtherDim_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain("region", "us-west")));
  }

  @Test
  public void testRangeFilter_onDeclaredDimension_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    // A range predicate (e.g. TENANT BETWEEN 'a' AND 'z') cannot be pruned against declared point values.
    Assert.assertTrue(s.possibleInDomain(rangeFilter(TENANT, "a", "z")));
  }

  @Test
  public void testMultipleDimensions_allMatch_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertTrue(s.possibleInDomain(ImmutableMap.of(
        TENANT, points("tenant_a"),
        "region", points("us-west")
    )));
  }

  @Test
  public void testMultipleDimensions_oneDimensionMismatches_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertFalse(s.possibleInDomain(ImmutableMap.of(
        TENANT, points("tenant_a"),
        "region", points("eu-east")
    )));
  }

  @Test
  public void testMultipleDimensions_onlyOneDimensionInDomain()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testGetDomainDimensions_returnsFilterKeys()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertTrue(s.getDomainDimensions().contains(TENANT));
    Assert.assertTrue(s.getDomainDimensions().contains("region"));
    Assert.assertEquals(2, s.getDomainDimensions().size());
  }

  @Test
  public void testGetDomainDimensions_emptyFilters_returnsEmpty()
  {
    Assert.assertTrue(spec(Collections.emptyMap()).getDomainDimensions().isEmpty());
  }

  @Test
  public void testGetType()
  {
    Assert.assertEquals(ShardSpec.Type.DIM_VALUE_SET, spec(Collections.emptyMap()).getType());
  }

  private static ObjectMapper newMapper()
  {
    return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  public void testJsonSerdeRoundTrip() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec original = new DimensionValueSetShardSpec(
        3,
        8,
        ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b"), "region", List.of("us-west"))
    );

    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), DimensionValueSetShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertEquals(original.getPartitionDimensionValues(), deserialized.getPartitionDimensionValues());
  }

  @Test
  public void testJsonSerdeContainsType() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec spec = new DimensionValueSetShardSpec(0, 1, ImmutableMap.of(TENANT, List.of("tenant_a")));
    final String json = mapper.writeValueAsString(spec);
    Assert.assertTrue(json.contains("\"type\":\"dim_value_set\""));
    Assert.assertTrue(json.contains("\"partitionDimensionValues\""));
  }

  @Test
  public void testJsonSerdeWithNullFilters() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec original = new DimensionValueSetShardSpec(0, 1, null);

    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), DimensionValueSetShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertTrue(deserialized.getPartitionDimensionValues().isEmpty());
  }

  @Test
  public void testEmptyStringValue_isDistinctFromNull()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
    // An IS NULL query (domain = (-inf, "")) must NOT match a segment that only declares the empty string.
    Assert.assertFalse(s.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testNullValue_matchesIsNullQueryOnly()
  {
    // A null/missing value is declared as a null list element.
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, Collections.singletonList(null)));
    Assert.assertTrue(s.possibleInDomain(nullDomain(TENANT)));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "")));
  }

  @Test
  public void testConcreteValueOnly_isPrunedForIsNullQuery()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testNullAndConcreteValues_matchBoth()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, Arrays.asList("tenant_a", null)));
    Assert.assertTrue(s.possibleInDomain(nullDomain(TENANT)));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testNullValue_jsonSerdeRoundTrip() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec original =
        new DimensionValueSetShardSpec(0, 1, ImmutableMap.of(TENANT, Arrays.asList("tenant_a", null)));

    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), DimensionValueSetShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertTrue(deserialized.getPartitionDimensionValues().get(TENANT).contains(null));
    Assert.assertTrue(deserialized.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testEmptyAllowedList_prunesEverything()
  {
    // An empty allowed list means no values were observed for the dimension, so any constraining query is pruned.
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of()));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
  }

  @Test
  public void testUnsortedValues_matchAndPrune()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_c", "tenant_a", "tenant_b")));
    Assert.assertFalse(s.hasSortedValues());
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_c")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_d")));
  }

  @Test
  public void testDuplicateValues_openLowerBoundStillMatchesLaterValue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("b", "b", "c")));
    Assert.assertFalse(s.hasSortedValues());
    Assert.assertTrue(s.possibleInDomain(rangeSet(TENANT, Range.openClosed("b", "c"))));
    Assert.assertFalse(s.possibleInDomain(rangeSet(TENANT, Range.open("b", "c"))));
  }

  @Test
  public void testRangeBoundTypes()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("b", "d")));
    Assert.assertTrue(s.hasSortedValues());
    Assert.assertTrue(s.possibleInDomain(rangeSet(TENANT, Range.closedOpen("b", "c"))));
    Assert.assertFalse(s.possibleInDomain(rangeSet(TENANT, Range.open("b", "d"))));
    Assert.assertTrue(s.possibleInDomain(rangeSet(TENANT, Range.openClosed("b", "d"))));
    Assert.assertTrue(s.possibleInDomain(rangeSet(TENANT, Range.atMost("b"))));
    Assert.assertFalse(s.possibleInDomain(rangeSet(TENANT, Range.lessThan("b"))));
    Assert.assertTrue(s.possibleInDomain(rangeSet(TENANT, Range.atLeast("d"))));
    Assert.assertFalse(s.possibleInDomain(rangeSet(TENANT, Range.greaterThan("d"))));
    Assert.assertTrue(s.possibleInDomain(rangeSet(TENANT, Range.all())));
  }

  @Test
  public void testMultipleDomainRanges_matchOnlyInLaterRange()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("m")));
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed("a", "c"));
    rangeSet.add(Range.closed("x", "z"));
    Assert.assertFalse(s.possibleInDomain(ImmutableMap.of(TENANT, rangeSet)));
    rangeSet.add(Range.singleton("m"));
    Assert.assertTrue(s.possibleInDomain(ImmutableMap.of(TENANT, rangeSet)));
  }

  @Test
  public void testLargeValueSet()
  {
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < 30_000; i++) {
      values.add("lkc-" + i);
    }
    Collections.sort(values);
    final DimensionValueSetShardSpec sorted = spec(ImmutableMap.of(TENANT, values));
    Assert.assertTrue(sorted.hasSortedValues());
    assertLargeValueSetPruning(sorted);

    final List<String> shuffled = new ArrayList<>(values);
    Collections.shuffle(shuffled, new Random(7));
    final DimensionValueSetShardSpec unsorted = spec(ImmutableMap.of(TENANT, shuffled));
    Assert.assertFalse(unsorted.hasSortedValues());
    assertLargeValueSetPruning(unsorted);
  }

  private static void assertLargeValueSetPruning(DimensionValueSetShardSpec s)
  {
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "lkc-0")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "lkc-29999")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "lkc-missing", "lkc-123")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "lkc-30000")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "lkc-missing", "other")));
  }

  @Test
  public void testManyQueryTenantsAgainstSmallSegment()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("lkc-1", "lkc-5", "lkc-9")));
    Assert.assertTrue(s.hasSortedValues());
    final String[] many = new String[1_000];
    for (int i = 0; i < many.length; i++) {
      many[i] = "lkc-x" + i;
    }
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, many)));
    many[500] = "lkc-5";
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, many)));
  }

  @Test
  public void testPublisherLayoutWithLeadingNull_usesSortedLookup()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, Arrays.asList(null, "", "tenant_a", "tenant_b")));
    Assert.assertTrue(s.hasSortedValues());
    Assert.assertTrue(s.possibleInDomain(nullDomain(TENANT)));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_b")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_c")));
  }

  @Test
  public void testNonPublisherLayouts_fallBackToScan()
  {
    Assert.assertFalse(spec(ImmutableMap.of(TENANT, Arrays.asList("tenant_a", null))).hasSortedValues());
    Assert.assertFalse(spec(ImmutableMap.of(TENANT, Arrays.asList(null, null))).hasSortedValues());
  }

  @Test
  public void testOneUnsortedDimension_fallsBackForAllDimensions()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("a", "b"), REGION, List.of("y", "x")));
    Assert.assertFalse(s.hasSortedValues());
    Assert.assertTrue(s.possibleInDomain(ImmutableMap.of(TENANT, points("b"), REGION, points("x"))));
    Assert.assertFalse(s.possibleInDomain(ImmutableMap.of(TENANT, points("b"), REGION, points("z"))));
  }

  @Test
  public void testJsonUnchangedAfterPruning() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec s =
        new DimensionValueSetShardSpec(0, 1, ImmutableMap.of(TENANT, Arrays.asList("tenant_b", null, "tenant_a")));
    final String before = mapper.writeValueAsString(s);
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertEquals(before, mapper.writeValueAsString(s));
    Assert.assertEquals(Arrays.asList("tenant_b", null, "tenant_a"), s.getPartitionDimensionValues().get(TENANT));
  }

  @Test
  public void testRandomizedEquivalenceWithLinearScan()
  {
    final Random random = new Random(42);
    for (int i = 0; i < 20_000; i++) {
      final Map<String, List<String>> values = new HashMap<>();
      values.put(TENANT, randomValues(random));
      if (random.nextBoolean()) {
        values.put(REGION, randomValues(random));
      }
      final Map<String, RangeSet<String>> domain = new HashMap<>();
      domain.put(TENANT, randomRangeSet(random));
      if (random.nextBoolean()) {
        domain.put(REGION, randomRangeSet(random));
      }
      final boolean expected = linearScan(values, domain);
      Assert.assertEquals("values=" + values + ", domain=" + domain, expected, spec(values).possibleInDomain(domain));

      final Map<String, List<String>> published = publisherLayout(values);
      final DimensionValueSetShardSpec publishedSpec = spec(published);
      Assert.assertTrue("values=" + published, publishedSpec.hasSortedValues());
      Assert.assertEquals("values=" + published + ", domain=" + domain, expected, publishedSpec.possibleInDomain(domain));
    }
  }

  private static Map<String, List<String>> publisherLayout(Map<String, List<String>> values)
  {
    final Map<String, List<String>> published = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : values.entrySet()) {
      final List<String> distinct = new ArrayList<>(new HashSet<>(entry.getValue()));
      distinct.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
      published.put(entry.getKey(), distinct);
    }
    return published;
  }

  private static Map<String, RangeSet<String>> rangeSet(String dimension, Range<String> range)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(range);
    return ImmutableMap.of(dimension, rangeSet);
  }

  private static List<String> randomValues(Random random)
  {
    final List<String> values = new ArrayList<>();
    final int size = random.nextInt(7);
    for (int i = 0; i < size; i++) {
      values.add(random.nextInt(10) == 0 ? null : UNIVERSE.get(random.nextInt(UNIVERSE.size())));
    }
    return values;
  }

  private static RangeSet<String> randomRangeSet(Random random)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    final int size = random.nextInt(4);
    for (int i = 0; i < size; i++) {
      rangeSet.add(randomRange(random));
    }
    return rangeSet;
  }

  private static Range<String> randomRange(Random random)
  {
    final String a = ENDPOINTS.get(random.nextInt(ENDPOINTS.size()));
    final String b = ENDPOINTS.get(random.nextInt(ENDPOINTS.size()));
    switch (random.nextInt(8)) {
      case 0:
        return Range.singleton(a);
      case 1:
        return Range.lessThan("");
      case 2:
        return Range.atLeast(a);
      case 3:
        return Range.greaterThan(a);
      case 4:
        return Range.atMost(a);
      case 5:
        return Range.lessThan(a);
      case 6:
        return Range.all();
      default:
        final int cmp = a.compareTo(b);
        if (cmp == 0) {
          return Range.singleton(a);
        }
        return Range.range(
            cmp < 0 ? a : b,
            random.nextBoolean() ? BoundType.OPEN : BoundType.CLOSED,
            cmp < 0 ? b : a,
            random.nextBoolean() ? BoundType.OPEN : BoundType.CLOSED
        );
    }
  }

  private static boolean linearScan(Map<String, List<String>> values, Map<String, RangeSet<String>> domain)
  {
    for (Map.Entry<String, List<String>> entry : values.entrySet()) {
      final RangeSet<String> rangeSet = domain.get(entry.getKey());
      if (rangeSet == null || rangeSet.isEmpty()) {
        continue;
      }
      boolean anyMatch = false;
      for (String value : entry.getValue()) {
        final Range<String> valueRange = value == null ? Range.lessThan("") : Range.singleton(value);
        if (!rangeSet.subRangeSet(valueRange).isEmpty()) {
          anyMatch = true;
          break;
        }
      }
      if (!anyMatch) {
        return false;
      }
    }
    return true;
  }
}
