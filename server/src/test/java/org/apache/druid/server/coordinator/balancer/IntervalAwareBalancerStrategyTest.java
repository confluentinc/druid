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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class IntervalAwareBalancerStrategyTest
{
  private static final String DS_WIKI = "wiki";
  private static final String DS_KOALA = "koala";
  private static final String MINUTE_START = "2024-01-01T00:00:00.000Z";

  private IntervalAwareBalancerStrategy strategy;
  private int uniqueServerId;

  @Before
  public void setUp()
  {
    // Default tests use per-datasource scoping; the cross-datasource behaviour
    // is covered explicitly in the dedicated tests below.
    strategy = new IntervalAwareBalancerStrategy(true);
    uniqueServerId = 0;
  }

  @Test
  public void testLeastLoadedServerForIntervalIsPreferred()
  {
    // Segments for the target 1-minute interval
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 6);

    // serverA already holds 3 of them, serverB holds 1, serverC holds none
    final ServerHolder serverA = createServerWith(intervalSegments.subList(0, 3));
    final ServerHolder serverB = createServerWith(intervalSegments.subList(3, 4));
    final ServerHolder serverC = createServerWith(new ArrayList<>());

    final DataSegment newSegment = intervalSegments.get(5);
    final Iterator<ServerHolder> ordered =
        strategy.findServersToLoadSegment(newSegment, Arrays.asList(serverA, serverB, serverC));

    // Emptiest server for this interval comes first, fullest last
    Assert.assertSame(serverC, ordered.next());
    Assert.assertSame(serverB, ordered.next());
    Assert.assertSame(serverA, ordered.next());
    Assert.assertFalse(ordered.hasNext());
  }

  @Test
  public void testPerDatasourceCountIgnoresOtherDatasources()
  {
    final IntervalAwareBalancerStrategy perDsStrategy = new IntervalAwareBalancerStrategy(true);

    final List<DataSegment> wikiSegments = minuteSegments(DS_WIKI, 3);
    final List<DataSegment> koalaSegments = minuteSegments(DS_KOALA, 3);

    // serverA holds 2 koala + 0 wiki (wiki count = 0 for the interval)
    // serverB holds 2 wiki (wiki count = 2 for the interval)
    final ServerHolder serverA = createServerWith(koalaSegments.subList(0, 2));
    final ServerHolder serverB = createServerWith(wikiSegments.subList(0, 2));

    // A new wiki segment should prefer serverA, because only the wiki count
    // matters in per-datasource mode (serverA has 0 wiki despite holding koala).
    final DataSegment newWiki = wikiSegments.get(2);
    final Iterator<ServerHolder> ordered =
        perDsStrategy.findServersToLoadSegment(newWiki, Arrays.asList(serverA, serverB));

    Assert.assertSame(serverA, ordered.next());
    Assert.assertSame(serverB, ordered.next());
  }

  @Test
  public void testTotalCountIsAcrossAllDatasources()
  {
    final IntervalAwareBalancerStrategy totalStrategy = new IntervalAwareBalancerStrategy(false);

    final List<DataSegment> wikiSegments = minuteSegments(DS_WIKI, 3);
    final List<DataSegment> koalaSegments = minuteSegments(DS_KOALA, 3);

    // serverA holds 2 wiki + 1 koala (total 3 for the interval)
    // serverB holds 1 wiki only (total 1 for the interval)
    final List<DataSegment> serverASegments = new ArrayList<>(wikiSegments.subList(0, 2));
    serverASegments.add(koalaSegments.get(0));
    final ServerHolder serverA = createServerWith(serverASegments);
    final ServerHolder serverB = createServerWith(wikiSegments.subList(2, 3));

    // A new koala segment should prefer serverB, because in total mode the count
    // is across all datasources, not just koala.
    final DataSegment newKoala = koalaSegments.get(1);
    final Iterator<ServerHolder> ordered =
        totalStrategy.findServersToLoadSegment(newKoala, Arrays.asList(serverA, serverB));

    Assert.assertSame(serverB, ordered.next());
    Assert.assertSame(serverA, ordered.next());
  }

  @Test
  public void testMovePrefersEmptierServerAndAvoidsOscillation()
  {
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 4);

    // source holds 3 segments for the interval, dest holds 0
    final ServerHolder source = createServerWith(intervalSegments.subList(0, 3));
    final ServerHolder dest = createServerWith(new ArrayList<>());

    final DataSegment toMove = intervalSegments.get(0);
    final ServerHolder chosen =
        strategy.findDestinationServerToMoveSegment(toMove, source, Arrays.asList(source, dest));
    Assert.assertSame(dest, chosen);

    // When source has 2 and dest has 1 (differ by one), no move should happen
    // to avoid oscillation.
    final ServerHolder source2 = createServerWith(intervalSegments.subList(0, 2));
    final ServerHolder dest2 = createServerWith(intervalSegments.subList(2, 3));
    final ServerHolder chosen2 =
        strategy.findDestinationServerToMoveSegment(
            intervalSegments.get(0),
            source2,
            Arrays.asList(source2, dest2)
        );
    Assert.assertNull(chosen2);
  }

  @Test
  public void testDropPrefersMostLoadedServerForInterval()
  {
    final List<DataSegment> intervalSegments = minuteSegments(DS_WIKI, 5);

    final ServerHolder serverA = createServerWith(intervalSegments.subList(0, 1));
    final ServerHolder serverB = createServerWith(intervalSegments.subList(1, 4));

    final DataSegment segmentToDrop = intervalSegments.get(0);
    final Iterator<ServerHolder> ordered =
        strategy.findServersToDropSegment(segmentToDrop, Arrays.asList(serverA, serverB));

    // Most heavily loaded server for the interval should be dropped from first
    Assert.assertSame(serverB, ordered.next());
    Assert.assertSame(serverA, ordered.next());
  }

  private List<DataSegment> minuteSegments(String datasource, int count)
  {
    return CreateDataSegments.ofDatasource(datasource)
                             .forIntervals(1, Granularities.MINUTE)
                             .startingAt(MINUTE_START)
                             .withNumPartitions(count)
                             .eachOfSizeInMb(100);
  }

  private ServerHolder createServerWith(List<DataSegment> segments)
  {
    final String name = "hist_" + uniqueServerId++;
    final DruidServer server =
        new DruidServer(name, name, null, 10L << 30, ServerType.HISTORICAL, "hot", 1);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }
}
