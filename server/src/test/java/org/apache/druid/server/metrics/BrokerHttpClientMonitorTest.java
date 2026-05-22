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

package org.apache.druid.server.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.http.OutboundHttpClientPool;
import org.apache.druid.java.util.http.client.pool.PoolStats;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BrokerHttpClientMonitorTest
{
  private OutboundHttpClientPool poolHolder;
  private BrokerHttpClientMonitor monitor;

  @Before
  public void setUp()
  {
    poolHolder = new OutboundHttpClientPool();
    monitor = new BrokerHttpClientMonitor(poolHolder);
  }

  @Test
  public void testDoMonitorReturnsTrueWithNoPoolRegistered()
  {
    Assert.assertTrue(monitor.doMonitor(new StubServiceEmitter("broker", "localhost")));
  }

  @Test
  public void testEmptyPoolEmitsNoEvents()
  {
    @SuppressWarnings("unchecked")
    ResourcePool<String, Object> pool = (ResourcePool<String, Object>) Mockito.mock(ResourcePool.class);
    Mockito.when(pool.getStats()).thenReturn(Collections.emptyMap());
    poolHolder.register(pool);

    final StubServiceEmitter emitter = new StubServiceEmitter("broker", "localhost");
    monitor.doMonitor(emitter);

    Assert.assertTrue(emitter.getEvents().isEmpty());
  }

  @Test
  public void testMultipleDestinationsEmitsPerDestinationMetrics()
  {
    @SuppressWarnings("unchecked")
    ResourcePool<String, Object> pool = (ResourcePool<String, Object>) Mockito.mock(ResourcePool.class);
    Map<String, PoolStats> stats = ImmutableMap.of(
        "http://host1:8083", new PoolStats(2, 3),
        "http://host2:8083", new PoolStats(5, 1)
    );
    Mockito.when(pool.getStats()).thenReturn(stats);
    poolHolder.register(pool);

    final StubServiceEmitter emitter = new StubServiceEmitter("broker", "localhost");
    monitor.doMonitor(emitter);

    // 2 metrics per destination
    Assert.assertEquals(4, emitter.getEvents().size());

    List<Map<String, Object>> events = emitter.getEvents().stream()
                                              .map(e -> e.toMap())
                                              .collect(java.util.stream.Collectors.toList());

    Assert.assertTrue(events.stream().anyMatch(e ->
        "broker/http/numActiveConnections".equals(e.get("metric")) &&
        "http://host1:8083".equals(e.get("destination")) &&
        Integer.valueOf(2).equals(e.get("value"))
    ));
    Assert.assertTrue(events.stream().anyMatch(e ->
        "broker/http/numRequestsQueued".equals(e.get("metric")) &&
        "http://host1:8083".equals(e.get("destination")) &&
        Integer.valueOf(3).equals(e.get("value"))
    ));
    Assert.assertTrue(events.stream().anyMatch(e ->
        "broker/http/numActiveConnections".equals(e.get("metric")) &&
        "http://host2:8083".equals(e.get("destination")) &&
        Integer.valueOf(5).equals(e.get("value"))
    ));
    Assert.assertTrue(events.stream().anyMatch(e ->
        "broker/http/numRequestsQueued".equals(e.get("metric")) &&
        "http://host2:8083".equals(e.get("destination")) &&
        Integer.valueOf(1).equals(e.get("value"))
    ));
  }
}
