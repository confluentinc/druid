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

import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.http.OutboundHttpClientPool;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.pool.PoolStats;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.Map;

/**
 * Monitor that emits per-destination outbound HTTP metrics for the Broker's Netty HttpClient:
 * - broker/http/numActiveConnections: connections currently checked out and carrying a request
 * - broker/http/numRequestsQueued: threads blocked waiting to acquire a connection
 */
public class BrokerHttpClientMonitor extends AbstractMonitor
{
  private final OutboundHttpClientPool pool;

  @Inject
  public BrokerHttpClientMonitor(@EscalatedClient OutboundHttpClientPool pool)
  {
    this.pool = pool;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Map.Entry<String, PoolStats> entry : pool.getStats().entrySet()) {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension("destination", entry.getKey());

      emitter.emit(builder.setMetric("broker/http/numActiveConnections", entry.getValue().getNumActive()));
      emitter.emit(builder.setMetric("broker/http/numRequestsQueued", entry.getValue().getNumQueued()));
    }

    return true;
  }
}
