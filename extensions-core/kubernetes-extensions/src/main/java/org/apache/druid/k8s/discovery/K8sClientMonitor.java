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


package org.apache.druid.k8s.discovery;

import okhttp3.OkHttpClient;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Emits connection pool and in-flight call gauges for the Kubernetes API client. The monitor is always registered,
 * and emits nothing until {@link K8sDiscoveryModule} builds the client, which only happens when discovery or leader
 * election uses Kubernetes.
 */
public class K8sClientMonitor extends AbstractMonitor
{
  public static final String CONNECTION_OPEN = "k8s/client/connection/open";
  public static final String CONNECTION_IDLE = "k8s/client/connection/idle";
  public static final String INFLIGHT = "k8s/client/inflight";

  private final AtomicInteger inflight = new AtomicInteger();
  private volatile OkHttpClient httpClient;

  /**
   * Counter of calls in progress, kept by {@link K8sClientEventListener}. OkHttp's own dispatcher count can't be used:
   * it drops a synchronous call once its response headers arrive, so it misses open watches and body reads.
   */
  AtomicInteger getInflightCounter()
  {
    return inflight;
  }

  void setHttpClient(OkHttpClient httpClient)
  {
    this.httpClient = httpClient;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final OkHttpClient client = httpClient;
    if (client != null) {
      emitter.emit(metric().setMetric(CONNECTION_OPEN, client.connectionPool().connectionCount()));
      emitter.emit(metric().setMetric(CONNECTION_IDLE, client.connectionPool().idleConnectionCount()));
      emitter.emit(metric().setMetric(INFLIGHT, inflight.get()));
    }
    return true;
  }

  private static ServiceMetricEvent.Builder metric()
  {
    return ServiceMetricEvent.builder().setDimension("peer", K8sClientEventListener.PEER);
  }
}
