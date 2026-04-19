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
import com.google.inject.Provider;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.server.router.Router;
import org.eclipse.jetty.client.Destination;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.transport.HttpDestination;

/**
 * Monitor that emits metrics for the Router's outbound Jetty HttpClient,
 * including the total number of requests currently queued across all destinations.
 */
public class RouterHttpClientMonitor extends AbstractMonitor
{
  private final Provider<HttpClient> httpClientProvider;

  @Inject
  public RouterHttpClientMonitor(@Router Provider<HttpClient> httpClientProvider)
  {
    this.httpClientProvider = httpClientProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final HttpClient httpClient = httpClientProvider.get();
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();

    int totalQueuedRequests = 0;
    for (Destination destination : httpClient.getDestinations()) {
      if (destination instanceof HttpDestination) {
        totalQueuedRequests += ((HttpDestination) destination).getQueuedRequestCount();
      }
    }

    emitter.emit(builder.setMetric("router/http/numRequestsQueued", totalQueuedRequests));

    return true;
  }
}
