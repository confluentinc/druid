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

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import okhttp3.OkHttpClient;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.MetricsModule;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class K8sDiscoveryModule implements DruidModule
{
  private static final String K8S_KEY = "k8s";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.discovery.k8s", K8sDiscoveryConfig.class);
    JsonConfigProvider.bind(binder, "druid.discovery.k8s.client", K8sApiClientConfig.class);

    binder.bind(K8sClientMonitor.class).in(LazySingleton.class);
    MetricsModule.register(binder, K8sClientMonitor.class);

    binder.bind(K8sApiClient.class).to(DefaultK8sApiClient.class).in(LazySingleton.class);
    binder.bind(K8sLeaderElectorFactory.class).to(DefaultK8sLeaderElectorFactory.class).in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidNodeDiscoveryProvider.class))
            .addBinding(K8S_KEY)
            .to(K8sDruidNodeDiscoveryProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidNodeAnnouncer.class))
            .addBinding(K8S_KEY)
            .to(K8sDruidNodeAnnouncer.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, Coordinator.class))
            .addBinding(K8S_KEY)
            .toProvider(
                K8sDruidLeaderSelectorProvider.K8sCoordinatorDruidLeaderSelectorProvider.class
            )
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, IndexingService.class))
            .addBinding(K8S_KEY)
            .toProvider(
                K8sDruidLeaderSelectorProvider.K8sIndexingServiceDruidLeaderSelectorProvider.class
            )
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public ApiClient makeApiClient(
      @Self DruidNode druidNode,
      K8sDiscoveryConfig discoveryConfig,
      K8sApiClientConfig clientConfig,
      ServiceEmitter emitter,
      K8sClientMonitor clientMonitor
  )
  {
    try {
      // Note: we can probably improve things here about figuring out how to find the K8S API server.
      final ApiClient client = Config.defaultClient();
      // Identifies Druid traffic in kube-apiserver audit logs and API Priority and Fairness metrics.
      client.setUserAgent(buildUserAgent(druidNode.getServiceName(), discoveryConfig.getClusterIdentifier()));
      client.setHttpClient(
          configureHttpClient(client.getHttpClient(), clientConfig, emitter, clientMonitor.getInflightCounter())
      );
      clientMonitor.setHttpClient(client.getHttpClient());
      return client;
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to create K8s ApiClient instance", ex);
    }
  }

  static OkHttpClient configureHttpClient(
      OkHttpClient httpClient,
      K8sApiClientConfig clientConfig,
      ServiceEmitter emitter,
      AtomicInteger inflight
  )
  {
    return httpClient.newBuilder()
                     .connectTimeout(clientConfig.getConnectTimeout().getMillis(), TimeUnit.MILLISECONDS)
                     .readTimeout(clientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS)
                     .addInterceptor(new K8sWatchReadTimeoutInterceptor())
                     .eventListenerFactory(K8sClientEventListener.factory(emitter, inflight))
                     .build();
  }

  static String buildUserAgent(String serviceName, String clusterIdentifier)
  {
    final String version = GuavaUtils.firstNonNull(
        K8sDiscoveryModule.class.getPackage().getImplementationVersion(),
        "unknown"
    );
    return StringUtils.format(
        "druid/%s (%s; k8s-discovery; cluster=%s)",
        sanitizeUserAgentValue(version),
        sanitizeUserAgentValue(serviceName),
        sanitizeUserAgentValue(clusterIdentifier)
    );
  }

  /**
   * Replaces characters that cannot appear in the User-Agent comment with '_'. Header values must be printable ASCII
   * (OkHttp rejects anything else, failing every request), and '(', ')', '\' and ';' delimit the comment's fields.
   */
  static String sanitizeUserAgentValue(String value)
  {
    final StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      final boolean allowed = c >= 0x20 && c < 0x7f && c != '(' && c != ')' && c != '\\' && c != ';';
      sb.append(allowed ? c : '_');
    }
    return sb.toString();
  }
}
