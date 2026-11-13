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

import com.google.common.collect.ImmutableMap;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.SocketPolicy;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs requests through a client built by {@link K8sDiscoveryModule#configureHttpClient} against a
 * {@link MockWebServer}, and checks the metrics {@link K8sClientEventListener} emits.
 */
public class K8sClientEventListenerTest
{
  private static final String LIST_PODS = "/api/v1/namespaces/druid/pods?labelSelector=a%3Db";
  private static final String WATCH_PODS = LIST_PODS + "&watch=true";
  private static final Map<String, Object> LIST_PODS_DIMS = ImmutableMap.of(
      "client", "discovery",
      "peer", "kube-apiserver",
      "verb", "list",
      "resource", "pods",
      "route", "/api/v1/namespaces/{namespace}/pods"
  );

  private MockWebServer server;
  private StubServiceEmitter emitter;
  private AtomicInteger inflight;

  @Before
  public void setUp() throws IOException
  {
    server = new MockWebServer();
    server.start();
    emitter = new StubServiceEmitter();
    inflight = new AtomicInteger();
  }

  @After
  public void tearDown() throws IOException
  {
    server.shutdown();
  }

  @Test
  public void testSuccessfulRequest() throws IOException
  {
    server.enqueue(new MockResponse().setBody("{\"items\":[]}"));
    server.enqueue(new MockResponse().setBody("{\"items\":[]}"));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    execute(client, LIST_PODS);
    execute(client, LIST_PODS);

    final Map<String, Object> okDims = withDims(LIST_PODS_DIMS, "statusClass", "2xx", "code", "200");
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, okDims, 2);
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_TIME, okDims, 2);
    // The second request reuses the first one's connection.
    emitter.verifyEmitted(K8sClientEventListener.CONNECTION_NEW, ImmutableMap.of("client", "discovery"), 1);
    emitter.verifyNotEmitted(K8sClientEventListener.THROTTLED_COUNT);
    emitter.verifyNotEmitted(K8sClientEventListener.ERROR_COUNT);
    emitter.verifyNotEmitted(K8sClientEventListener.RETRY_COUNT);
    Assert.assertEquals(0, inflight.get());
  }

  @Test
  public void testInflightCountsOpenWatch() throws IOException
  {
    server.enqueue(new MockResponse().setChunkedBody("{\"type\":\"ADDED\",\"object\":{}}\n", 8));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    try (Response response = client.newCall(request(WATCH_PODS)).execute()) {
      Assert.assertEquals(1, inflight.get());
      response.body().string();
    }
    Assert.assertEquals(0, inflight.get());
  }

  @Test
  public void testThrottledRequest() throws IOException
  {
    server.enqueue(new MockResponse().setResponseCode(429).setHeader("Retry-After", "1"));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    execute(client, LIST_PODS);

    final Map<String, Object> throttledDims = withDims(LIST_PODS_DIMS, "statusClass", "4xx", "code", "429");
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, throttledDims, 1);
    emitter.verifyEmitted(K8sClientEventListener.THROTTLED_COUNT, throttledDims, 1);
  }

  @Test
  public void testWatchIsCountedOnceAndNotTimed() throws IOException
  {
    server.enqueue(new MockResponse().setChunkedBody("{\"type\":\"ADDED\",\"object\":{}}\n", 8));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    execute(client, WATCH_PODS);

    emitter.verifyEmitted(
        K8sClientEventListener.REQUEST_COUNT,
        ImmutableMap.of("verb", "watch", "statusClass", "2xx", "code", "200"),
        1
    );
    emitter.verifyNotEmitted(K8sClientEventListener.REQUEST_TIME);
    emitter.verifyNotEmitted(K8sClientEventListener.ERROR_COUNT);
  }

  @Test
  public void testWatchClosedEarlyIsNotAnError() throws IOException
  {
    server.enqueue(new MockResponse().setChunkedBody("{\"type\":\"ADDED\",\"object\":{}}\n", 8)
                                     .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    try (Response response = client.newCall(request(WATCH_PODS)).execute()) {
      response.body().string();
    }
    catch (IOException e) {
      // Expected: the server drops the watch midway through the body.
    }

    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, ImmutableMap.of("verb", "watch"), 1);
    emitter.verifyNotEmitted(K8sClientEventListener.ERROR_COUNT);
  }

  @Test
  public void testRedirectedWatchIsCountedOnceWithItsRetries() throws IOException
  {
    // The watch is redirected, then the final stream drops midway: one count for the final 200, the redirect reported
    // as a retry, and no error for the stream ending.
    server.enqueue(new MockResponse().setResponseCode(302).setHeader("Location", WATCH_PODS));
    server.enqueue(new MockResponse().setChunkedBody("{\"type\":\"ADDED\",\"object\":{}}\n", 8)
                                     .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    try (Response response = client.newCall(request(WATCH_PODS)).execute()) {
      response.body().string();
    }
    catch (IOException e) {
      // Expected: the server drops the watch midway through the body.
    }

    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, 1);
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, ImmutableMap.of("verb", "watch", "code", "200"), 1);
    emitter.verifyValue(K8sClientEventListener.RETRY_COUNT, ImmutableMap.of("verb", "watch"), 1);
    emitter.verifyNotEmitted(K8sClientEventListener.ERROR_COUNT);
  }

  @Test
  public void testWatchFailingAfterRedirectIsAnError() throws IOException
  {
    server.enqueue(new MockResponse().setResponseCode(302).setHeader("Location", closedServerUrl(WATCH_PODS)));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    Assert.assertThrows(IOException.class, () -> execute(client, WATCH_PODS));

    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, 1);
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, ImmutableMap.of("verb", "watch", "code", "error"), 1);
    emitter.verifyEmitted(
        K8sClientEventListener.ERROR_COUNT,
        ImmutableMap.of("verb", "watch", "errorType", "connectionRefused"),
        1
    );
    // The redirect target refused the connection, so the request was never sent again.
    emitter.verifyNotEmitted(K8sClientEventListener.RETRY_COUNT);
  }

  @Test
  public void testRequestFailingAfterRedirectHasNoStatus() throws IOException
  {
    server.enqueue(new MockResponse().setResponseCode(302).setHeader("Location", closedServerUrl(LIST_PODS)));
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    Assert.assertThrows(IOException.class, () -> execute(client, LIST_PODS));

    // The redirect's 302 is not the call's result.
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, 1);
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, ImmutableMap.of("statusClass", "error"), 1);
    emitter.verifyEmitted(K8sClientEventListener.ERROR_COUNT, ImmutableMap.of("code", "error"), 1);
  }

  @Test
  public void testConnectionRefused() throws IOException
  {
    final String url = server.url(LIST_PODS).toString();
    server.shutdown();
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, null));

    Assert.assertThrows(IOException.class, () -> client.newCall(new Request.Builder().url(url).build()).execute());
    Assert.assertEquals(0, inflight.get());

    final Map<String, Object> errorDims = withDims(LIST_PODS_DIMS, "statusClass", "error", "code", "error");
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, errorDims, 1);
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_TIME, errorDims, 1);
    emitter.verifyEmitted(
        K8sClientEventListener.ERROR_COUNT,
        withDims(errorDims, "errorType", "connectionRefused"),
        1
    );
    // One failure per address the host resolves to (localhost may resolve to both IPv4 and IPv6).
    Assert.assertFalse(
        emitter.getMetricValues(
            K8sClientEventListener.CONNECTION_FAILED,
            ImmutableMap.of("client", "discovery", "errorType", "connectionRefused")
        ).isEmpty()
    );
  }

  @Test
  public void testReadTimeoutAppliesToRequestsButNotWatches() throws IOException
  {
    final OkHttpClient client = buildClient(new K8sApiClientConfig(null, Duration.millis(200)));

    server.enqueue(new MockResponse().setBody("{}").setHeadersDelay(1, TimeUnit.SECONDS));
    Assert.assertThrows(IOException.class, () -> execute(client, LIST_PODS));
    emitter.verifyEmitted(K8sClientEventListener.ERROR_COUNT, ImmutableMap.of("errorType", "timeout"), 1);

    emitter.flush();
    server.enqueue(new MockResponse().setBody("{}").setHeadersDelay(1, TimeUnit.SECONDS));
    execute(client, WATCH_PODS);
    emitter.verifyEmitted(K8sClientEventListener.REQUEST_COUNT, ImmutableMap.of("verb", "watch", "code", "200"), 1);
    emitter.verifyNotEmitted(K8sClientEventListener.ERROR_COUNT);
  }

  @Test
  public void testConfiguredTimeoutsAreApplied()
  {
    final OkHttpClient client = buildClient(new K8sApiClientConfig(Duration.standardSeconds(3), Duration.standardSeconds(30)));
    Assert.assertEquals(3_000, client.connectTimeoutMillis());
    Assert.assertEquals(30_000, client.readTimeoutMillis());

    final OkHttpClient defaultClient = buildClient(new K8sApiClientConfig(null, null));
    Assert.assertEquals(10_000, defaultClient.connectTimeoutMillis());
    Assert.assertEquals(0, defaultClient.readTimeoutMillis());
  }

  private static String closedServerUrl(String pathAndQuery) throws IOException
  {
    final MockWebServer closed = new MockWebServer();
    closed.start();
    final String url = closed.url(pathAndQuery).toString();
    closed.shutdown();
    return url;
  }

  private OkHttpClient buildClient(K8sApiClientConfig config)
  {
    return K8sDiscoveryModule.configureHttpClient(new OkHttpClient(), config, emitter, inflight);
  }

  private void execute(OkHttpClient client, String pathAndQuery) throws IOException
  {
    try (Response response = client.newCall(request(pathAndQuery)).execute()) {
      response.body().string();
    }
  }

  private Request request(String pathAndQuery)
  {
    return new Request.Builder().url(server.url(pathAndQuery)).build();
  }

  private static Map<String, Object> withDims(Map<String, Object> base, String k1, Object v1, String k2, Object v2)
  {
    return ImmutableMap.<String, Object>builder().putAll(base).put(k1, v1).put(k2, v2).build();
  }

  private static Map<String, Object> withDims(Map<String, Object> base, String k1, Object v1)
  {
    return ImmutableMap.<String, Object>builder().putAll(base).put(k1, v1).build();
  }
}
