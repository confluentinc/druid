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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Watch;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.DruidNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Runs {@link DefaultK8sApiClient#watchPods} against a canned watch stream to cover its event remapping.
 */
public class DefaultK8sApiClientWatchPodsTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  static {
    // DiscoveryDruidNode's @JsonCreator injects the ObjectMapper itself.
    JSON_MAPPER.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, JSON_MAPPER));
  }

  private final DiscoveryDruidNode testNode = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private MockWebServer server;
  private DefaultK8sApiClient client;

  @Before
  public void setUp() throws IOException
  {
    server = new MockWebServer();
    server.start();
    ApiClient apiClient = new ApiClient().setBasePath("http://" + server.getHostName() + ":" + server.getPort());
    client = new DefaultK8sApiClient(apiClient, JSON_MAPPER);
  }

  @After
  public void tearDown() throws IOException
  {
    server.shutdown();
  }

  @Test
  public void testBookmarkAsFirstEventSurfacesResourceVersion() throws Exception
  {
    enqueueWatchStream(bookmarkLine("100"));

    WatchResult result = client.watchPods("testns", null, "99", NodeRole.ROUTER);
    try {
      Assert.assertTrue(result.hasNext());
      Watch.Response<DiscoveryDruidNodeAndResourceVersion> bookmark = result.next();
      Assert.assertNotNull(bookmark);
      Assert.assertEquals(WatchResult.BOOKMARK, bookmark.type);
      Assert.assertEquals("100", bookmark.object.getResourceVersion());
      Assert.assertNull(bookmark.object.getNode());
    }
    finally {
      result.close();
    }
  }

  @Test
  public void testBookmarkAfterRealEventSurfacesItsOwnResourceVersion() throws Exception
  {
    enqueueWatchStream(podLine(WatchResult.ADDED, "200", true, true, nodeAnnotation()) + bookmarkLine("201"));

    WatchResult result = client.watchPods("testns", null, "99", NodeRole.ROUTER);
    try {
      Assert.assertTrue(result.hasNext());
      Watch.Response<DiscoveryDruidNodeAndResourceVersion> added = result.next();
      Assert.assertEquals(WatchResult.ADDED, added.type);
      Assert.assertEquals("200", added.object.getResourceVersion());

      Assert.assertTrue(result.hasNext());
      Watch.Response<DiscoveryDruidNodeAndResourceVersion> bookmark = result.next();
      Assert.assertEquals(WatchResult.BOOKMARK, bookmark.type);
      Assert.assertEquals("201", bookmark.object.getResourceVersion());
    }
    finally {
      result.close();
    }
  }

  @Test
  public void testModifiedReadyPodIsRemappedToAdded() throws Exception
  {
    enqueueWatchStream(podLine("MODIFIED", "200", true, true, nodeAnnotation()));

    Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = readSingleEvent();
    Assert.assertEquals(WatchResult.ADDED, response.type);
    Assert.assertEquals("200", response.object.getResourceVersion());
    Assert.assertEquals(testNode, response.object.getNode());
  }

  @Test
  public void testModifiedPodWithNotReadySidecarIsRemappedToNotReady() throws Exception
  {
    enqueueWatchStream(podLine("MODIFIED", "200", true, false, nodeAnnotation()));

    Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = readSingleEvent();
    Assert.assertEquals(WatchResult.NOT_READY, response.type);
    Assert.assertEquals("200", response.object.getResourceVersion());
    Assert.assertEquals(testNode, response.object.getNode());
  }

  @Test
  public void testModifiedPodOnLostNodeIsRemappedToNotReady() throws Exception
  {
    enqueueWatchStream(podLine("MODIFIED", "200", "False", true, true, nodeAnnotation()));

    Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = readSingleEvent();
    Assert.assertEquals(WatchResult.NOT_READY, response.type);
    Assert.assertEquals(testNode, response.object.getNode());
  }

  @Test
  public void testAddedNotReadyPodIsSkipped() throws Exception
  {
    enqueueWatchStream(
        podLine(WatchResult.ADDED, "200", false, true, nodeAnnotation())
        + podLine("MODIFIED", "201", true, true, nodeAnnotation())
    );

    Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = readSingleEvent();
    Assert.assertEquals(WatchResult.ADDED, response.type);
    Assert.assertEquals("201", response.object.getResourceVersion());
  }

  @Test
  public void testDeletedNotReadyPodIsNotRemapped() throws Exception
  {
    enqueueWatchStream(podLine(WatchResult.DELETED, "200", false, false, nodeAnnotation()));

    Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = readSingleEvent();
    Assert.assertEquals(WatchResult.DELETED, response.type);
    Assert.assertEquals(testNode, response.object.getNode());
  }

  @Test
  public void testUndeserializableAnnotationSurfacesNullObject() throws Exception
  {
    enqueueWatchStream(podLine("MODIFIED", "200", true, true, "not-json"));

    Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = readSingleEvent();
    Assert.assertEquals(WatchResult.ADDED, response.type);
    Assert.assertNull(response.object);
  }

  private Watch.Response<DiscoveryDruidNodeAndResourceVersion> readSingleEvent() throws Exception
  {
    WatchResult result = client.watchPods("testns", null, "99", NodeRole.ROUTER);
    try {
      Assert.assertTrue(result.hasNext());
      Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = result.next();
      Assert.assertNotNull(response);
      Assert.assertFalse(result.hasNext());
      return response;
    }
    finally {
      result.close();
    }
  }

  private void enqueueWatchStream(String body)
  {
    server.enqueue(new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body));
  }

  private String nodeAnnotation() throws Exception
  {
    return JSON_MAPPER.writeValueAsString(testNode);
  }

  private static String bookmarkLine(String resourceVersion) throws Exception
  {
    return JSON_MAPPER.writeValueAsString(
        ImmutableMap.of(
            "type", WatchResult.BOOKMARK,
            "object", ImmutableMap.of(
                "kind", "Pod",
                "apiVersion", "v1",
                "metadata", ImmutableMap.of("resourceVersion", resourceVersion)
            )
        )
    ) + "\n";
  }

  private static String podLine(
      String type,
      String resourceVersion,
      boolean druidReady,
      boolean sidecarReady,
      String nodeAnnotation
  ) throws Exception
  {
    String readyCondition = druidReady && sidecarReady ? "True" : "False";
    return podLine(type, resourceVersion, readyCondition, druidReady, sidecarReady, nodeAnnotation);
  }

  private static String podLine(
      String type,
      String resourceVersion,
      String readyCondition,
      boolean druidReady,
      boolean sidecarReady,
      String nodeAnnotation
  ) throws Exception
  {
    Map<String, Object> pod = new HashMap<>();
    pod.put("kind", "Pod");
    pod.put("apiVersion", "v1");
    pod.put(
        "metadata",
        ImmutableMap.of(
            "name", "test-pod",
            "resourceVersion", resourceVersion,
            "annotations", ImmutableMap.of(K8sDruidNodeAnnouncer.getInfoAnnotation(NodeRole.ROUTER), nodeAnnotation)
        )
    );
    pod.put(
        "status",
        ImmutableMap.of(
            "conditions",
            ImmutableList.of(ImmutableMap.of("type", "Ready", "status", readyCondition)),
            "containerStatuses",
            ImmutableList.of(containerStatus("druid", druidReady), containerStatus("envoy", sidecarReady))
        )
    );
    return JSON_MAPPER.writeValueAsString(ImmutableMap.of("type", type, "object", pod)) + "\n";
  }

  private static Map<String, Object> containerStatus(String name, boolean ready)
  {
    return ImmutableMap.of("name", name, "image", name, "imageID", name, "ready", ready, "restartCount", 0);
  }
}
