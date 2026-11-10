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

import okhttp3.Request;
import okhttp3.RequestBody;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

public class K8sApiRequestTest
{
  private static final String BASE = "https://10.0.0.1:443";

  @Test
  public void testListPods()
  {
    assertRequest(
        "GET",
        "/api/v1/namespaces/druid/pods?labelSelector=druidDiscoveryAnnouncement-cluster-identifier%3Dc1",
        "discovery",
        "list",
        "pods",
        "/api/v1/namespaces/{namespace}/pods",
        false
    );
  }

  @Test
  public void testWatchPods()
  {
    assertRequest(
        "GET",
        "/api/v1/namespaces/druid/pods?labelSelector=a%3Db&resourceVersion=123&watch=true",
        "discovery",
        "watch",
        "pods",
        "/api/v1/namespaces/{namespace}/pods",
        true
    );
  }

  @Test
  public void testPatchPod()
  {
    assertRequest(
        "PATCH",
        "/api/v1/namespaces/druid/pods/druid-broker-0",
        "discovery",
        "patch",
        "pods",
        "/api/v1/namespaces/{namespace}/pods/{name}",
        false
    );
  }

  @Test
  public void testConfigMapLock()
  {
    final String path = "/api/v1/namespaces/druid/configmaps/c1-leaderelection-coordinator";
    final String route = "/api/v1/namespaces/{namespace}/configmaps/{name}";
    assertRequest("GET", path, "leaderElection", "get", "configmaps", route, false);
    assertRequest("PUT", path, "leaderElection", "update", "configmaps", route, false);
    assertRequest(
        "POST",
        "/api/v1/namespaces/druid/configmaps",
        "leaderElection",
        "create",
        "configmaps",
        "/api/v1/namespaces/{namespace}/configmaps",
        false
    );
  }

  @Test
  public void testLeaseInNamedGroup()
  {
    assertRequest(
        "GET",
        "/apis/coordination.k8s.io/v1/namespaces/druid/leases/lock",
        "leaderElection",
        "get",
        "leases",
        "/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases/{name}",
        false
    );
  }

  @Test
  public void testSubresourceAndTrailingSegments()
  {
    assertRequest(
        "GET",
        "/api/v1/namespaces/druid/pods/p1/status",
        "discovery",
        "get",
        "pods",
        "/api/v1/namespaces/{namespace}/pods/{name}/status",
        false
    );
    assertRequest(
        "GET",
        "/api/v1/namespaces/druid/pods/p1/proxy/some/path",
        "discovery",
        "get",
        "pods",
        "/api/v1/namespaces/{namespace}/pods/{name}/proxy/...",
        false
    );
  }

  @Test
  public void testNamespaceObjectAndTrailingSlash()
  {
    assertRequest("GET", "/api/v1/namespaces/druid", "other", "get", "namespaces", "/api/v1/namespaces/{name}", false);
    assertRequest("GET", "/api/v1/namespaces/druid/pods/", "discovery", "list", "pods", "/api/v1/namespaces/{namespace}/pods", false);
  }

  @Test
  public void testDeleteVerbs()
  {
    assertRequest("DELETE", "/api/v1/namespaces/druid/pods/p1", "discovery", "delete", "pods", "/api/v1/namespaces/{namespace}/pods/{name}", false);
    assertRequest("DELETE", "/api/v1/namespaces/druid/pods", "discovery", "deletecollection", "pods", "/api/v1/namespaces/{namespace}/pods", false);
  }

  @Test
  public void testNonResourcePath()
  {
    assertRequest("GET", "/version", "other", "get", "unknown", "unknown", false);
    assertRequest("GET", "/api", "other", "get", "unknown", "unknown", false);
  }

  private static void assertRequest(
      String method,
      String pathAndQuery,
      String client,
      String verb,
      String resource,
      String route,
      boolean watch
  )
  {
    final K8sApiRequest request = K8sApiRequest.from(
        new Request.Builder().url(BASE + pathAndQuery).method(method, body(method)).build()
    );
    Assert.assertEquals(pathAndQuery, client, request.getClient());
    Assert.assertEquals(pathAndQuery, verb, request.getVerb());
    Assert.assertEquals(pathAndQuery, resource, request.getResource());
    Assert.assertEquals(pathAndQuery, route, request.getRoute());
    Assert.assertEquals(pathAndQuery, watch, request.isWatch());
  }

  @Nullable
  private static RequestBody body(String method)
  {
    return "GET".equals(method) || "DELETE".equals(method) ? null : RequestBody.create(new byte[0]);
  }
}
