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

import okhttp3.HttpUrl;
import okhttp3.Request;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;

/**
 * Describes a kube-apiserver request for metrics: the client that sent it, its verb (as the apiserver's audit log
 * names it), resource and route template. Namespaces and object names are replaced with placeholders, so all values
 * stay bounded when used as metric dimensions.
 */
public class K8sApiRequest
{
  static final String CLIENT_DISCOVERY = "discovery";
  static final String CLIENT_LEADER_ELECTION = "leaderElection";
  static final String CLIENT_OTHER = "other";
  static final String UNKNOWN = "unknown";

  private final String client;
  private final String verb;
  private final String resource;
  private final String route;
  private final boolean watch;

  K8sApiRequest(String client, String verb, String resource, String route, boolean watch)
  {
    this.client = client;
    this.verb = verb;
    this.resource = resource;
    this.route = route;
    this.watch = watch;
  }

  public static K8sApiRequest from(Request request)
  {
    final HttpUrl url = request.url();
    final List<String> segments = url.pathSegments();
    int size = segments.size();
    if (size > 0 && segments.get(size - 1).isEmpty()) {
      // A trailing slash yields an empty last segment.
      size--;
    }

    // Core API paths are /api/{version}/..., all other groups are /apis/{group}/{version}/...
    int i;
    if (size >= 2 && "api".equals(segments.get(0))) {
      i = 2;
    } else if (size >= 3 && "apis".equals(segments.get(0))) {
      i = 3;
    } else {
      return new K8sApiRequest(CLIENT_OTHER, StringUtils.toLowerCase(request.method()), UNKNOWN, UNKNOWN, false);
    }

    final StringBuilder route = new StringBuilder();
    for (int j = 0; j < i; j++) {
      route.append('/').append(segments.get(j));
    }
    if (i + 2 < size && "namespaces".equals(segments.get(i))) {
      route.append("/namespaces/{namespace}");
      i += 2;
    }

    String resource = UNKNOWN;
    if (i < size) {
      resource = segments.get(i++);
      route.append('/').append(resource);
    }
    final boolean hasName = i < size;
    if (hasName) {
      route.append("/{name}");
      i++;
    }
    if (i < size) {
      // Subresource, such as pods/{name}/status.
      route.append('/').append(segments.get(i++));
    }
    if (i < size) {
      route.append("/...");
    }

    final boolean watch = "true".equals(url.queryParameter("watch"));
    return new K8sApiRequest(
        clientForResource(resource),
        verb(request.method(), hasName, watch),
        resource,
        route.toString(),
        watch
    );
  }

  private static String verb(String method, boolean hasName, boolean watch)
  {
    switch (method) {
      case "GET":
        return watch ? "watch" : hasName ? "get" : "list";
      case "POST":
        return "create";
      case "PUT":
        return "update";
      case "PATCH":
        return "patch";
      case "DELETE":
        return hasName ? "delete" : "deletecollection";
      default:
        return StringUtils.toLowerCase(method);
    }
  }

  /**
   * Discovery and leader election share one ApiClient, so the resource tells them apart: discovery announces and
   * watches pods, while leader election holds a ConfigMap lock.
   */
  private static String clientForResource(String resource)
  {
    switch (resource) {
      case "pods":
        return CLIENT_DISCOVERY;
      case "configmaps":
      case "leases":
        return CLIENT_LEADER_ELECTION;
      default:
        return CLIENT_OTHER;
    }
  }

  public String getClient()
  {
    return client;
  }

  public String getVerb()
  {
    return verb;
  }

  public String getResource()
  {
    return resource;
  }

  public String getRoute()
  {
    return route;
  }

  public boolean isWatch()
  {
    return watch;
  }
}
