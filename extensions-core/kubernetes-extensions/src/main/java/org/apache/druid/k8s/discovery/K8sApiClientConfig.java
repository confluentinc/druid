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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * HTTP settings for the Kubernetes API client used by discovery and leader election, bound to
 * {@code druid.discovery.k8s.client}.
 */
public class K8sApiClientConfig
{
  // client-java's defaults: OkHttp's 10s connect timeout, and no read timeout.
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.standardSeconds(10);
  private static final Duration DEFAULT_READ_TIMEOUT = Duration.ZERO;

  @JsonProperty
  private final Duration connectTimeout;

  @JsonProperty
  private final Duration readTimeout;

  @JsonCreator
  public K8sApiClientConfig(
      @JsonProperty("connectTimeout") @Nullable Duration connectTimeout,
      @JsonProperty("readTimeout") @Nullable Duration readTimeout
  )
  {
    this.connectTimeout = connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT : connectTimeout;
    this.readTimeout = readTimeout == null ? DEFAULT_READ_TIMEOUT : readTimeout;
    Preconditions.checkArgument(this.connectTimeout.getMillis() >= 0, "connectTimeout must not be negative");
    Preconditions.checkArgument(this.readTimeout.getMillis() >= 0, "readTimeout must not be negative");
  }

  /**
   * Timeout for establishing a TCP connection to the API server. Zero means no timeout.
   */
  @JsonProperty
  public Duration getConnectTimeout()
  {
    return connectTimeout;
  }

  /**
   * Timeout between bytes read from the API server on non-watch requests. Zero means no timeout. Watches never time
   * out on read, because a watch can be quiet for minutes when nothing changes.
   */
  @JsonProperty
  public Duration getReadTimeout()
  {
    return readTimeout;
  }

  @Override
  public String toString()
  {
    return "K8sApiClientConfig{" +
           "connectTimeout=" + connectTimeout +
           ", readTimeout=" + readTimeout +
           '}';
  }
}
