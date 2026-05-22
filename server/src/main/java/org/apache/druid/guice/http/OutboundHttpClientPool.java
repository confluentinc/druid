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

package org.apache.druid.guice.http;

import org.apache.druid.java.util.http.client.pool.PoolStats;
import org.apache.druid.java.util.http.client.pool.ResourcePool;

import java.util.Collections;
import java.util.Map;

/**
 * Guice-bindable handle to the underlying {@link ResourcePool} of an outbound Netty HTTP client.
 * Populated by {@link HttpClientModule.HttpClientProvider} when the client is constructed, then
 * read by monitors that emit per-destination pool stats.
 */
public class OutboundHttpClientPool
{
  private volatile ResourcePool<String, ?> pool;

  public void register(ResourcePool<String, ?> pool)
  {
    this.pool = pool;
  }

  /**
   * Returns a snapshot of per-destination pool state, or an empty map if no pool has been registered yet.
   */
  public Map<String, PoolStats> getStats()
  {
    final ResourcePool<String, ?> p = pool;
    return p == null ? Collections.emptyMap() : p.getStats();
  }
}
