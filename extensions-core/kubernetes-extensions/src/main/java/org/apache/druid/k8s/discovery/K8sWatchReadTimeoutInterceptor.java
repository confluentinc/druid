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

import okhttp3.Interceptor;
import okhttp3.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Turns off the read timeout for watch requests, so that {@link K8sApiClientConfig#getReadTimeout()} only applies to
 * ordinary requests. A watch is a long-lived response that can go minutes without a byte when nothing changes.
 */
public class K8sWatchReadTimeoutInterceptor implements Interceptor
{
  @Override
  public Response intercept(Chain chain) throws IOException
  {
    if (chain.readTimeoutMillis() != 0 && "true".equals(chain.request().url().queryParameter("watch"))) {
      return chain.withReadTimeout(0, TimeUnit.MILLISECONDS).proceed(chain.request());
    }
    return chain.proceed(chain.request());
  }
}
