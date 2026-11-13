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
import okhttp3.Request;
import okhttp3.Response;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Handles watch requests, which stay open for as long as the apiserver or Druid keeps them.
 *
 * <ul>
 *   <li>Turns off the read timeout, so {@link K8sApiClientConfig#getReadTimeout()} only applies to ordinary requests.
 *   A watch can go minutes without a byte when nothing changes.</li>
 *   <li>Counts the watch once its final response arrives. As an application interceptor it runs outside OkHttp's
 *   redirect and retry handling, so it never sees intermediate responses. Ending a watch after that, by Druid or the
 *   apiserver, is not counted as a failure.</li>
 * </ul>
 */
public class K8sWatchInterceptor implements Interceptor
{
  private final ServiceEmitter emitter;

  public K8sWatchInterceptor(ServiceEmitter emitter)
  {
    this.emitter = emitter;
  }

  @Override
  public Response intercept(Chain chain) throws IOException
  {
    final Request request = chain.request();
    final K8sApiRequest apiRequest = K8sApiRequest.from(request);
    if (!apiRequest.isWatch()) {
      return chain.proceed(request);
    }

    final Chain watchChain = chain.readTimeoutMillis() == 0 ? chain : chain.withReadTimeout(0, TimeUnit.MILLISECONDS);
    final Response response;
    try {
      response = watchChain.proceed(request);
    }
    catch (IOException e) {
      emitter.emit(K8sClientEventListener.requestMetric(apiRequest, -1).setMetric(K8sClientEventListener.REQUEST_COUNT, 1));
      emitter.emit(
          K8sClientEventListener.requestMetric(apiRequest, -1)
                                .setDimension("errorType", K8sClientEventListener.errorType(chain.call(), e))
                                .setMetric(K8sClientEventListener.ERROR_COUNT, 1)
      );
      throw e;
    }

    emitter.emit(
        K8sClientEventListener.requestMetric(apiRequest, response.code())
                              .setMetric(K8sClientEventListener.REQUEST_COUNT, 1)
    );
    if (response.code() == 429) {
      emitter.emit(
          K8sClientEventListener.requestMetric(apiRequest, response.code())
                                .setMetric(K8sClientEventListener.THROTTLED_COUNT, 1)
      );
    }
    return response;
  }
}
