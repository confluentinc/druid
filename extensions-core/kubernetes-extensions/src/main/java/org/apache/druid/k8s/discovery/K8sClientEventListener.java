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

import okhttp3.Call;
import okhttp3.EventListener;
import okhttp3.Handshake;
import okhttp3.Protocol;
import okhttp3.Response;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Emits request, error and connection metrics for each call the Kubernetes API client makes. OkHttp creates one
 * listener per call through {@link #factory(ServiceEmitter)}, and calls it on the thread running that call.
 *
 * <p>A request is counted when it completes, so its time includes reading the whole response body. A watch stays open
 * until the apiserver or Druid closes it, so it is counted once its response headers arrive, and not timed.
 */
public class K8sClientEventListener extends EventListener
{
  public static final String REQUEST_COUNT = "k8s/client/request/count";
  public static final String REQUEST_TIME = "k8s/client/request/time";
  public static final String THROTTLED_COUNT = "k8s/client/throttled/count";
  public static final String ERROR_COUNT = "k8s/client/error/count";
  public static final String RETRY_COUNT = "k8s/client/retry/count";
  public static final String CONNECTION_NEW = "k8s/client/connection/new";
  public static final String CONNECTION_FAILED = "k8s/client/connection/failed";
  public static final String TLS_HANDSHAKE_TIME = "k8s/client/tlsHandshake/time";

  static final String PEER = "kube-apiserver";
  static final String STATUS_CLASS_ERROR = "error";

  private final ServiceEmitter emitter;
  private final AtomicInteger inflight;
  private final K8sApiRequest request;
  private final long callStartNanos = System.nanoTime();
  private long secureConnectStartNanos;
  private int requestAttempts;
  private int responseCode = -1;

  K8sClientEventListener(ServiceEmitter emitter, AtomicInteger inflight, Call call)
  {
    this.emitter = emitter;
    this.inflight = inflight;
    this.request = K8sApiRequest.from(call.request());
  }

  /**
   * @param inflight incremented when a call starts and decremented when it ends, so it counts calls in progress,
   *                 including open watches.
   */
  public static EventListener.Factory factory(ServiceEmitter emitter, AtomicInteger inflight)
  {
    return call -> new K8sClientEventListener(emitter, inflight, call);
  }

  @Override
  public void callStart(Call call)
  {
    inflight.incrementAndGet();
  }

  @Override
  public void requestHeadersStart(Call call)
  {
    requestAttempts++;
  }

  @Override
  public void responseHeadersEnd(Call call, Response response)
  {
    // Redirects and auth challenges produce several responses for one call; the last one is the call's result.
    responseCode = response.code();
    if (responseCode == 429) {
      emitter.emit(requestMetric().setMetric(THROTTLED_COUNT, 1));
    }
    if (request.isWatch()) {
      emitRequestCount();
    }
  }

  @Override
  public void callEnd(Call call)
  {
    inflight.decrementAndGet();
    if (!request.isWatch()) {
      emitRequestCount();
      emitter.emit(requestMetric().setMetric(REQUEST_TIME, elapsedMillis(callStartNanos)));
    }
    emitRetries();
  }

  @Override
  public void callFailed(Call call, IOException ioe)
  {
    inflight.decrementAndGet();
    // A watch that already got its response headers was counted then; ending it, by Druid or the apiserver, is not
    // a failed request.
    if (request.isWatch() && responseCode != -1) {
      return;
    }
    emitRequestCount();
    if (!request.isWatch()) {
      emitter.emit(requestMetric().setMetric(REQUEST_TIME, elapsedMillis(callStartNanos)));
    }
    emitter.emit(requestMetric().setDimension("errorType", errorType(call, ioe)).setMetric(ERROR_COUNT, 1));
    emitRetries();
  }

  @Override
  public void secureConnectStart(Call call)
  {
    secureConnectStartNanos = System.nanoTime();
  }

  @Override
  public void secureConnectEnd(Call call, @Nullable Handshake handshake)
  {
    emitter.emit(connectionMetric().setMetric(TLS_HANDSHAKE_TIME, elapsedMillis(secureConnectStartNanos)));
  }

  @Override
  public void connectEnd(Call call, InetSocketAddress inetSocketAddress, Proxy proxy, @Nullable Protocol protocol)
  {
    emitter.emit(connectionMetric().setMetric(CONNECTION_NEW, 1));
  }

  @Override
  public void connectFailed(
      Call call,
      InetSocketAddress inetSocketAddress,
      Proxy proxy,
      @Nullable Protocol protocol,
      IOException ioe
  )
  {
    emitter.emit(connectionMetric().setDimension("errorType", errorType(call, ioe)).setMetric(CONNECTION_FAILED, 1));
  }

  private void emitRequestCount()
  {
    emitter.emit(requestMetric().setMetric(REQUEST_COUNT, 1));
  }

  /**
   * Counts requests the HTTP client sent more than once for one call: retries after connection failures, redirects
   * and auth challenges.
   */
  private void emitRetries()
  {
    if (requestAttempts > 1) {
      emitter.emit(requestMetric().setMetric(RETRY_COUNT, requestAttempts - 1));
    }
  }

  private ServiceMetricEvent.Builder requestMetric()
  {
    return connectionMetric()
        .setDimension("verb", request.getVerb())
        .setDimension("resource", request.getResource())
        .setDimension("route", request.getRoute())
        .setDimension("statusClass", responseCode == -1 ? STATUS_CLASS_ERROR : (responseCode / 100) + "xx")
        .setDimension("code", responseCode == -1 ? STATUS_CLASS_ERROR : String.valueOf(responseCode));
  }

  private ServiceMetricEvent.Builder connectionMetric()
  {
    return ServiceMetricEvent.builder()
                             .setDimension("client", request.getClient())
                             .setDimension("peer", PEER);
  }

  static String errorType(Call call, IOException ioe)
  {
    if (call.isCanceled()) {
      return "canceled";
    } else if (ioe instanceof SocketTimeoutException) {
      return "timeout";
    } else if (ioe instanceof ConnectException) {
      return "connectionRefused";
    } else if (ioe instanceof UnknownHostException) {
      return "unknownHost";
    } else if (ioe instanceof SSLException) {
      return "tls";
    } else {
      return "io";
    }
  }

  private static long elapsedMillis(long startNanos)
  {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }
}
