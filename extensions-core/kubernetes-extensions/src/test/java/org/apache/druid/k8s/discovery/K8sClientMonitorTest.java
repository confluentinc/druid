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

import okhttp3.OkHttpClient;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

public class K8sClientMonitorTest
{
  @Test
  public void testEmitsNothingBeforeClientIsBuilt()
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    Assert.assertTrue(new K8sClientMonitor().doMonitor(emitter));
    Assert.assertTrue(emitter.getEvents().isEmpty());
  }

  @Test
  public void testEmitsPoolAndInflightGauges()
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final K8sClientMonitor monitor = new K8sClientMonitor();
    monitor.setHttpClient(new OkHttpClient());
    monitor.getInflightCounter().set(2);

    Assert.assertTrue(monitor.doMonitor(emitter));

    emitter.verifyValue(K8sClientMonitor.CONNECTION_OPEN, 0);
    emitter.verifyValue(K8sClientMonitor.CONNECTION_IDLE, 0);
    emitter.verifyValue(K8sClientMonitor.INFLIGHT, 2);
  }
}
