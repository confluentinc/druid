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

import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DefaultK8sApiClientTest
{
  @Test
  public void testIsPodReady_nullStatus()
  {
    V1Pod pod = new V1Pod();
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_nullContainerStatuses()
  {
    V1Pod pod = pod("True");
    pod.getStatus().setContainerStatuses(null);
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_emptyContainerStatuses()
  {
    V1Pod pod = pod("True");
    pod.getStatus().setContainerStatuses(Collections.emptyList());
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsPodReady_readyConditionAndAllContainersReady()
  {
    Assert.assertTrue(DefaultK8sApiClient.isPodReady(pod("True", true, true)));
  }

  @Test
  public void testIsPodReady_containerNotReady()
  {
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod("True", false)));
  }

  @Test
  public void testIsPodReady_sidecarNotReady()
  {
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod("True", true, false)));
  }

  @Test
  public void testIsPodReady_containerReadyNull()
  {
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod("True", (Boolean) null)));
  }

  @Test
  public void testIsPodReady_readyConditionFalseWithStaleReadyContainers()
  {
    // What the node controller leaves behind when a node stops reporting.
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod("False", true, true)));
  }

  @Test
  public void testIsPodReady_readyConditionUnknown()
  {
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod("Unknown", true)));
  }

  @Test
  public void testIsPodReady_readyConditionMissing()
  {
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod(null, true)));
  }

  @Test
  public void testIsPodReady_onlyOtherConditionsTrue()
  {
    V1Pod pod = pod(null, true);
    pod.getStatus().setConditions(Collections.singletonList(new V1PodCondition().type("ContainersReady").status("True")));
    Assert.assertFalse(DefaultK8sApiClient.isPodReady(pod));
  }

  @Test
  public void testIsTerminatingBeyondCheckDuration_noDeletionTimestamp()
  {
    Assert.assertFalse(DefaultK8sApiClient.isTerminatingBeyondCheckDuration(terminatingPod(null), Duration.standardMinutes(10)));
  }

  @Test
  public void testIsTerminatingBeyondCheckDuration_pastThreshold()
  {
    V1Pod pod = terminatingPod(OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(10));
    Assert.assertTrue(DefaultK8sApiClient.isTerminatingBeyondCheckDuration(pod, Duration.standardMinutes(1)));
  }

  @Test
  public void testIsTerminatingBeyondCheckDuration_withinThreshold()
  {
    V1Pod pod = terminatingPod(OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(1));
    Assert.assertFalse(DefaultK8sApiClient.isTerminatingBeyondCheckDuration(pod, Duration.standardMinutes(10)));
  }

  @Test
  public void testIsTerminatingBeyondCheckDuration_nullDurationDefaultsTo30Seconds()
  {
    Assert.assertTrue(DefaultK8sApiClient.isTerminatingBeyondCheckDuration(terminatingPod(OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(10)), null));
    Assert.assertFalse(DefaultK8sApiClient.isTerminatingBeyondCheckDuration(terminatingPod(OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(10)), null));
  }

  private static V1Pod terminatingPod(@Nullable OffsetDateTime deletionTimestamp)
  {
    return new V1Pod().metadata(new V1ObjectMeta().deletionTimestamp(deletionTimestamp)).spec(new V1PodSpec());
  }

  private static V1Pod pod(@Nullable String readyCondition, Boolean... containerReady)
  {
    V1PodStatus status = new V1PodStatus();
    if (readyCondition != null) {
      status.setConditions(Collections.singletonList(new V1PodCondition().type("Ready").status(readyCondition)));
    }
    List<V1ContainerStatus> containerStatuses = new ArrayList<>();
    for (Boolean ready : containerReady) {
      containerStatuses.add(new V1ContainerStatus().ready(ready));
    }
    status.setContainerStatuses(containerStatuses);
    V1Pod pod = new V1Pod();
    pod.setStatus(status);
    return pod;
  }
}
