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

import org.junit.Assert;
import org.junit.Test;

public class K8sDiscoveryModuleTest
{
  @Test
  public void testBuildUserAgent()
  {
    final String userAgent = K8sDiscoveryModule.buildUserAgent("druid/broker", "test-cluster");
    Assert.assertTrue(userAgent, userAgent.startsWith("druid/"));
    Assert.assertTrue(userAgent, userAgent.endsWith(" (druid/broker; k8s-discovery; cluster=test-cluster)"));
  }

  @Test
  public void testBuildUserAgentSanitizesNonAsciiServiceName()
  {
    final String userAgent = K8sDiscoveryModule.buildUserAgent("druid/東京", "test-cluster");
    Assert.assertTrue(userAgent, userAgent.endsWith(" (druid/__; k8s-discovery; cluster=test-cluster)"));
    Assert.assertTrue(userAgent, userAgent.chars().allMatch(c -> c >= 0x20 && c < 0x7f));
  }

  @Test
  public void testSanitizeUserAgentValue()
  {
    Assert.assertEquals("druid/broker", K8sDiscoveryModule.sanitizeUserAgentValue("druid/broker"));
    Assert.assertEquals("a__b__c_d_", K8sDiscoveryModule.sanitizeUserAgentValue("a()b\\;c\nd\u00e9"));
  }
}
