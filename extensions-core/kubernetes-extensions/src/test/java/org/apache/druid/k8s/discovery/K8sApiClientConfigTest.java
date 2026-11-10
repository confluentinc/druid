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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class K8sApiClientConfigTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testDefaults() throws Exception
  {
    final K8sApiClientConfig config = JSON_MAPPER.readValue("{}", K8sApiClientConfig.class);
    Assert.assertEquals(Duration.standardSeconds(10), config.getConnectTimeout());
    Assert.assertEquals(Duration.ZERO, config.getReadTimeout());
  }

  @Test
  public void testCustomValues() throws Exception
  {
    final K8sApiClientConfig config = JSON_MAPPER.readValue(
        "{\"connectTimeout\": \"PT5S\", \"readTimeout\": \"PT30S\"}",
        K8sApiClientConfig.class
    );
    Assert.assertEquals(Duration.standardSeconds(5), config.getConnectTimeout());
    Assert.assertEquals(Duration.standardSeconds(30), config.getReadTimeout());
  }

  @Test
  public void testNegativeTimeoutRejected()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new K8sApiClientConfig(Duration.millis(-1), null)
    );
  }
}
