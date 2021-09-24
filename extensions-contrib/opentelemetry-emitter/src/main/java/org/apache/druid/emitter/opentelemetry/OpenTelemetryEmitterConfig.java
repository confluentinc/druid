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

package org.apache.druid.emitter.opentelemetry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class OpenTelemetryEmitterConfig
{
  @JsonProperty
  private final String protocol;

  @JsonProperty
  private final String endpoint;

  @JsonProperty
  private final String exporter;

  @JsonCreator
  public OpenTelemetryEmitterConfig(
      @JsonProperty("protocol") @Nullable String protocol,
      @JsonProperty("endpoint") @Nullable String endpoint,
      @JsonProperty("exporter") @Nullable String exporter
  )
  {
    this.protocol = protocol;
    this.endpoint = endpoint;
    this.exporter = exporter;
  }

  public String getProtocol()
  {
    return protocol;
  }

  public String getEndpoint()
  {
    return endpoint;
  }

  public String getExporter()
  {
    return exporter;
  }
}
