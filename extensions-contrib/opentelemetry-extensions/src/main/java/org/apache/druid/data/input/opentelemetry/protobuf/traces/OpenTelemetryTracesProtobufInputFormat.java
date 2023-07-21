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

package org.apache.druid.data.input.opentelemetry.protobuf.traces;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.opentelemetry.protobuf.Utils;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.util.Objects;

public class OpenTelemetryTracesProtobufInputFormat implements InputFormat
{
  static String DEFAULT_RESOURCE_ATTR_PREFIX = "resource.";
  static String DEFAULT_SPAN_NAME_DIMENSION = "span_name";
  static String DEFAULT_SPAN_ID_DIMENSION = "span_id";
  static String DEFAULT_PARENT_SPAN_ID_DIMENSION = "parent_span_id";
  static String DEFAULT_TRACE_ID_DIMENSION = "trace_id";
  static String DEFAULT_END_TIME_DIMENSION = "end_time";
  static String DEFAULT_STATUS_CODE_DIMENSION = "status.code";
  static String DEFAULT_STATUS_MESSAGE_DIMENSION = "status.message";
  static String DEFAULT_KIND_DIMENSION = "kind";

  private final String spanAttributePrefix;
  private final String resourceAttributePrefix;
  private final String spanNameDimension;
  private final String spanIdDimension;
  private final String parentSpanIdDimension;
  private final String traceIdDimension;
  private final String endTimeDimension;
  private final String statusCodeDimension;
  private final String statusMessageDimension;
  private final String kindDimension;

  @JsonProperty
  public String getSpanNameDimension()
  {
    return spanNameDimension;
  }

  @JsonProperty
  public String getResourceAttributePrefix()
  {
    return resourceAttributePrefix;
  }

  @JsonProperty
  public String getSpanIdDimension()
  {
    return spanIdDimension;
  }

  @JsonProperty
  public String getParentSpanIdDimension()
  {
    return parentSpanIdDimension;
  }

  @JsonProperty
  public String getTraceIdDimension()
  {
    return traceIdDimension;
  }

  @JsonProperty
  public String getEndTimeDimension()
  {
    return endTimeDimension;
  }

  @JsonProperty
  public String getStatusCodeDimension()
  {
    return statusCodeDimension;
  }

  @JsonProperty
  public String getStatusMessageDimension()
  {
    return statusMessageDimension;
  }

  @JsonProperty
  public String getKindDimension()
  {
    return kindDimension;
  }

  @JsonProperty
  public String getSpanAttributePrefix()
  {
    return spanAttributePrefix;
  }

  private String validateDimensionName(String input, String dimensionName)
  {
    Preconditions.checkArgument(!input.isEmpty(),
                                dimensionName + " dimension cannot be empty");
    return input;
  }

  public OpenTelemetryTracesProtobufInputFormat(
      @JsonProperty("spanAttributePrefix") String spanAttributePrefix,
      @JsonProperty("resourceAttributePrefix") String resourceAttributePrefix,
      @JsonProperty("spanNameDimension") String spanNameDimension,
      @JsonProperty("spanIdDimension") String spanIdDimension,
      @JsonProperty("parentSpanIdDimension") String parentSpanIdDimension,
      @JsonProperty("traceIdDimension") String traceIdDimension,
      @JsonProperty("endTimeDimension") String endTimeDimension,
      @JsonProperty("statusCodeDimension") String statusCodeDimension,
      @JsonProperty("statusMessageDimension") String statusMessageDimension,
      @JsonProperty("kindDimension") String kindDimension
  )
  {

    this.spanAttributePrefix = StringUtils.nullToEmptyNonDruidDataString(spanAttributePrefix);
    this.resourceAttributePrefix = resourceAttributePrefix == null ? DEFAULT_RESOURCE_ATTR_PREFIX : resourceAttributePrefix;

    this.spanNameDimension = spanNameDimension == null ? DEFAULT_SPAN_NAME_DIMENSION :
                             validateDimensionName(spanNameDimension, "spanNameDimension");
    this.spanIdDimension = spanIdDimension == null ? DEFAULT_SPAN_ID_DIMENSION :
                           validateDimensionName(spanIdDimension, "spanIdDimension");
    this.parentSpanIdDimension = parentSpanIdDimension == null ? DEFAULT_PARENT_SPAN_ID_DIMENSION :
                                 validateDimensionName(parentSpanIdDimension, "parentSpanIdDimension");
    this.traceIdDimension = traceIdDimension == null ? DEFAULT_TRACE_ID_DIMENSION :
                            validateDimensionName(traceIdDimension, "traceIdDimension");
    this.endTimeDimension = endTimeDimension == null ? DEFAULT_END_TIME_DIMENSION :
                            validateDimensionName(endTimeDimension, "endTimeDimension");
    this.statusCodeDimension = statusCodeDimension == null ? DEFAULT_STATUS_CODE_DIMENSION :
                               validateDimensionName(statusCodeDimension, "statusCodeDimension");
    this.statusMessageDimension = statusMessageDimension == null ? DEFAULT_STATUS_MESSAGE_DIMENSION :
                               validateDimensionName(statusMessageDimension, "statusMessageDimension");
    this.kindDimension = kindDimension == null ? DEFAULT_KIND_DIMENSION :
                         validateDimensionName(kindDimension, "kindDimension");

  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new OpenTelemetryTracesProtobufReader(
        inputRowSchema.getDimensionsSpec(),
        Utils.getSettableEntity(source),
        spanAttributePrefix,
        resourceAttributePrefix,
        spanNameDimension,
        spanIdDimension,
        parentSpanIdDimension,
        traceIdDimension,
        endTimeDimension,
        statusCodeDimension,
        statusMessageDimension,
        kindDimension
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpenTelemetryTracesProtobufInputFormat that = (OpenTelemetryTracesProtobufInputFormat) o;
    return Objects.equals(spanAttributePrefix, that.spanAttributePrefix)
           && Objects.equals(resourceAttributePrefix, that.resourceAttributePrefix)
           && Objects.equals(spanNameDimension, that.spanNameDimension)
           && Objects.equals(spanIdDimension, that.spanIdDimension)
           && Objects.equals(parentSpanIdDimension, that.parentSpanIdDimension)
           && Objects.equals(traceIdDimension, that.traceIdDimension)
           && Objects.equals(endTimeDimension, that.endTimeDimension)
           && Objects.equals(statusCodeDimension, that.statusCodeDimension)
           && Objects.equals(statusMessageDimension, that.statusMessageDimension)
           && Objects.equals(kindDimension, that.kindDimension);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        spanAttributePrefix,
        resourceAttributePrefix,
        spanNameDimension,
        spanIdDimension,
        parentSpanIdDimension,
        traceIdDimension,
        endTimeDimension,
        statusCodeDimension,
        statusMessageDimension,
        kindDimension
    );
  }
}
