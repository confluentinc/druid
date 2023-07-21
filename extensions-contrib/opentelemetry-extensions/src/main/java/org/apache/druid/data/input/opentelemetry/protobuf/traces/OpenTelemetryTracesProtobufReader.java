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

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.apache.commons.codec.binary.Hex;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.opentelemetry.protobuf.AbstractProtobufReader;
import org.apache.druid.data.input.opentelemetry.protobuf.Utils;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OpenTelemetryTracesProtobufReader extends AbstractProtobufReader
{
  private final String spanAttributePrefix;
  private final String resourceAttributePrefix;

  // Number of '*Dimension' variables
  private static final int DEFAULT_COLUMN_COUNT = 8;

  private final String spanNameDimension;
  private final String spanIdDimension;
  private final String parentSpanIdDimension;
  private final String traceIdDimension;

  private final String endTimeDimension;
  private final String statusCodeDimension;
  private final String statusMessageDimension;
  private final String kindDimension;

  public OpenTelemetryTracesProtobufReader(
      DimensionsSpec dimensionsSpec,
      SettableByteEntity<? extends ByteEntity> source,
      String spanAttributePrefix,
      String resourceAttributePrefix,
      String spanNameDimension,
      String spanIdDimension,
      String parentSpanIdDimension,
      String traceIdDimension,
      String endTimeDimension,
      String statusCodeDimension,
      String statusMessageDimension,
      String kindDimension
  )
  {
    super(dimensionsSpec, source);
    this.resourceAttributePrefix = resourceAttributePrefix;
    this.spanAttributePrefix = spanAttributePrefix;
    this.spanNameDimension = spanNameDimension;
    this.spanIdDimension = spanIdDimension;
    this.parentSpanIdDimension = parentSpanIdDimension;
    this.traceIdDimension = traceIdDimension;
    this.endTimeDimension = endTimeDimension;
    this.statusCodeDimension = statusCodeDimension;
    this.statusMessageDimension = statusMessageDimension;
    this.kindDimension = kindDimension;
  }

  @Override
  public List<InputRow> parseData(ByteBuffer byteBuffer)
      throws InvalidProtocolBufferException
  {
    return parseTracesData(TracesData.parseFrom(byteBuffer));
  }

  private List<InputRow> parseTracesData(final TracesData tracesData)
  {
    return tracesData.getResourceSpansList()
        .stream()
        .flatMap(resourceSpans -> {
          Map<String, Object> resourceAttributes = Utils.getResourceAttributes(resourceSpans.getResource(),
                                                                               resourceAttributePrefix);
          return resourceSpans.getScopeSpansList()
              .stream()
              .flatMap(scopeSpans -> scopeSpans.getSpansList()
                   .stream()
                   .map(span -> parseSpan(span, resourceAttributes)));
        })
        .collect(Collectors.toList());
  }

  private InputRow parseSpan(Span span, Map<String, Object> resourceAttributes)
  {
    int capacity = resourceAttributes.size() + span.getAttributesCount() +
                   DEFAULT_COLUMN_COUNT;
    Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
    event.put(spanNameDimension, span.getName());
    event.put(spanIdDimension, Hex.encodeHexString(span.getSpanId().asReadOnlyByteBuffer()));
    event.put(parentSpanIdDimension, Hex.encodeHexString(span.getParentSpanId().asReadOnlyByteBuffer()));
    event.put(traceIdDimension, Hex.encodeHexString(span.getTraceId().asReadOnlyByteBuffer()));
    event.put(endTimeDimension, TimeUnit.NANOSECONDS.toMillis(span.getEndTimeUnixNano()));
    event.put(statusCodeDimension, span.getStatus().getCodeValue());
    event.put(statusMessageDimension, span.getStatus().getMessage());

    String spanKind = span.getKind().toString();
    // remove the "SPAN_KIND_" prefix
    event.put(kindDimension, spanKind.substring(spanKind.lastIndexOf('_') + 1));
    event.putAll(resourceAttributes);
    span.getAttributesList().forEach(att -> {
      Object value = Utils.parseAnyValue(att.getValue());
      if (value != null) {
        event.put(spanAttributePrefix + att.getKey(), value);
      }
    });

    return createRow(TimeUnit.NANOSECONDS.toMillis(span.getStartTimeUnixNano()), event);
  }
}
