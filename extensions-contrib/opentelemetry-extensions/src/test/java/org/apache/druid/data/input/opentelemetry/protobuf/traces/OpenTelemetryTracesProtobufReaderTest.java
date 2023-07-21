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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.apache.commons.codec.binary.Hex;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.druid.data.input.opentelemetry.protobuf.TestUtils.assertDimensionEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OpenTelemetryTracesProtobufReaderTest
{
  private static final String INSTRUMENTATION_SCOPE_NAME = "mock-instr-lib";
  private static final String INSTRUMENTATION_SCOPE_VERSION = "1.0";

  private static final String ATTRIBUTE_NAMESPACE = "namespace";
  private static final String ATTRIBUTE_VALUE_NAMESPACE = "namespace_val";
  private static final String ATTRIBUTE_SERVICE = "service";
  private static final String ATTRIBUTE_VALUE_TEST = "test-service";

  private static final String NAME_VALUE = "span-name";
  private static final byte[] SPAN_ID_VALUE = "abcd".getBytes(StandardCharsets.UTF_8);
  private static final String SPAN_ID_VALUE_HEX = Hex.encodeHexString(SPAN_ID_VALUE);
  private static final byte[] PARENT_ID_VALUE = "1234".getBytes(StandardCharsets.UTF_8);
  private static final String PARENT_ID_VALUE_HEX = Hex.encodeHexString(PARENT_ID_VALUE);
  private static final byte[] TRACE_ID_VALUE = "zyxwvutsrqponmlk".getBytes(StandardCharsets.UTF_8);
  private static final String TRACE_ID_VALUE_HEX = Hex.encodeHexString(TRACE_ID_VALUE);
  private static final Span.SpanKind SPAN_KIND_VALUE = Span.SpanKind.SPAN_KIND_SERVER;

  private final long now = System.nanoTime();
  private final long before = now - 1000;
  private final TracesData.Builder dataBuilder = TracesData.newBuilder();

  private final Span.Builder spanBuilder = dataBuilder
      .addResourceSpansBuilder()
      .addScopeSpansBuilder()
      .addSpansBuilder();

  private final String spanAttributePrefix = "span.";
  private final String resourceAttributePrefix = "resource.";
  private final String spanNameDimension = "name";
  private final String spanIdDimension = "span_id";
  private final String parentSpanIdDimension = "parent_span_id";
  private final String traceIdDimension = "trace_id";
  private final String endTimeDimension = "end_time";
  private final String statusCodeDimension = "status_code";
  private final String statusMessageDimension = "status_message";
  private final String kindDimension = "kind";

  private final DimensionsSpec dimensionsSpec = new DimensionsSpec(ImmutableList.of(
      new StringDimensionSchema("resource." + ATTRIBUTE_NAMESPACE),
      new StringDimensionSchema("span." + ATTRIBUTE_SERVICE)
  ));

  @Before
  public void setUp()
  {
    dataBuilder
        .getResourceSpansBuilder(0)
        .getResourceBuilder()
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(ATTRIBUTE_NAMESPACE)
                    .setValue(AnyValue.newBuilder().setStringValue(ATTRIBUTE_VALUE_NAMESPACE))
                    .build());

    dataBuilder
        .getResourceSpansBuilder(0)
        .getScopeSpansBuilder(0)
        .getScopeBuilder()
        .setName(INSTRUMENTATION_SCOPE_NAME)
        .setVersion(INSTRUMENTATION_SCOPE_VERSION);

    spanBuilder
        .setStartTimeUnixNano(before)
        .setEndTimeUnixNano(now)
        .setName(NAME_VALUE)
        .setStatus(Status.newBuilder().setCodeValue(1).setMessage("OK").build())
        .setTraceId(ByteString.copyFrom(TRACE_ID_VALUE))
        .setSpanId(ByteString.copyFrom(SPAN_ID_VALUE))
        .setParentSpanId(ByteString.copyFrom(PARENT_ID_VALUE))
        .setKind(SPAN_KIND_VALUE)
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(ATTRIBUTE_SERVICE)
                    .setValue(AnyValue.newBuilder().setStringValue(ATTRIBUTE_VALUE_TEST))
                    .build());
  }

  private CloseableIterator<InputRow> getDataIterator(DimensionsSpec spec)
  {
    TracesData tracesData = dataBuilder.build();
    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(tracesData.toByteArray()));
    return new OpenTelemetryTracesProtobufReader(
        spec,
        settableByteEntity,
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
    ).read();
  }

  private void verifyDefaultFirstRowData(InputRow row)
  {
    assertDimensionEquals(row, "resource.namespace", ATTRIBUTE_VALUE_NAMESPACE);
    assertDimensionEquals(row, spanNameDimension, NAME_VALUE);
    assertDimensionEquals(row, spanIdDimension, SPAN_ID_VALUE_HEX);
    assertDimensionEquals(row, parentSpanIdDimension, PARENT_ID_VALUE_HEX);
    assertDimensionEquals(row, traceIdDimension, TRACE_ID_VALUE_HEX);
    assertDimensionEquals(row, endTimeDimension, Long.toString(TimeUnit.NANOSECONDS.toMillis(now)));
    assertDimensionEquals(row, statusCodeDimension, Integer.toString(1));
    assertDimensionEquals(row, statusMessageDimension, "OK");
    assertDimensionEquals(row, kindDimension, "SERVER");
  }

  @Test
  public void testTrace()
  {
    CloseableIterator<InputRow> rows = getDataIterator(dimensionsSpec);
    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    assertEquals(2, row.getDimensions().size());
    verifyDefaultFirstRowData(row);
    assertDimensionEquals(row, "span.service", ATTRIBUTE_VALUE_TEST);
  }

  @Test
  public void testBatchedTraceParse()
  {
    Span.Builder secondSpanBuilder = dataBuilder
        .addResourceSpansBuilder()
        .addScopeSpansBuilder()
        .addSpansBuilder();

    dataBuilder
        .getResourceSpansBuilder(1)
        .getResourceBuilder()
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(ATTRIBUTE_NAMESPACE)
                    .setValue(AnyValue.newBuilder().setStringValue(ATTRIBUTE_VALUE_NAMESPACE))
                    .build());
    dataBuilder
        .getResourceSpansBuilder(1)
        .getScopeSpansBuilder(0)
        .getScopeBuilder()
        .setName(INSTRUMENTATION_SCOPE_NAME)
        .setVersion(INSTRUMENTATION_SCOPE_VERSION);

    String spanName2 = "span-2";
    byte[] traceId2 = "trace-2".getBytes(StandardCharsets.UTF_8);
    byte[] spanId2 = "span-2".getBytes(StandardCharsets.UTF_8);
    byte[] parentId2 = "parent-2".getBytes(StandardCharsets.UTF_8);
    Span.SpanKind spanKind2 = Span.SpanKind.SPAN_KIND_CLIENT;
    String spanAttributeKey2 = "someIntAttribute";
    int spanAttributeVal2 = 23;
    String statusMessage2 = "NOT_OK";
    int statusCode2 = 400;

    secondSpanBuilder
        .setStartTimeUnixNano(before)
        .setEndTimeUnixNano(now)
        .setName(spanName2)
        .setStatus(Status.newBuilder().setCodeValue(statusCode2).setMessage(statusMessage2).build())
        .setTraceId(ByteString.copyFrom(traceId2))
        .setSpanId(ByteString.copyFrom(spanId2))
        .setParentSpanId(ByteString.copyFrom(parentId2))
        .setKind(spanKind2)
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(spanAttributeKey2)
                    .setValue(AnyValue.newBuilder().setIntValue(spanAttributeVal2))
                    .build());
    CloseableIterator<InputRow> rows = getDataIterator(dimensionsSpec);
    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    assertEquals(2, rowList.size());

    InputRow row = rowList.get(0);
    assertEquals(2, row.getDimensions().size());
    assertDimensionEquals(row, spanAttributePrefix + ATTRIBUTE_SERVICE, ATTRIBUTE_VALUE_TEST);
    verifyDefaultFirstRowData(row);

    row = rowList.get(1);
    assertEquals(2, row.getDimensions().size());
    assertDimensionEquals(row, resourceAttributePrefix + ATTRIBUTE_NAMESPACE,
                          ATTRIBUTE_VALUE_NAMESPACE);
    assertDimensionEquals(row, spanAttributePrefix + spanAttributeKey2,
                          Integer.toString(spanAttributeVal2));
    assertDimensionEquals(row, spanNameDimension, spanName2);
    assertDimensionEquals(row, spanIdDimension, Hex.encodeHexString(spanId2));
    assertDimensionEquals(row, parentSpanIdDimension, Hex.encodeHexString(parentId2));
    assertDimensionEquals(row, traceIdDimension, Hex.encodeHexString(traceId2));
    assertDimensionEquals(row, endTimeDimension, Long.toString(TimeUnit.NANOSECONDS.toMillis(now)));
    assertDimensionEquals(row, statusCodeDimension, Integer.toString(statusCode2));
    assertDimensionEquals(row, statusMessageDimension, statusMessage2);
    assertDimensionEquals(row, kindDimension, "CLIENT");
  }

  @Test
  public void testDimensionSpecExclusions()
  {
    String excludedAttribute = spanAttributePrefix + ATTRIBUTE_SERVICE;
    DimensionsSpec dimensionsSpecWithExclusions = DimensionsSpec.builder().setDimensionExclusions(ImmutableList.of(
        excludedAttribute
    )).build();
    CloseableIterator<InputRow> rows = getDataIterator(dimensionsSpecWithExclusions);
    assertTrue(rows.hasNext());
    InputRow row = rows.next();
    assertFalse(row.getDimensions().contains(excludedAttribute));
    assertEquals(9, row.getDimensions().size());
    verifyDefaultFirstRowData(row);
  }

  @Test
  public void testInvalidProtobuf() throws IOException
  {
    byte[] invalidProtobuf = new byte[] {0x04, 0x01};
    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(invalidProtobuf));
    try (CloseableIterator<InputRow> rows = new OpenTelemetryTracesProtobufReader(
        dimensionsSpec,
        settableByteEntity,
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
    ).read()) {
      Assert.assertThrows(ParseException.class, () -> rows.hasNext());
      Assert.assertThrows(ParseException.class, () -> rows.next());
    }
  }

  @Test
  public void testInvalidAttributeValueType()
  {
    String excludededAttributeKey = "array";
    spanBuilder
        .addAttributes(KeyValue.newBuilder()
                         .setKey(excludededAttributeKey)
                         .setValue(AnyValue.newBuilder().setArrayValue(ArrayValue.getDefaultInstance()))
                          .build());
    CloseableIterator<InputRow> rows = getDataIterator(dimensionsSpec);
    assertTrue(rows.hasNext());
    InputRow row = rows.next();
    assertDimensionEquals(row, spanAttributePrefix + ATTRIBUTE_SERVICE, ATTRIBUTE_VALUE_TEST);
    assertFalse(row.getDimensions().contains(spanAttributePrefix + excludededAttributeKey));
    verifyDefaultFirstRowData(row);
  }
}
