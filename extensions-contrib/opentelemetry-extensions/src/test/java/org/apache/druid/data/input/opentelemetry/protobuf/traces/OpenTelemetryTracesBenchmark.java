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
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Fork(1)
@State(Scope.Benchmark)
public class OpenTelemetryTracesBenchmark
{
  private static ByteBuffer BUFFER;

  @Param(value = {"1", "2", "4", "8" })
  private int resourceSpanCount = 1;

  @Param(value = {"1"})
  private int instrumentationScopeCount = 1;

  @Param(value = {"1", "2", "4", "8" })
  private int spansCount = 1;

  private static final long START = TimeUnit.MILLISECONDS.toNanos(Instant.parse("2019-07-12T09:30:01.123Z")
                                                                         .toEpochMilli());

  private static final long END = START + 1_000_000L;

  private static final OpenTelemetryTracesProtobufInputFormat INPUT_FORMAT =
      new OpenTelemetryTracesProtobufInputFormat(
          "test.span.",
          "test.resource.",
          "test.name",
          "test.span_id",
          "test.parent.span",
          "test.trace.id",
          "test.end.time",
          "test.status.code",
          "test.status.message",
          "test.kind"
      );

  private static final InputRowSchema ROW_SCHEMA = new InputRowSchema(null,
                         new DimensionsSpec(ImmutableList.of(
                             new StringDimensionSchema("name"),
                             new StringDimensionSchema("span.id"),
                             new StringDimensionSchema("foo_key"))),
                         null);

  private ByteBuffer createTracesBuffer()
  {
    TracesData.Builder tracesData = TracesData.newBuilder();
    for (int i = 0; i < resourceSpanCount; i++) {
      ResourceSpans.Builder resourceSpansBuilder = tracesData.addResourceSpansBuilder();
      Resource.Builder resourceBuilder = resourceSpansBuilder.getResourceBuilder();

      for (int resourceAttributeI = 0; resourceAttributeI < 5; resourceAttributeI++) {
        KeyValue.Builder resourceAttributeBuilder = resourceBuilder.addAttributesBuilder();
        resourceAttributeBuilder.setKey("resource.label_key_" + resourceAttributeI);
        resourceAttributeBuilder.setValue(AnyValue.newBuilder().setStringValue("resource.label_value"));
      }

      for (int j = 0; j < instrumentationScopeCount; j++) {
        ScopeSpans.Builder scopeSpansBuilder = resourceSpansBuilder.addScopeSpansBuilder();

        for (int k = 0; k < spansCount; k++) {
          Span.Builder spanBuilder = scopeSpansBuilder.addSpansBuilder();
          spanBuilder.setStartTimeUnixNano(START)
                     .setEndTimeUnixNano(END)
                     .setStatus(Status.newBuilder().setCodeValue(100).setMessage("Dummy").build())
                     .setName("spanName")
                     .setSpanId(ByteString.copyFrom("Span-Id", StandardCharsets.UTF_8))
                     .setParentSpanId(ByteString.copyFrom("Parent-Span-Id", StandardCharsets.UTF_8))
                     .setTraceId(ByteString.copyFrom("Trace-Id", StandardCharsets.UTF_8))
                     .setKind(Span.SpanKind.SPAN_KIND_CONSUMER);

          for (int spanAttributeI = 0; spanAttributeI < 10; spanAttributeI++) {
            KeyValue.Builder attributeBuilder = spanBuilder.addAttributesBuilder();
            attributeBuilder.setKey("foo_key_" + spanAttributeI);
            attributeBuilder.setValue(AnyValue.newBuilder().setStringValue("foo-value"));
          }
        }
      }
    }
    return ByteBuffer.wrap(tracesData.build().toByteArray());
  }

  @Setup
  public void init()
  {
    BUFFER = createTracesBuffer();
  }

  @Benchmark
  public void measureSerde(Blackhole blackhole) throws IOException
  {
    for (CloseableIterator<InputRow> it = INPUT_FORMAT.createReader(ROW_SCHEMA, new ByteEntity(BUFFER), null).read(); it.hasNext(); ) {
      InputRow row = it.next();
      blackhole.consume(row);
    }
  }
}
