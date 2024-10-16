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

package org.apache.druid.data.input.opencensus.protobuf;

import com.google.common.collect.ImmutableList;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.curator.shaded.com.google.common.base.Predicate;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;


public class OpenCensusProtobufReaderTest
{
  private static final long TIMESTAMP = TimeUnit.MILLISECONDS.toNanos(Instant.parse("2019-07-12T09:30:01.123Z").toEpochMilli());
  public static final String RESOURCE_ATTRIBUTE_COUNTRY = "country";
  public static final String RESOURCE_ATTRIBUTE_VALUE_USA = "usa";

  public static final String RESOURCE_ATTRIBUTE_ENV = "env";
  public static final String RESOURCE_ATTRIBUTE_VALUE_DEVEL = "devel";

  public static final String INSTRUMENTATION_SCOPE_NAME = "mock-instr-lib";
  public static final String INSTRUMENTATION_SCOPE_VERSION = "1.0";

  public static final String METRIC_ATTRIBUTE_COLOR = "color";
  public static final String METRIC_ATTRIBUTE_VALUE_RED = "red";

  public static final String METRIC_ATTRIBUTE_FOO_KEY = "foo_key";
  public static final String METRIC_ATTRIBUTE_FOO_VAL = "foo_value";

  private final MetricsData.Builder metricsDataBuilder = MetricsData.newBuilder();

  private final Metric.Builder metricBuilder = metricsDataBuilder.addResourceMetricsBuilder()
      .addScopeMetricsBuilder()
      .addMetricsBuilder();

  private final DimensionsSpec dimensionsSpec = new DimensionsSpec(ImmutableList.of(
      new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_COLOR),
      new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_FOO_KEY),
      new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_ENV),
      new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_COUNTRY)
  ));

  public static final String TOPIC = "telemetry.metrics.otel";
  public static final int PARTITION = 2;
  public static final long OFFSET = 13095752723L;
  public static final long TS = 1643974867555L;
  public static final TimestampType TSTYPE = TimestampType.CREATE_TIME;
  public static final byte[] V0_HEADER_BYTES = ByteBuffer.allocate(Integer.BYTES)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putInt(1)
    .array();
  private static final Header HEADERV1 = new RecordHeader("v", V0_HEADER_BYTES);
  private static final Headers HEADERS = new RecordHeaders(new Header[]{HEADERV1});

  @Before
  public void setUp()
  {
    metricsDataBuilder
        .getResourceMetricsBuilder(0)
        .getResourceBuilder()
        .addAttributes(KeyValue.newBuilder()
          .setKey(RESOURCE_ATTRIBUTE_COUNTRY)
          .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_USA)));

    metricsDataBuilder
      .getResourceMetricsBuilder(0)
      .getScopeMetricsBuilder(0)
      .getScopeBuilder()
      .setName(INSTRUMENTATION_SCOPE_NAME)
      .setVersion(INSTRUMENTATION_SCOPE_VERSION);

  }

  @Test
  public void testSumWithAttributes() throws IOException
  {
    metricBuilder
        .setName("example_sum")
        .getSumBuilder()
        .addDataPointsBuilder()
        .setAsInt(6)
        .setTimeUnixNano(TIMESTAMP)
        .addAttributesBuilder() // test sum with attributes
        .setKey(METRIC_ATTRIBUTE_COLOR)
        .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());


    MetricsData metricsData = metricsDataBuilder.build();
    ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET, TS, TSTYPE, -1, -1,
        null, metricsData.toByteArray(), HEADERS, Optional.empty());
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat(
        "metric.name",
        null,
        "descriptor.",
        "custom."
    );

    SettableByteEntity<KafkaRecordEntity> entity = new SettableByteEntity<>();
    InputEntityReader reader = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
    ), entity, null);

    entity.setEntity(new KafkaRecordEntity(consumerRecord));
    try (CloseableIterator<InputRow> rows = reader.read()) {
      List<InputRow> rowList = new ArrayList<>();
      rows.forEachRemaining(rowList::add);
      Assert.assertEquals(1, rowList.size());

      InputRow row = rowList.get(0);
      Assert.assertEquals(4, row.getDimensions().size());
      assertDimensionEquals(row, "metric.name", "example_sum");
      assertDimensionEquals(row, "custom.country", "usa");
      assertDimensionEquals(row, "descriptor.color", "red");
      assertDimensionEquals(row, "value", "6");
    }
  }

  @Test
  public void testGaugeWithAttributes() throws IOException
  {
    metricBuilder.setName("example_gauge")
      .getGaugeBuilder()
      .addDataPointsBuilder()
      .setAsInt(6)
      .setTimeUnixNano(TIMESTAMP)
      .addAttributesBuilder() // test sum with attributes
      .setKey(METRIC_ATTRIBUTE_COLOR)
      .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    MetricsData metricsData = metricsDataBuilder.build();
    ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET, TS, TSTYPE, -1, -1,
        null, metricsData.toByteArray(), HEADERS, Optional.empty());
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
        "descriptor.",
        "custom.");
    SettableByteEntity<KafkaRecordEntity> entity = new SettableByteEntity<>();
    InputEntityReader reader = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
    ), entity, null);

    entity.setEntity(new KafkaRecordEntity(consumerRecord));
    try (CloseableIterator<InputRow> rows = reader.read()) {
      Assert.assertTrue(rows.hasNext());
      InputRow row = rows.next();

      Assert.assertEquals(4, row.getDimensions().size());
      assertDimensionEquals(row, "metric.name", "example_gauge");
      assertDimensionEquals(row, "custom.country", "usa");
      assertDimensionEquals(row, "descriptor.color", "red");
      assertDimensionEquals(row, "value", "6");
    }
  }

  @Test
  public void testBatchedMetricParse() throws IOException
  {
    metricBuilder.setName("example_sum")
      .getSumBuilder()
      .addDataPointsBuilder()
      .setAsInt(6)
      .setTimeUnixNano(TIMESTAMP)
      .addAttributesBuilder() // test sum with attributes
      .setKey(METRIC_ATTRIBUTE_COLOR)
      .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    // Create Second Metric
    Metric.Builder gaugeMetricBuilder = metricsDataBuilder.addResourceMetricsBuilder()
        .addScopeMetricsBuilder()
        .addMetricsBuilder();

    metricsDataBuilder.getResourceMetricsBuilder(1)
        .getResourceBuilder()
        .addAttributes(KeyValue.newBuilder()
          .setKey(RESOURCE_ATTRIBUTE_ENV)
          .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_DEVEL))
          .build());

    metricsDataBuilder.getResourceMetricsBuilder(1)
      .getScopeMetricsBuilder(0)
      .getScopeBuilder()
      .setName(INSTRUMENTATION_SCOPE_NAME)
      .setVersion(INSTRUMENTATION_SCOPE_VERSION);

    gaugeMetricBuilder.setName("example_gauge")
      .getGaugeBuilder()
      .addDataPointsBuilder()
      .setAsInt(8)
      .setTimeUnixNano(TIMESTAMP)
      .addAttributesBuilder() // test sum with attributes
      .setKey(METRIC_ATTRIBUTE_FOO_KEY)
      .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_FOO_VAL).build());

    MetricsData metricsData = metricsDataBuilder.build();
    ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET, TS, TSTYPE, -1, -1,
        null, metricsData.toByteArray(), HEADERS, Optional.empty());
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
         "descriptor.",
        "custom.");
    SettableByteEntity<KafkaRecordEntity> entity = new SettableByteEntity<>();
    InputEntityReader reader = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
    ), entity, null);

    entity.setEntity(new KafkaRecordEntity(consumerRecord));
    try (CloseableIterator<InputRow> rows = reader.read()) {
      Assert.assertTrue(rows.hasNext());
      InputRow row = rows.next();

      Assert.assertEquals(4, row.getDimensions().size());
      assertDimensionEquals(row, "metric.name", "example_sum");
      assertDimensionEquals(row, "custom.country", "usa");
      assertDimensionEquals(row, "descriptor.color", "red");
      assertDimensionEquals(row, "value", "6");

      Assert.assertTrue(rows.hasNext());
      row = rows.next();
      Assert.assertEquals(4, row.getDimensions().size());
      assertDimensionEquals(row, "metric.name", "example_gauge");
      assertDimensionEquals(row, "custom.env", "devel");
      assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
      assertDimensionEquals(row, "value", "8");
    }
  }

  @Test
  public void testDimensionSpecExclusions() throws IOException
  {
    metricsDataBuilder.getResourceMetricsBuilder(0)
      .getResourceBuilder()
      .addAttributesBuilder()
      .setKey(RESOURCE_ATTRIBUTE_ENV)
      .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_DEVEL).build());

    metricBuilder.setName("example_gauge")
        .getGaugeBuilder()
        .addDataPointsBuilder()
        .setAsInt(6)
        .setTimeUnixNano(TIMESTAMP)
        .addAllAttributes(ImmutableList.of(
          KeyValue.newBuilder()
            .setKey(METRIC_ATTRIBUTE_COLOR)
            .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build()).build(),
          KeyValue.newBuilder()
            .setKey(METRIC_ATTRIBUTE_FOO_KEY)
            .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_FOO_VAL).build()).build()));

    DimensionsSpec dimensionsSpecWithExclusions = DimensionsSpec.builder().setDimensionExclusions(
        ImmutableList.of(
          "descriptor." + METRIC_ATTRIBUTE_COLOR,
          "custom." + RESOURCE_ATTRIBUTE_COUNTRY
        )).build();

    MetricsData metricsData = metricsDataBuilder.build();
    ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET, TS, TSTYPE, -1, -1,
        null, metricsData.toByteArray(), HEADERS, Optional.empty());
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
        "descriptor.",
        "custom.");

    SettableByteEntity<KafkaRecordEntity> entity = new SettableByteEntity<>();
    InputEntityReader reader = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpecWithExclusions,
        ColumnsFilter.all()
    ), entity, null);

    entity.setEntity(new KafkaRecordEntity(consumerRecord));
    try (CloseableIterator<InputRow> rows = reader.read()) {
      Assert.assertTrue(rows.hasNext());
      InputRow row = rows.next();

      Assert.assertEquals(4, row.getDimensions().size());
      assertDimensionEquals(row, "metric.name", "example_gauge");
      assertDimensionEquals(row, "value", "6");
      assertDimensionEquals(row, "custom.env", "devel");
      assertDimensionEquals(row, "descriptor.foo_key", "foo_value");
      Assert.assertFalse(row.getDimensions().contains("custom.country"));
      Assert.assertFalse(row.getDimensions().contains("descriptor.color"));
    }
  }

  @Test
  public void testInvalidProtobuf() throws IOException
  {
    byte[] invalidProtobuf = new byte[] {0x00, 0x01};
    ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET, TS, TSTYPE, -1, -1,
        null, invalidProtobuf, HEADERS, Optional.empty());
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
        null,
        "descriptor.",
        "custom.");

    SettableByteEntity<KafkaRecordEntity> entity = new SettableByteEntity<>();
    InputEntityReader reader = inputFormat.createReader(new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        dimensionsSpec,
        ColumnsFilter.all()
    ), entity, null);

    entity.setEntity(new KafkaRecordEntity(consumerRecord));
    try (CloseableIterator<InputRow> rows = reader.read()) {
      Assert.assertThrows(ParseException.class, () -> rows.hasNext());
      Assert.assertThrows(NoSuchElementException.class, () -> rows.next());
    }
  }

  @Test
  public void testMultipleInvalidProtobuf() throws IOException
  {
    byte[] invalidProtobuf = new byte[] {0x00, 0x01};
    byte[] validProtobuf = new byte[] {};
    ConsumerRecord<byte[], byte[]> invalidConsumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET, TS, TSTYPE, -1, -1,
            null, invalidProtobuf, HEADERS, Optional.empty());
    ConsumerRecord<byte[], byte[]> validConsumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET + 1, TS, TSTYPE, -1, -1,
            null, validProtobuf, HEADERS, Optional.empty());
    List<OrderedPartitionableRecord<Integer, Long, KafkaRecordEntity>> records = new ArrayList<>();
    records.add(new OrderedPartitionableRecord<>(
            invalidConsumerRecord.topic(),
            invalidConsumerRecord.partition(),
            invalidConsumerRecord.offset(),
            ImmutableList.of(new KafkaRecordEntity(invalidConsumerRecord))
    ));
    records.add(new OrderedPartitionableRecord<>(
            validConsumerRecord.topic(),
            validConsumerRecord.partition(),
            validConsumerRecord.offset(),
            ImmutableList.of(new KafkaRecordEntity(validConsumerRecord))
    ));
    int recordsProcessed = 0;
    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name",
            null,
            "descriptor.",
            "custom.");
    for (OrderedPartitionableRecord<Integer, Long, KafkaRecordEntity> record : records) {

      SettableByteEntity<ByteEntity> entity = new SettableByteEntity<>();
      OpenCensusProtobufReader readR = new OpenCensusProtobufReader(
              dimensionsSpec,
              entity,
              "metric.name",
              "descriptor.",
              "custom."

      );
      InputEntityReader reader = inputFormat.createReader(new InputRowSchema(
              new TimestampSpec("timestamp", "iso", null),
              dimensionsSpec,
              ColumnsFilter.all()
      ), entity, null);
      System.out.println("Processing record " + record.getSequenceNumber());
      final List<InputRow> rows = new ArrayList<>();
      for (ByteEntity byteEntity : record.getData()) {
        System.out.println("Processing byte entity " + record.getData().indexOf(byteEntity));
        entity.setEntity(byteEntity);
        try (FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
                readR.read(),
                mock(Predicate.class),
                mock(RowIngestionMeters.class),
                mock(ParseExceptionHandler.class)
        )) {
          rowIterator.forEachRemaining(rows::add);
        }
      }
      recordsProcessed += 1;
    }
    Assert.assertEquals(recordsProcessed, 2);
  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }

}
