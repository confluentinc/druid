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

package org.apache.druid.data.input.kafkainput;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class KafkaInputFormatTest
{
  private KafkaRecordEntity inputEntity;
  private long timestamp = DateTimes.of("2021-06-24").getMillis();
  private static final Iterable<Header> SAMPLE_HEADERS = ImmutableList.of(new Header() {
      @Override
      public String key()
      {
        return "encoding";
      }
      @Override
      public byte[] value()
      {
        return "application/json".getBytes(StandardCharsets.UTF_8);
      }
    },
      new Header() {
      @Override
      public String key()
      {
        return "kafkapkc";
      }
      @Override
      public byte[] value()
      {
        return "pkc-bar".getBytes(StandardCharsets.UTF_8);
      }
    });
  private KafkaInputFormat format;

  @Before
  public void setUp()
  {
    format = new KafkaInputFormat(
        new KafkaStringHeaderFormat(null),
        // Key Format
        new JsonInputFormat(
            new JSONPathSpec(true, ImmutableList.of()),
            null, null, false //make sure JsonReader is used
        ),
        // Value Format
        new JsonInputFormat(
            new JSONPathSpec(
                true,
                ImmutableList.of(
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                    new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                    new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                    new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                    new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                    new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
                )
            ),
            null, null, false //make sure JsonReader is used
        ),
        "kafka.newheader.", "kafka.newkey.", "kafka.newts."
    );
  }

  @Test
  public void testWithHeaderKeyAndValue() throws IOException
  {
    final byte[] key = StringUtils.toUtf8(
        "{\n"
        + "    \"key\": \"sampleKey\"\n"
        + "}");

    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-24\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}");

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = new KafkaRecordEntity(new ConsumerRecord<byte[], byte[]>(
        "sample", 0, 0, timestamp,
        null, null, 0, 0,
        key, payload, headers));

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of(
                "bar", "foo",
                "kafka.newheader.encoding",
                "kafka.newheader.kafkapkc",
                "kafka.newts.timestamp"
            ))),
            Collections.emptyList()
        ),
        inputEntity,
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();

        // Payload verifications
        Assert.assertEquals(DateTimes.of("2021-06-24"), row.getTimestamp());
        Assert.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assert.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));

        // Header verification
        Assert.assertEquals("application/json", Iterables.getOnlyElement(row.getDimension("kafka.newheader.encoding")));
        Assert.assertEquals("pkc-bar", Iterables.getOnlyElement(row.getDimension("kafka.newheader.kafkapkc")));
        Assert.assertEquals(String.valueOf(DateTimes.of("2021-06-24").getMillis()),
                            Iterables.getOnlyElement(row.getDimension("kafka.newts.timestamp")));

        // Key verification
        Assert.assertEquals("sampleKey", Iterables.getOnlyElement(row.getDimension("kafka.newkey.key")));

        Assert.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assert.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assert.assertTrue(row.getDimension("jq_omg2").isEmpty());

        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testWithOutKey() throws IOException
  {
    final byte[] payload = StringUtils.toUtf8(
        "{\n"
        + "    \"timestamp\": \"2021-06-24\",\n"
        + "    \"bar\": null,\n"
        + "    \"foo\": \"x\",\n"
        + "    \"baz\": 4,\n"
        + "    \"o\": {\n"
        + "        \"mg\": 1\n"
        + "    }\n"
        + "}");

    Headers headers = new RecordHeaders(SAMPLE_HEADERS);
    inputEntity = new KafkaRecordEntity(new ConsumerRecord<byte[], byte[]>(
        "sample", 0, 0, timestamp,
        null, null, 0, 0,
        null, payload, headers));

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of(
                "bar", "foo",
                "kafka.newheader.encoding",
                "kafka.newheader.kafkapkc",
                "kafka.newts.timestamp"
            ))),
            Collections.emptyList()
        ),
        inputEntity,
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();

        // Key verification
        Assert.assertTrue(row.getDimension("kafka.newkey.key").isEmpty());
        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }

  }
}
