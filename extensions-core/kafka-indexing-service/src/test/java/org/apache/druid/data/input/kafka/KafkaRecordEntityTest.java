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

package org.apache.druid.data.input.kafka;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class KafkaRecordEntityTest
{
  @Test
  void testGetHeader()
  {
    RecordHeaders headers = new RecordHeaders();
    headers.add("metric.name", StringUtils.toUtf8("a"));
    Assertions.assertArrayEquals(StringUtils.toUtf8("a"), entity(headers).getHeader("metric.name"));
  }

  @Test
  void testGetHeaderReturnsLastValue()
  {
    RecordHeaders headers = new RecordHeaders();
    headers.add("k", StringUtils.toUtf8("first"));
    headers.add("k", StringUtils.toUtf8("second"));
    Assertions.assertArrayEquals(StringUtils.toUtf8("second"), entity(headers).getHeader("k"));
  }

  @Test
  void testGetHeaderMissing()
  {
    Assertions.assertNull(entity(new RecordHeaders()).getHeader("metric.name"));
  }

  private static KafkaRecordEntity entity(RecordHeaders headers)
  {
    return new KafkaRecordEntity(
        new ConsumerRecord<>("topic", 0, 0, 0L, TimestampType.CREATE_TIME, -1, -1, null, new byte[]{1}, headers, Optional.empty())
    );
  }
}
