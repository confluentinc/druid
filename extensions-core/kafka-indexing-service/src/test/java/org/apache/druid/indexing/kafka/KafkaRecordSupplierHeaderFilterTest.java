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

package org.apache.druid.indexing.kafka;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilteringConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test KafkaRecordSupplier with header-based filtering integrated into the main poll() method.
 */
public class KafkaRecordSupplierHeaderFilterTest
{
  private KafkaConsumer<byte[], byte[]> mockConsumer;

  private KafkaRecordSupplier recordSupplier;

  @BeforeClass
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  @Before
  public void setUp()
  {
    mockConsumer = EasyMock.createMock(KafkaConsumer.class);
  }

  @Test
  public void testNoHeaderFilter()
  {
    // Test that records are not filtered when no header filter is configured
    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, null, "test-datasource");

    ConsumerRecord<byte[], byte[]> record1 = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> record2 = createRecord("topic", 0, 101L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(record1, record2)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assert.assertEquals("Should include all records when no filter", 2, results.size());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInHeaderFilterSingleValue()
  {
    // Test filtering with in filter (single value)
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter, "test-datasource");

    ConsumerRecord<byte[], byte[]> prodRecord = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord = createRecord("topic", 0, 101L,
        headers("environment", "staging"));
    ConsumerRecord<byte[], byte[]> noHeaderRecord = createRecord("topic", 0, 102L,
        new RecordHeaders()); // No headers

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord, stagingRecord, noHeaderRecord)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    // With permissive filtering: production record + no-header record should be included
    Assert.assertEquals("Should include production record and no-header record", 2, results.size());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber()); // Production record
    Assert.assertEquals(102L, (long) results.get(1).getSequenceNumber()); // No-header record
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInHeaderFilterMultipleValues()
  {
    // Test filtering with in filter (multiple values)
    InDimFilter filter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter, "test-datasource");

    ConsumerRecord<byte[], byte[]> userServiceRecord = createRecord("topic", 0, 100L,
        headers("service", "user-service"));
    ConsumerRecord<byte[], byte[]> paymentServiceRecord = createRecord("topic", 0, 101L,
        headers("service", "payment-service"));
    ConsumerRecord<byte[], byte[]> orderServiceRecord = createRecord("topic", 0, 102L,
        headers("service", "order-service"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(userServiceRecord, paymentServiceRecord, orderServiceRecord)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assert.assertEquals("Should include user-service and payment-service records", 2, results.size());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testInFilterWithMultipleHeaders()
  {
    // Test InDimFilter with multiple possible values
    InDimFilter serviceFilter = new InDimFilter("service", Arrays.asList("user-service", "payment-service"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(serviceFilter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter, "test-datasource");

    ConsumerRecord<byte[], byte[]> userServiceRecord = createRecord("topic", 0, 100L,
        headers("service", "user-service"));
    ConsumerRecord<byte[], byte[]> paymentServiceRecord = createRecord("topic", 0, 101L,
        headers("service", "payment-service"));
    ConsumerRecord<byte[], byte[]> orderServiceRecord = createRecord("topic", 0, 102L,
        headers("service", "order-service"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(userServiceRecord, paymentServiceRecord, orderServiceRecord)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assert.assertEquals("Should include user-service and payment-service records", 2, results.size());
    Assert.assertEquals(100L, (long) results.get(0).getSequenceNumber());
    Assert.assertEquals(101L, (long) results.get(1).getSequenceNumber());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMultiplePolls()
  {
    // Test that statistics accumulate across multiple polls
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter, "test-datasource");

    // First poll
    ConsumerRecord<byte[], byte[]> prodRecord1 = createRecord("topic", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord1 = createRecord("topic", 0, 101L,
        headers("environment", "staging"));

    // Second poll
    ConsumerRecord<byte[], byte[]> prodRecord2 = createRecord("topic", 0, 102L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> stagingRecord2 = createRecord("topic", 0, 103L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord1, stagingRecord1)));
    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(prodRecord2, stagingRecord2)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results1 =
        recordSupplier.poll(1000);

    Assert.assertEquals("First poll should include 1 record", 1, results1.size());

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results2 =
        recordSupplier.poll(1000);

    Assert.assertEquals("Second poll should include 1 record", 1, results2.size());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testEmptyPoll()
  {
    // Test that empty polls don't affect statistics
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, false, headerFilter, "test-datasource");

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Collections.emptyList()));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assert.assertEquals("Empty poll should return empty list", 0, results.size());
    EasyMock.verify(mockConsumer);
  }

  @Test
  public void testMultiTopic()
  {
    // Test header filtering with multi-topic configuration
    InDimFilter filter = new InDimFilter("environment", Collections.singletonList("production"), null);
    KafkaHeaderBasedFilteringConfig headerFilter = new KafkaHeaderBasedFilteringConfig(filter, null, null);

    recordSupplier = new KafkaRecordSupplier(mockConsumer, true, headerFilter); // multiTopic = true

    ConsumerRecord<byte[], byte[]> topic1Record = createRecord("topic1", 0, 100L,
        headers("environment", "production"));
    ConsumerRecord<byte[], byte[]> topic2Record = createRecord("topic2", 0, 101L,
        headers("environment", "staging"));

    EasyMock.expect(mockConsumer.poll(EasyMock.anyObject(Duration.class)))
        .andReturn(createConsumerRecords(Arrays.asList(topic1Record, topic2Record)));
    EasyMock.replay(mockConsumer);

    List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> results =
        recordSupplier.poll(1000);

    Assert.assertEquals("Should only include production record", 1, results.size());
    EasyMock.verify(mockConsumer);
    Assert.assertEquals("topic1", results.get(0).getStream());
    Assert.assertTrue("Should be multi-topic partition",
        results.get(0).getPartitionId().isMultiTopicPartition());
  }

  // Helper methods

  private ConsumerRecord<byte[], byte[]> createRecord(String topic, int partition, long offset, RecordHeaders headers)
  {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        topic,
        partition,
        offset,
        "test-key".getBytes(StandardCharsets.UTF_8),
        "test-value".getBytes(StandardCharsets.UTF_8)
    );

    // Set headers using reflection since ConsumerRecord headers are final
    try {
      Field headersField = ConsumerRecord.class.getDeclaredField("headers");
      headersField.setAccessible(true);
      headersField.set(record, headers);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to set headers on test record", e);
    }

    return record;
  }

  private RecordHeaders headers(String... keyValuePairs)
  {
    if (keyValuePairs.length % 2 != 0) {
      throw new IllegalArgumentException("Key-value pairs must be even number of arguments");
    }

    RecordHeaders headers = new RecordHeaders();
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      String key = keyValuePairs[i];
      String value = keyValuePairs[i + 1];
      headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }
    return headers;
  }

  private ConsumerRecords<byte[], byte[]> createConsumerRecords(List<ConsumerRecord<byte[], byte[]>> records)
  {
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    for (ConsumerRecord<byte[], byte[]> record : records) {
      TopicPartition tp = new TopicPartition(record.topic(), record.partition());
      recordsMap.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
    }
    return new ConsumerRecords<>(recordsMap);
  }
}
