/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class EnrichResourceNameTransformTest
{
  // Simple mock lookup provider for testing
  private static final LookupExtractorFactoryContainerProvider MOCK_LOOKUP_PROVIDER =
      new LookupExtractorFactoryContainerProvider()
      {
        @Override
        public Optional<LookupExtractorFactoryContainer> get(String lookupName)
        {
          return Optional.empty();
        }

        @Override
        public Set<String> getAllLookupNames()
        {
          return ImmutableSet.of();
        }
      };

  private static final MapInputRowParser PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
        new TimestampSpec("t", "auto", DateTimes.of("2020-01-01")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("metric_name", "connector_id", "tenant")))
      )
  );

  @Test
  public void testAllResourceMetricPrefixMatching()
  {
    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-", "kafka_"),
        "kafka_resource",
        ImmutableSet.of("tableflow-"),
        "tableflow_resource",
        ImmutableSet.of("connect-", "kafka-connect-"),
        "connect_resource",
        ImmutableSet.of("ksql-"),
        "ksql_resource",
        ImmutableSet.of("schema_registry-"),
        "schema_registry_resource",
        ImmutableSet.of("fcp-"),
        "fcp_resource",
        "ResourceNameLookup",
        MOCK_LOOKUP_PROVIDER
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test Connect prefix matching with connector.id lookup
    Map<String, Object> connectRowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "connect-kafka-source-metrics")
        .put("connector_id", "lcc-abc123")
        .build();

    InputRow connectRow = parser.parseBatch(connectRowData).get(0);
    Assert.assertNotNull(connectRow);
    // Since lookup provider is null, should return null
    Assert.assertNull(connectRow.getRaw("resource_name"));

    // Test Kafka prefix matching with tenant lookup
    Map<String, Object> kafkaRowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "kafka-producer-metrics")
        .put("tenant", "lkc-xyz789")
        .build();

    InputRow kafkaRow = parser.parseBatch(kafkaRowData).get(0);
    Assert.assertNotNull(kafkaRow);
    // Since lookup provider is null, should return null
    Assert.assertNull(kafkaRow.getRaw("resource_name"));
  }

  @Test
  public void testNoPrefixMatch()
  {
    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-"),
        "kafka_resource",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "ResourceNameLookup",
        MOCK_LOOKUP_PROVIDER
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test no prefix matching
    Map<String, Object> rowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "unknown-metrics")
        .build();

    InputRow row = parser.parseBatch(rowData).get(0);
    Assert.assertNotNull(row);
    Assert.assertNull(row.getRaw("resource_name"));
  }

  @Test
  public void testNullMetricName()
  {
    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-"),
        "kafka_resource",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "ResourceNameLookup",
        MOCK_LOOKUP_PROVIDER
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test null metric name
    Map<String, Object> rowData = ImmutableMap.<String, Object>builder()
        .put("other_field", "value")
        .build();

    InputRow row = parser.parseBatch(rowData).get(0);
    Assert.assertNotNull(row);
    Assert.assertNull(row.getRaw("resource_name"));
  }

  @Test
  public void testGetRequiredColumns()
  {
    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-"),
        "kafka_resource",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "ResourceNameLookup",
        MOCK_LOOKUP_PROVIDER
    );

    Set<String> requiredColumns = transform.getRequiredColumns();
    Assert.assertTrue(requiredColumns.contains("resource_name"));
    Assert.assertTrue(requiredColumns.contains("metric_name"));
    Assert.assertTrue(requiredColumns.contains("kafka_resource"));
    Assert.assertTrue(requiredColumns.contains("tableflow_resource"));
    Assert.assertTrue(requiredColumns.contains("connect_resource"));
    Assert.assertTrue(requiredColumns.contains("ksql_resource"));
    Assert.assertTrue(requiredColumns.contains("schema_registry_resource"));
    Assert.assertTrue(requiredColumns.contains("fcp_resource"));
  }
}
