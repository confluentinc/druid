/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.druid.ConfluentExtensionsModule;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ExtractTransformTest
{

  // druid-37 removed InputRowParser/TimeAndDimsParseSpec; build rows directly and apply the transformer.
  private static final List<String> DIMENSIONS = ImmutableList.of("topic", "tenant");

  private static InputRow transform(TransformSpec transformSpec, Map<String, Object> raw)
  {
    return transformSpec.toTransformer().transform(
        new MapBasedInputRow(DateTimes.of("2020-01-01"), DIMENSIONS, raw)
    );
  }

  private static final Map<String, Object> ROW1 = ImmutableMap.<String, Object>builder()
      .put("topic", "lkc-abc123_mytopic")
      .build();

  private static final Map<String, Object> ROW2 = ImmutableMap.<String, Object>builder()
      .put("tenant", "lkc-xyz789")
      .put("tenant_topic", "topic0")
      .put("topic", "lkc-abc123_mytopic")
      .build();

  private static final Map<String, Object> ROW3 = ImmutableMap.<String, Object>builder()
      .put("topic", "invalid-topic")
      .build();

  private static final Map<String, Object> ROW4 = ImmutableMap.<String, Object>builder()
      .build();


  @Test
  public void testExtraction()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRow row = transform(transformSpec, ROW1);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertEquals(ImmutableList.of("lkc-abc123"), row.getDimension("tenant"));
    Assert.assertEquals(ImmutableList.of("mytopic"), row.getDimension("tenant_topic"));
  }

  @Test
  public void testInternal()
  {
    Assert.assertEquals(null, TenantUtils.extractTenantTopic("__consumer_offsets"));
    Assert.assertEquals(null, TenantUtils.extractTenant("__consumer_offsets"));
    Assert.assertEquals(null, TenantUtils.extractTenantTopic("other.topic"));
    Assert.assertEquals(null, TenantUtils.extractTenant("other.topic"));
  }

  @Test
  public void testPreserveExistingFields()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRow row = transform(transformSpec, ROW2);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertEquals(ImmutableList.of("lkc-xyz789"), row.getDimension("tenant"));
    Assert.assertEquals(ImmutableList.of("topic0"), row.getDimension("tenant_topic"));
  }

  @Test
  public void testInvalidTopics()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRow row = transform(transformSpec, ROW3);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertNull(row.getRaw("tenant"));
    Assert.assertNull(row.getRaw("tenant_topic"));
  }

  @Test
  public void testNullTopic()
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTransform("tenant", "topic"),
            new ExtractTenantTopicTransform("tenant_topic", "topic")
        )
    );

    final InputRow row = transform(transformSpec, ROW4);

    Assert.assertNotNull(row);
    Assert.assertEquals(ImmutableList.of("topic", "tenant"), row.getDimensions());
    Assert.assertNull(row.getRaw("tenant"));
    Assert.assertNull(row.getRaw("tenant_topic"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExtractTenantTopicTransform("tenant_topic", "topic"),
            new ExtractTenantTransform("tenant", "topic")
        )
    );

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerModules(new ConfluentExtensionsModule().getJacksonModules());

    Assert.assertEquals(
        transformSpec,
        jsonMapper.readValue(jsonMapper.writeValueAsString(transformSpec), TransformSpec.class)
    );
  }
}
