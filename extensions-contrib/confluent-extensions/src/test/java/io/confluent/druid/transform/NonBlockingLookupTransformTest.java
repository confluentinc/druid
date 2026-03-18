/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.transform.RowFunction;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class NonBlockingLookupTransformTest
{
  @Test
  public void testLookupNotAvailable()
  {
    LookupExtractorFactoryContainerProvider provider = Mockito.mock(LookupExtractorFactoryContainerProvider.class);
    Mockito.when(provider.get("test_lookup")).thenReturn(Optional.empty());

    NonBlockingLookupTransform transform = new NonBlockingLookupTransform(
        "output",
        "test_lookup",
        "input",
        provider
    );

    RowFunction fn = transform.getRowFunction();
    MapBasedInputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        Collections.emptyList(),
        ImmutableMap.of("input", "key1")
    );

    String result = (String) fn.eval(row);
    Assert.assertNull(result);
  }

  @Test
  public void testLookupNotInitialized()
  {
    LookupExtractorFactory factory = Mockito.mock(LookupExtractorFactory.class);
    Mockito.when(factory.isInitialized()).thenReturn(false);

    LookupExtractorFactoryContainer container = Mockito.mock(LookupExtractorFactoryContainer.class);
    Mockito.when(container.getLookupExtractorFactory()).thenReturn(factory);

    LookupExtractorFactoryContainerProvider provider = Mockito.mock(LookupExtractorFactoryContainerProvider.class);
    Mockito.when(provider.get("test_lookup")).thenReturn(Optional.of(container));

    NonBlockingLookupTransform transform = new NonBlockingLookupTransform(
        "output",
        "test_lookup",
        "input",
        provider
    );

    RowFunction fn = transform.getRowFunction();
    MapBasedInputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        Collections.emptyList(),
        ImmutableMap.of("input", "key1")
    );

    String result = (String) fn.eval(row);
    Assert.assertNull(result);
  }

  @Test
  public void testLookupSuccess()
  {
    LookupExtractor extractor = Mockito.mock(LookupExtractor.class);
    Mockito.when(extractor.apply("key1")).thenReturn("value1");

    LookupExtractorFactory factory = Mockito.mock(LookupExtractorFactory.class);
    Mockito.when(factory.isInitialized()).thenReturn(true);
    Mockito.when(factory.get()).thenReturn(extractor);

    LookupExtractorFactoryContainer container = Mockito.mock(LookupExtractorFactoryContainer.class);
    Mockito.when(container.getLookupExtractorFactory()).thenReturn(factory);

    LookupExtractorFactoryContainerProvider provider = Mockito.mock(LookupExtractorFactoryContainerProvider.class);
    Mockito.when(provider.get("test_lookup")).thenReturn(Optional.of(container));

    NonBlockingLookupTransform transform = new NonBlockingLookupTransform(
        "output",
        "test_lookup",
        "input",
        provider
    );

    RowFunction fn = transform.getRowFunction();
    MapBasedInputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        Collections.emptyList(),
        ImmutableMap.of("input", "key1")
    );

    String result = (String) fn.eval(row);
    Assert.assertEquals("value1", result);
  }

  @Test
  public void testLookupKeyNotFound()
  {
    LookupExtractor extractor = Mockito.mock(LookupExtractor.class);
    Mockito.when(extractor.apply("key1")).thenReturn(null);

    LookupExtractorFactory factory = Mockito.mock(LookupExtractorFactory.class);
    Mockito.when(factory.isInitialized()).thenReturn(true);
    Mockito.when(factory.get()).thenReturn(extractor);

    LookupExtractorFactoryContainer container = Mockito.mock(LookupExtractorFactoryContainer.class);
    Mockito.when(container.getLookupExtractorFactory()).thenReturn(factory);

    LookupExtractorFactoryContainerProvider provider = Mockito.mock(LookupExtractorFactoryContainerProvider.class);
    Mockito.when(provider.get("test_lookup")).thenReturn(Optional.of(container));

    NonBlockingLookupTransform transform = new NonBlockingLookupTransform(
        "output",
        "test_lookup",
        "input",
        provider
    );

    RowFunction fn = transform.getRowFunction();
    MapBasedInputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        Collections.emptyList(),
        ImmutableMap.of("input", "key1")
    );

    String result = (String) fn.eval(row);
    Assert.assertNull(result);
  }

  @Test
  public void testInputColumnNull()
  {
    LookupExtractorFactoryContainerProvider provider = Mockito.mock(LookupExtractorFactoryContainerProvider.class);

    NonBlockingLookupTransform transform = new NonBlockingLookupTransform(
        "output",
        "test_lookup",
        "input",
        provider
    );

    RowFunction fn = transform.getRowFunction();
    MapBasedInputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        Collections.emptyList(),
        ImmutableMap.of("other_column", "value")
    );

    String result = (String) fn.eval(row);
    Assert.assertNull(result);
  }
}
