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

package org.apache.druid.data.input.opentelemetry.protobuf.metrics;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.opentelemetry.protobuf.AbstractProtobufReader;
import org.apache.druid.data.input.opentelemetry.protobuf.Utils;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OpenTelemetryMetricsProtobufReader extends AbstractProtobufReader
{
  private static final Logger log = new Logger(OpenTelemetryMetricsProtobufReader.class);
  private final String metricDimension;
  private final String valueDimension;
  private final String metricAttributePrefix;
  private final String resourceAttributePrefix;

  public OpenTelemetryMetricsProtobufReader(
      DimensionsSpec dimensionsSpec,
      SettableByteEntity<? extends ByteEntity> source,
      String metricDimension,
      String valueDimension,
      String metricAttributePrefix,
      String resourceAttributePrefix
  )
  {
    super(dimensionsSpec, source);
    this.metricDimension = metricDimension;
    this.valueDimension = valueDimension;
    this.metricAttributePrefix = metricAttributePrefix;
    this.resourceAttributePrefix = resourceAttributePrefix;
  }

  private List<InputRow> parseMetricsData(final MetricsData metricsData)
  {
    return metricsData.getResourceMetricsList()
        .stream()
        .flatMap(resourceMetrics -> {
          Map<String, Object> resourceAttributes = Utils.getResourceAttributes(resourceMetrics.getResource(),
                                                                         resourceAttributePrefix);
          return resourceMetrics.getScopeMetricsList()
              .stream()
              .flatMap(scopeMetrics -> scopeMetrics.getMetricsList()
                  .stream()
                  .flatMap(metric -> parseMetric(metric, resourceAttributes).stream()));
        })
        .collect(Collectors.toList());
  }

  private List<InputRow> parseMetric(Metric metric, Map<String, Object> resourceAttributes)
  {
    final List<InputRow> inputRows;
    String metricName = metric.getName();
    switch (metric.getDataCase()) {
      case SUM: {
        inputRows = new ArrayList<>(metric.getSum().getDataPointsCount());
        metric.getSum()
            .getDataPointsList()
            .forEach(dataPoint -> inputRows.add(parseNumberDataPoint(dataPoint, resourceAttributes, metricName)));
        break;
      }
      case GAUGE: {
        inputRows = new ArrayList<>(metric.getGauge().getDataPointsCount());
        metric.getGauge()
            .getDataPointsList()
            .forEach(dataPoint -> inputRows.add(parseNumberDataPoint(dataPoint, resourceAttributes, metricName)));
        break;
      }
      // TODO Support HISTOGRAM and SUMMARY metrics
      case HISTOGRAM:
      case SUMMARY:
      default:
        log.trace("Metric type %s is not supported", metric.getDataCase());
        inputRows = Collections.emptyList();

    }
    return inputRows;
  }

  private InputRow parseNumberDataPoint(NumberDataPoint dataPoint,
                                        Map<String, Object> resourceAttributes,
                                        String metricName)
  {

    int capacity = resourceAttributes.size()
          + dataPoint.getAttributesCount()
          + 2; // metric name + value columns
    Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
    event.put(metricDimension, metricName);

    if (dataPoint.hasAsInt()) {
      event.put(valueDimension, dataPoint.getAsInt());
    } else {
      event.put(valueDimension, dataPoint.getAsDouble());
    }

    event.putAll(resourceAttributes);
    dataPoint.getAttributesList().forEach(att -> {
      Object value = Utils.parseAnyValue(att.getValue());
      if (value != null) {
        event.put(metricAttributePrefix + att.getKey(), value);
      }
    });

    return createRow(TimeUnit.NANOSECONDS.toMillis(dataPoint.getTimeUnixNano()), event);
  }

  @Override
  public List<InputRow> parseData(ByteBuffer byteBuffer)
      throws InvalidProtocolBufferException
  {
    return parseMetricsData(MetricsData.parseFrom(byteBuffer));
  }
}
