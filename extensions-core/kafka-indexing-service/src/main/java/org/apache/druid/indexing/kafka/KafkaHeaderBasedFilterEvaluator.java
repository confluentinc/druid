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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilteringConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.MonitorUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Evaluates Kafka header filters for pre-ingestion filtering.
 */
public class KafkaHeaderBasedFilterEvaluator
{
  private static final Logger log = new Logger(KafkaHeaderBasedFilterEvaluator.class);

  private final Filter filter;
  private final Charset encoding;
  private final KafkaHeaderFilterMetrics metrics;

  private final Cache<ByteBuffer, String> stringDecodingCache;


  public KafkaHeaderBasedFilterEvaluator(KafkaHeaderBasedFilteringConfig headerBasedFilteringConfig)
  {
    this.encoding = Charset.forName(headerBasedFilteringConfig.getEncoding());
    this.metrics = new KafkaHeaderFilterMetrics();
    this.stringDecodingCache = Caffeine.newBuilder()
        .maximumSize(headerBasedFilteringConfig.getStringDecodingCacheSize())
        .build();

    this.filter = headerBasedFilteringConfig.getFilter().toFilter();

    log.info("Initialized Kafka header filter with encoding [%s] - direct evaluation for [%s] with Caffeine string cache (max %d entries)",
             headerBasedFilteringConfig.getEncoding(),
             this.filter.getClass().getSimpleName(),
             headerBasedFilteringConfig.getStringDecodingCacheSize());
  }


  /**
   * Evaluates whether a Kafka record should be included based on its headers.
   *
   * @param record the Kafka consumer record
   * @return true if the record should be included, false if it should be filtered out
   */
  public boolean shouldIncludeRecord(ConsumerRecord<byte[], byte[]> record)
  {
    long startTime = System.nanoTime();
    try {
      boolean shouldInclude = evaluateInclusion(record.headers());

      long evaluationTime = System.nanoTime() - startTime;
      metrics.recordEvaluation(evaluationTime);

      if (!shouldInclude) {
        metrics.recordFiltered();
      }

      return shouldInclude;
    }
    catch (Exception e) {
      long evaluationTime = System.nanoTime() - startTime;
      metrics.recordError(evaluationTime);
      log.warn(
          e,
          "Error evaluating header filter for record at topic [%s] partition [%d] offset [%d], including record",
          record.topic(),
          record.partition(),
          record.offset()
      );
      return true; // Default to including record on error
    }
  }

  private boolean evaluateInclusion(Headers headers)
  {
    if (!(filter instanceof InDimFilter)) {
      // Only InDimFilter supported
      throw new IllegalStateException("Unsupported filter type: " + filter.getClass().getSimpleName());
    }

    InDimFilter inFilter = (InDimFilter) filter;

    // Permissive behavior: missing headers result in inclusion
    if (headers == null) {
      return true;
    }

    Header header = headers.lastHeader(inFilter.getDimension());
    // Permissive behavior: header is null or empty
    if (header == null || header.value() == null) {
      return true;
    }

    String headerValue = getDecodedHeaderValue(header.value());
    return inFilter.getValues().contains(headerValue);
  }


  /**
   * Decode header bytes to string with caching.
   * Returns null if decoding fails.
   */
  @Nullable
  private String getDecodedHeaderValue(byte[] headerBytes)
  {
    try {
      ByteBuffer key = ByteBuffer.wrap(headerBytes);
      return stringDecodingCache.get(key, k -> new String(headerBytes, encoding));
    }
    catch (Exception e) {
      log.warn(e, "Failed to decode header bytes, treating as null");
      return null;
    }
  }

  /**
   * Gets the metrics for this evaluator.
   */
  public KafkaHeaderFilterMetrics getMetrics()
  {
    return metrics;
  }


  /**
   * Comprehensive metrics collector for header filter evaluation.
   * Tracks performance, errors, and filtering statistics.
   */
  public static class KafkaHeaderFilterMetrics extends AbstractMonitor
  {
    // Core filtering metrics
    private final AtomicLong totalEvaluations = new AtomicLong(0);
    private final AtomicLong filteredRecords = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    // Performance metrics
    private final AtomicLong totalEvaluationTimeNanos = new AtomicLong(0);
    private final AtomicLong maxEvaluationTimeNanos = new AtomicLong(0);

    void recordEvaluation(long evaluationTimeNanos)
    {
      totalEvaluations.incrementAndGet();
      totalEvaluationTimeNanos.addAndGet(evaluationTimeNanos);

      // Update max evaluation time using compare-and-swap
      long currentMax = maxEvaluationTimeNanos.get();
      while (evaluationTimeNanos > currentMax &&
             !maxEvaluationTimeNanos.compareAndSet(currentMax, evaluationTimeNanos)) {
        currentMax = maxEvaluationTimeNanos.get();
      }
    }

    void recordFiltered()
    {
      filteredRecords.incrementAndGet();
    }

    void recordError(long evaluationTimeNanos)
    {
      totalEvaluations.incrementAndGet();
      errorCount.incrementAndGet();
      totalEvaluationTimeNanos.addAndGet(evaluationTimeNanos);
    }

    // Getters for metrics
    public long getTotalEvaluations()
    {
      return totalEvaluations.get();
    }

    public long getFilteredRecords()
    {
      return filteredRecords.get();
    }

    public long getErrorCount()
    {
      return errorCount.get();
    }

    public double getAverageEvaluationTimeNanos()
    {
      long total = totalEvaluations.get();
      return total > 0 ? (double) totalEvaluationTimeNanos.get() / total : 0.0;
    }

    public long getMaxEvaluationTimeNanos()
    {
      return maxEvaluationTimeNanos.get();
    }

    public boolean monitor(ServiceEmitter emitter, String dataSource)
    {
      return doMonitor(emitter, dataSource, null);
    }

    private boolean doMonitor(ServiceEmitter emitter, String dataSource, @Nullable Map<String, String[]> dimensions)
    {
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension(DruidMetrics.DATASOURCE, dataSource);

      if (dimensions != null) {
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      }

      // Core filtering metrics
      emitter.emit(builder.setMetric("ingest/events/headerFiltered", getFilteredRecords()));
      emitter.emit(builder.setMetric("ingest/events/headerFilterEvaluations", getTotalEvaluations()));
      emitter.emit(builder.setMetric("ingest/events/headerFilterErrors", getErrorCount()));

      // Performance metrics
      emitter.emit(builder.setMetric("ingest/events/headerFilterAvgTimeNanos", getAverageEvaluationTimeNanos()));
      emitter.emit(builder.setMetric("ingest/events/headerFilterMaxTimeNanos", getMaxEvaluationTimeNanos()));

      return true;
    }

    @Override
    public boolean doMonitor(ServiceEmitter emitter)
    {
      return monitor(emitter, "unknown");
    }

    /**
     * Reset all metrics (useful for testing)
     */
    public void reset()
    {
      totalEvaluations.set(0);
      filteredRecords.set(0);
      errorCount.set(0);
      totalEvaluationTimeNanos.set(0);
      maxEvaluationTimeNanos.set(0);
    }

    @Override
    public void start()
    {
      // No-op
    }

    @Override
    public void stop()
    {
      // No-op
    }
  }
}
