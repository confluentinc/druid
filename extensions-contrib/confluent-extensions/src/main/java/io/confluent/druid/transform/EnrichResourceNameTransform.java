/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Row;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class EnrichResourceNameTransform implements Transform
{
  private final String name;
  private final String metricNameDimension;
  private final Set<String> kafkaPrefixes;
  private final String kafkaResourceIdDimension;
  private final Set<String> tableflowPrefixes;
  private final String tableflowResourceIdDimension;
  private final Set<String> connectPrefixes;
  private final String connectResourceIdDimension;
  private final Set<String> ksqlPrefixes;
  private final String ksqlResourceIdDimension;
  private final Set<String> schemaRegistryPrefixes;
  private final String schemaRegistryResourceIdDimension;
  private final Set<String> fcpPrefixes;
  private final String fcpResourceIdDimension;
  private final String lookupName;
  private final LookupExtractorFactoryContainerProvider lookupProvider;

  public EnrichResourceNameTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("metric_name_dimension") final String metric_name_dimension,
      @JsonProperty("kafka_metric_prefixes") final Set<String> kafka_metric_prefixes,
      @JsonProperty("kafka_resource_id_dimension") final String kafka_resource_id_dimension,
      @JsonProperty("tableflow_metric_prefixes") final Set<String> tableflow_metric_prefixes,
      @JsonProperty("tableflow_resource_id_dimension") final String tableflow_resource_id_dimension,
      @JsonProperty("connect_metric_prefixes") final Set<String> connect_metric_prefixes,
      @JsonProperty("connect_resource_id_dimension") final String connect_resource_id_dimension,
      @JsonProperty("ksql_metric_prefixes") final Set<String> ksql_metric_prefixes,
      @JsonProperty("ksql_resource_id_dimension") final String ksql_resource_id_dimension,
      @JsonProperty("schema_registry_metric_prefixes") final Set<String> schema_registry_metric_prefixes,
      @JsonProperty("schema_registry_resource_id_dimension") final String schema_registry_resource_id_dimension,
      @JsonProperty("fcp_metric_prefixes") final Set<String> fcp_metric_prefixes,
      @JsonProperty("fcp_resource_id_dimension") final String fcp_resource_id_dimension,
      @JsonProperty("lookup_name") final String lookup_name,
      @JacksonInject LookupExtractorFactoryContainerProvider lookupProvider
  )
  {
    this.name = Preconditions.checkNotNull(name, "Specify output-column name");
    this.metricNameDimension = Preconditions.checkNotNull(metric_name_dimension, "Specify metric-name column");
    this.kafkaPrefixes = kafka_metric_prefixes != null ? kafka_metric_prefixes : new HashSet<>();
    this.kafkaResourceIdDimension = Preconditions.checkNotNull(kafka_resource_id_dimension, "Specify kafka-id column");
    this.tableflowPrefixes = tableflow_metric_prefixes != null ? tableflow_metric_prefixes : new HashSet<>();
    this.tableflowResourceIdDimension = Preconditions.checkNotNull(tableflow_resource_id_dimension, "Specify kafka-id column for tableflow metrics");
    this.connectPrefixes = connect_metric_prefixes != null ? connect_metric_prefixes : new HashSet<>();
    this.connectResourceIdDimension = Preconditions.checkNotNull(connect_resource_id_dimension, "Specify connector-id column");
    this.ksqlPrefixes = ksql_metric_prefixes != null ? ksql_metric_prefixes : new HashSet<>();
    this.ksqlResourceIdDimension = Preconditions.checkNotNull(ksql_resource_id_dimension, "Specify ksql-id column");
    this.schemaRegistryPrefixes = schema_registry_metric_prefixes != null ? schema_registry_metric_prefixes : new HashSet<>();
    this.schemaRegistryResourceIdDimension = Preconditions.checkNotNull(schema_registry_resource_id_dimension, "Specify sr-id column");
    this.fcpPrefixes = fcp_metric_prefixes != null ? fcp_metric_prefixes : new HashSet<>();
    this.fcpResourceIdDimension = Preconditions.checkNotNull(fcp_resource_id_dimension, "Specify fcp-id column");
    this.lookupName = Preconditions.checkNotNull(lookup_name, "name");
    this.lookupProvider = Preconditions.checkNotNull(lookupProvider, "lookupProvider");
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getMetricNameDimension()
  {
    return metricNameDimension;
  }

  @Override
  public RowFunction getRowFunction()
  {
    return row -> {
      Optional<LookupExtractorFactoryContainer> container = lookupProvider.get(lookupName);
      if (!container.isPresent()) {
        return null;
      }
      LookupExtractor lookup = container.get().getLookupExtractorFactory().get();
      String metricName = row.getRaw(metricNameDimension).toString();

      if (metricName != null) {
        // Check if metric name starts with any kafka prefix
        for (String prefix : kafkaPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, kafkaResourceIdDimension, true);
          }
        }
        // Check if metric name starts with any connect prefix
        for (String prefix : connectPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, connectResourceIdDimension, false);
          }
        }
        // Check if metric name starts with any ksql prefix
        for (String prefix : ksqlPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, ksqlResourceIdDimension, false);
          }
        }
        // Check if metric name starts with any schema registry prefix
        for (String prefix : schemaRegistryPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, schemaRegistryResourceIdDimension, false);
          }
        }
        // Check if metric name starts with any flink-compute-pool prefix
        for (String prefix : fcpPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, fcpResourceIdDimension, false);
          }
        }
        // Check if metric name starts with any tableflow prefix
        for (String prefix : tableflowPrefixes) {
          if (metricName.startsWith(prefix)) {
            return enrichNameFromLookup(row, lookup, tableflowResourceIdDimension, false);
          }
        }
      }

      return null;
    };
  }

  private String enrichNameFromLookup(Row row, LookupExtractor lookup, String resourceIdDimension, boolean isKafka)
  {
    Object resourceIdObject = row.getRaw(resourceIdDimension);
    if (resourceIdObject == null) {
      return null;
    }
    String resourceId = resourceIdObject.toString();
    if (isKafka) {
      resourceId = TenantUtils.extractTenant(resourceId);
    }
    return lookup.apply(resourceId);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    Set<String> columns = new HashSet<>();
    columns.add(this.name);
    columns.add(this.metricNameDimension);
    columns.add(this.kafkaResourceIdDimension);
    columns.add(this.tableflowResourceIdDimension);
    columns.add(this.connectResourceIdDimension);
    columns.add(this.ksqlResourceIdDimension);
    columns.add(this.schemaRegistryResourceIdDimension);
    columns.add(this.fcpResourceIdDimension);
    return columns;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EnrichResourceNameTransform)) {
      return false;
    }
    EnrichResourceNameTransform that = (EnrichResourceNameTransform) o;
    return name.equals(that.name) &&
           Objects.equals(kafkaPrefixes, that.kafkaPrefixes) &&
           Objects.equals(tableflowPrefixes, that.tableflowPrefixes) &&
           Objects.equals(connectPrefixes, that.connectPrefixes) &&
           Objects.equals(ksqlPrefixes, that.ksqlPrefixes) &&
           Objects.equals(schemaRegistryPrefixes, that.schemaRegistryPrefixes) &&
           Objects.equals(fcpPrefixes, that.fcpPrefixes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, kafkaPrefixes, tableflowPrefixes, connectPrefixes,
            ksqlPrefixes, schemaRegistryPrefixes, fcpPrefixes);
  }

  @Override
  public String toString()
  {
    return "EnrichResourceNameTransform{" +
           "name='" + name + '\'' +
           ", kafka_metric_prefixes=" + kafkaPrefixes +
           ", tableflow_metric_prefixes=" + tableflowPrefixes +
           ", connect_metric_prefixes=" + connectPrefixes +
           ", ksql_metric_prefixes=" + ksqlPrefixes +
           ", schema_registry_metric_prefixes=" + schemaRegistryPrefixes +
           ", fcp_metric_prefixes=" + fcpPrefixes +
           '}';
  }
}
