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
      @JsonProperty("metricNameDimension") final String metricNameDimension,
      @JsonProperty("kafkaNamePrefixes") final Set<String> kafkaMetricPrefixes,
      @JsonProperty("kafkaResourceIdDimension") final String kafkaResourceIdDimension,
      @JsonProperty("tableflowMetricPrefixes") final Set<String> tableflowMetricPrefixes,
      @JsonProperty("tableflowResourceIdDimension") final String tableflowResourceIdDimension,
      @JsonProperty("connectMetricPrefixes") final Set<String> connectMetricPrefixes,
      @JsonProperty("connectResourceIdDimension") final String connectResourceIdDimension,
      @JsonProperty("ksqlMetricPrefixes") final Set<String> ksqlMetricPrefixes,
      @JsonProperty("ksqlResourceIdDimension") final String ksqlResourceIdDimension,
      @JsonProperty("schemaRegistryMetricPrefixes") final Set<String> schemaRegistryMetricPrefixes,
      @JsonProperty("schemaRegistryResourceIdDimension") final String schemaRegistryResourceIdDimension,
      @JsonProperty("fcpMetricPrefixes") final Set<String> fcpMetricPrefixes,
      @JsonProperty("fcpResourceIdDimension") final String fcpResourceIdDimension,
      @JsonProperty("lookupName") final String lookupName,
      @JacksonInject LookupExtractorFactoryContainerProvider lookupProvider
  )
  {
    this.name = Preconditions.checkNotNull(name, "Specify output-column name");
    this.metricNameDimension = Preconditions.checkNotNull(metricNameDimension, "Specify metric-name column");
    this.kafkaPrefixes = kafkaMetricPrefixes != null ? kafkaMetricPrefixes : new HashSet<>();
    this.kafkaResourceIdDimension = Preconditions.checkNotNull(kafkaResourceIdDimension, "Specify kafka-id column");
    this.tableflowPrefixes = tableflowMetricPrefixes != null ? tableflowMetricPrefixes : new HashSet<>();
    this.tableflowResourceIdDimension = Preconditions.checkNotNull(tableflowResourceIdDimension, "Specify kafka-id column for tableflow metrics");
    this.connectPrefixes = connectMetricPrefixes != null ? connectMetricPrefixes : new HashSet<>();
    this.connectResourceIdDimension = Preconditions.checkNotNull(connectResourceIdDimension, "Specify connector-id column");
    this.ksqlPrefixes = ksqlMetricPrefixes != null ? ksqlMetricPrefixes : new HashSet<>();
    this.ksqlResourceIdDimension = Preconditions.checkNotNull(ksqlResourceIdDimension, "Specify ksql-id column");
    this.schemaRegistryPrefixes = schemaRegistryMetricPrefixes != null ? schemaRegistryMetricPrefixes : new HashSet<>();
    this.schemaRegistryResourceIdDimension = Preconditions.checkNotNull(schemaRegistryResourceIdDimension, "Specify sr-id column");
    this.fcpPrefixes = fcpMetricPrefixes != null ? fcpMetricPrefixes : new HashSet<>();
    this.fcpResourceIdDimension = Preconditions.checkNotNull(fcpResourceIdDimension, "Specify fcp-id column");
    this.lookupName = Preconditions.checkNotNull(lookupName, "name");
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

  @JsonProperty
  public String getKafkaResourceIdDimension()
  {
    return kafkaResourceIdDimension;
  }

  @JsonProperty
  public String getTableflowResourceIdDimension()
  {
    return tableflowResourceIdDimension;
  }

  @JsonProperty
  public String getConnectNameDimension()
  {
    return connectResourceIdDimension;
  }

  @JsonProperty
  public String getKsqlResourceIdDimension()
  {
    return ksqlResourceIdDimension;
  }

  @JsonProperty
  public String getSchemaRegistryResourceIdDimension()
  {
    return schemaRegistryResourceIdDimension;
  }

  @JsonProperty
  public String getFcpResourceIdDimension()
  {
    return fcpResourceIdDimension;
  }

  @JsonProperty
  public String getLookupName()
  {
    return lookupName;
  }

  @JsonProperty
  public Set<String> getKafkaPrefixes()
  {
    return kafkaPrefixes;
  }

  @JsonProperty
  public Set<String> getTableflowPrefixes()
  {
    return tableflowPrefixes;
  }

  @JsonProperty
  public Set<String> getConnectPrefixes()
  {
    return connectPrefixes;
  }

  @JsonProperty
  public Set<String> getKsqlPrefixes()
  {
    return ksqlPrefixes;
  }

  @JsonProperty
  public Set<String> getSchemaRegistryPrefixes()
  {
    return schemaRegistryPrefixes;
  }

  @JsonProperty
  public Set<String> getFcpPrefixes()
  {
    return fcpPrefixes;
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
      ", kafkaMetricPrefixes=" + kafkaPrefixes +
      ", tableflowMetricPrefixes=" + tableflowPrefixes +
      ", connectMetricPrefixes=" + connectPrefixes +
      ", ksqlMetricPrefixes=" + ksqlPrefixes +
      ", schemaRegistryMetricPrefixes=" + schemaRegistryPrefixes +
      ", fcpMetricPrefixes=" + fcpPrefixes +
      '}';
  }
}
