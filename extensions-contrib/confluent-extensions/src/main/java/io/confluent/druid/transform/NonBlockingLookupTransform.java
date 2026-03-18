/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A non-blocking lookup transform that returns null if the lookup is not ready or the key is not found.
 * This is useful for Kafka-based lookups that may take time to initialize.
 *
 * <p>Unlike the standard lookup() expression function which throws an exception if the lookup is
 * not available, this transform gracefully degrades by returning null. This allows ingestion tasks
 * to continue processing rows even when lookups are still loading from Kafka.
 *
 * <p>Usage example:
 * <pre>
 * {
 *   "type": "nonBlockingLookup",
 *   "name": "principal_display_name",
 *   "lookupName": "resource_name_lookup",
 *   "inputColumn": "user_resource_id"
 * }
 * </pre>
 *
 * <p>Behavior:
 * <ul>
 *   <li>If lookup is not available yet → returns null</li>
 *   <li>If lookup is not initialized yet → returns null</li>
 *   <li>If lookup key is not found → returns null</li>
 *   <li>If lookup succeeds → returns the looked-up value</li>
 * </ul>
 *
 * @see org.apache.druid.query.lookup.RegisteredLookupExtractionFn for the blocking version
 */
public class NonBlockingLookupTransform implements Transform
{
  private static final EmittingLogger log = new EmittingLogger(NonBlockingLookupTransform.class);

  private final String name;
  private final String lookupName;
  private final String inputColumn;
  private final LookupExtractorFactoryContainerProvider lookupProvider;

  private final AtomicBoolean notReadyLogged = new AtomicBoolean(false);

  @JsonCreator
  public NonBlockingLookupTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("lookupName") final String lookupName,
      @JsonProperty("inputColumn") final String inputColumn,
      @JacksonInject LookupExtractorFactoryContainerProvider lookupProvider
  )
  {
    this.name = Preconditions.checkNotNull(name, "name is required");
    this.lookupName = Preconditions.checkNotNull(lookupName, "lookupName is required");
    this.inputColumn = Preconditions.checkNotNull(inputColumn, "inputColumn is required");
    this.lookupProvider = Preconditions.checkNotNull(lookupProvider, "lookupProvider is required");
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getLookupName()
  {
    return lookupName;
  }

  @JsonProperty
  public String getInputColumn()
  {
    return inputColumn;
  }

  @Override
  public RowFunction getRowFunction()
  {
    return row -> {
      try {
        Optional<LookupExtractorFactoryContainer> container = lookupProvider.get(lookupName);
        if (!container.isPresent()) {
          if (notReadyLogged.compareAndSet(false, true)) {
            log.info("Lookup [%s] not available yet, will return null until ready", lookupName);
          }
          return null;
        }
        LookupExtractorFactory factory = container.get().getLookupExtractorFactory();
        if (!factory.isInitialized()) {
          if (notReadyLogged.compareAndSet(false, true)) {
            log.info("Lookup [%s] not initialized yet, will return null until ready", lookupName);
          }
          return null;
        }
        if (notReadyLogged.compareAndSet(true, false)) {
          log.info("Lookup [%s] is now ready", lookupName);
        }

        Object inputValue = row.getRaw(inputColumn);
        if (inputValue == null) {
          return null;
        }
        String key = inputValue.toString();
        LookupExtractor lookup = factory.get();
        return lookup.apply(key);
      }
      catch (Exception ex) {
        log.warn(ex, "Exception during lookup [%s], returning null", lookupName);
        return null;
      }
    };
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Collections.singleton(inputColumn);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NonBlockingLookupTransform)) {
      return false;
    }

    NonBlockingLookupTransform that = (NonBlockingLookupTransform) o;

    return name.equals(that.name) &&
           lookupName.equals(that.lookupName) &&
           inputColumn.equals(that.inputColumn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, lookupName, inputColumn);
  }

  @Override
  public String toString()
  {
    return "NonBlockingLookupTransform{" +
           "name='" + name + '\'' +
           ", lookupName='" + lookupName + '\'' +
           ", inputColumn='" + inputColumn + '\'' +
           '}';
  }
}
