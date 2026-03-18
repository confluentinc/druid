/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.confluent.druid.transform.EnrichResourceNameTransform;
import io.confluent.druid.transform.ExtractTenantTopicTransform;
import io.confluent.druid.transform.ExtractTenantTransform;
import io.confluent.druid.transform.NonBlockingLookupTransform;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class ConfluentExtensionsModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("ConfluentTransformsModule")
            .registerSubtypes(
                new NamedType(ExtractTenantTransform.class, "extractTenant"),
                new NamedType(ExtractTenantTopicTransform.class, "extractTenantTopic"),
                new NamedType(EnrichResourceNameTransform.class, "enrichResourceName"),
                new NamedType(NonBlockingLookupTransform.class, "nonBlockingLookup")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
