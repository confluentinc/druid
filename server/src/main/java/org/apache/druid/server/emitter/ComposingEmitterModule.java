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

package org.apache.druid.server.emitter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.ComposingEmitter;
import org.apache.druid.java.util.emitter.core.Emitter;

import java.util.List;

/**
 */
public class ComposingEmitterModule implements DruidModule
{
  private static Logger log = new Logger(ComposingEmitterModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.composing", ComposingEmitterConfig.class);
  }

  @Provides
  @ManageLifecycle
  @Named("composing")
  public Emitter getEmitter(ComposingEmitterConfig config, final Injector injector)
  {
    log.info("Creating Composing Emitter with %s", config.getEmitters());

    List<Emitter> emitters = Lists.transform(
        config.getEmitters(),
        new Function<String, Emitter>()
        {
          @Override
          public Emitter apply(String s)
          {
// Detected the use of a crypographic function. Please review this for compliance. https://go/fips-compliance
            return injector.getInstance(Key.get(Emitter.class, Names.named(s)));
          }
        }
    );

    return new ComposingEmitter(emitters);
  }
}

