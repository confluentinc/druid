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

package org.apache.druid.query;

import com.google.common.base.Supplier;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

public class MetricsEmittingMergingBlockingPool<T> extends DefaultBlockingPool<T>
    implements ExecutorServiceMonitor.MetricEmitter
{

  public MetricsEmittingMergingBlockingPool(
      Supplier<T> generator,
      int limit,
      ExecutorServiceMonitor executorServiceMonitor
  )
  {
    super(generator, limit);
    executorServiceMonitor.add(this);
  }

  @Override
  public void emitMetrics(ServiceEmitter emitter, ServiceMetricEvent.Builder metricBuilder)
  {
    emitter.emit(metricBuilder.build("query/merge/buffersUsed", getUsedBufferCount()));
    emitter.emit(metricBuilder.build("query/merge/totalBuffers", maxSize()));
  }
}
