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

package org.apache.druid.data.input.opentelemetry.protobuf;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class Utils
{
  public static SettableByteEntity<? extends ByteEntity> getSettableEntity(InputEntity source)
  {
    // Sampler passes a KafkaRecordEntity directly, while the normal code path wraps the same entity in a
    // SettableByteEntity
    if (source instanceof SettableByteEntity) {
      return (SettableByteEntity<? extends ByteEntity>) source;
    } else {
      SettableByteEntity<ByteEntity> wrapper = new SettableByteEntity<>();
      wrapper.setEntity((ByteEntity) source);
      return wrapper;
    }
  }

  @Nullable
  public static Object parseAnyValue(AnyValue value)
  {
    switch (value.getValueCase()) {
      case INT_VALUE:
        return value.getIntValue();
      case BOOL_VALUE:
        return value.getBoolValue();
      case DOUBLE_VALUE:
        return value.getDoubleValue();
      case STRING_VALUE:
        return value.getStringValue();

      // TODO: Support KVLIST_VALUE, ARRAY_VALUE and BYTES_VALUE

      default:
        // VALUE_NOT_SET
        return null;
    }
  }

  public static Map<String, Object> getResourceAttributes(Resource resource, String resourceAttributePrefix)
  {
    return resource
        .getAttributesList()
        .stream()
        .collect(
            HashMap::new,
            (m, kv) -> {
              Object value = Utils.parseAnyValue(kv.getValue());
              if (value != null) {
                m.put(resourceAttributePrefix + kv.getKey(), value);
              }
            },
            HashMap::putAll
        );
  }
}
