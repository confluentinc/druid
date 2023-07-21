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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public abstract class AbstractProtobufReader implements InputEntityReader
{

  protected final SettableByteEntity<? extends ByteEntity> source;
  protected final DimensionsSpec dimensionsSpec;

  public AbstractProtobufReader(DimensionsSpec dimensionsSpec,
                                SettableByteEntity<? extends ByteEntity> source)
  {
    this.dimensionsSpec = dimensionsSpec;
    this.source = source;
  }

  public abstract List<InputRow> parseData(ByteBuffer byteBuffer) throws InvalidProtocolBufferException;

  @Override
  public CloseableIterator<InputRow> read()
  {
    Supplier<Iterator<InputRow>> supplier = Suppliers.memoize(() -> readAsList().iterator());
    return CloseableIterators.withEmptyBaggage(new Iterator<InputRow>() {
      @Override
      public boolean hasNext()
      {
        return supplier.get().hasNext();
      }
      @Override
      public InputRow next()
      {
        return supplier.get().next();
      }
    });
  }

  public List<InputRow> readAsList()
  {
    try {
      ByteBuffer buffer = source.getEntity().getBuffer();
      List<InputRow> rows = parseData(buffer);
      // Explicitly move the position assuming that all the remaining bytes have been consumed because the protobuf
      // parser does not update the position itself
      buffer.position(buffer.limit());
      return rows;
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(null, e, "Protobuf message could not be parsed");
    }
  }

  protected InputRow createRow(long timeUnixMilli, Map<String, Object> event)
  {
    final List<String> dimensions;
    if (!dimensionsSpec.getDimensionNames().isEmpty()) {
      dimensions = dimensionsSpec.getDimensionNames();
    } else {
      dimensions = new ArrayList<>(Sets.difference(event.keySet(), dimensionsSpec.getDimensionExclusions()));
    }
    return new MapBasedInputRow(timeUnixMilli, dimensions, event);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}
