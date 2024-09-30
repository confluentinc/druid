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

package org.apache.druid.java.util.emitter.core;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;

/**
 */
public class NoopEmitter implements Emitter
{

  @Override
  public void start()
  {
    // Do nothing
  }

  @Override
  public void emit(Event event)
  {
    // Do nothing
  }

  @Override
  public void flush()
  {
    // Do nothing
  }

  @Override
  public void close()
  {
    // Do nothing
  }

  @Override
  public String toString()
  {
    return "NoopEmitter{}";
  }
}
