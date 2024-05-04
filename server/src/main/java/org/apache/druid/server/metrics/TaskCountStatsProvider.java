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

package org.apache.druid.server.metrics;

import java.util.Map;
import org.apache.druid.java.util.common.Pair;

public interface TaskCountStatsProvider
{
  /**
   * Return the number of successful tasks for each datasource during emission period.
   */
  Map<Pair<String,String>, Long> getSuccessfulTaskCount();

  /**
   * Return the number of failed tasks for each datasource during emission period.
   */
  Map<Pair<String,String>, Long> getFailedTaskCount();

  /**
   * Return the number of current running tasks for each datasource.
   */
  Map<Pair<String,String>, Long> getRunningTaskCount();

  /**
   * Return the number of current pending tasks for each datasource.
   */
  Map<Pair<String,String>, Long> getPendingTaskCount();

  /**
   * Return the number of current waiting tasks for each datasource.
   */
  Map<Pair<String,String>, Long> getWaitingTaskCount();
}
