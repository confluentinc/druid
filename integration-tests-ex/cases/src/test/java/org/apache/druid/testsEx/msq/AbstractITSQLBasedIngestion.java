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

package org.apache.druid.testsEx.msq;

import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.testing.utils.MsqTestQueryHelper;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.testsEx.indexer.AbstractITBatchIndexTest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class AbstractITSQLBasedIngestion
{
  public static final Logger LOG = new Logger(TestQueryHelper.class);
  @Inject
  private MsqTestQueryHelper msqHelper;

  @Inject
  protected TestQueryHelper queryHelper;

  @Inject
  private DataLoaderHelper dataLoaderHelper;

  /**
   * Reads file as utf-8 string and replace %%DATASOURCE%% with the provide datasource value.
   */
  protected String getStringFromFileAndReplaceDatasource(String filePath, String datasource)
  {
    String fileString;
    try {
      InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(filePath);
      fileString = IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new ISE(e, "could not read query file: %s", filePath);
    }

    fileString = StringUtils.replace(
        fileString,
        "%%DATASOURCE%%",
        datasource
    );

    return fileString;
  }

  /**
   * Reads native queries from a file and runs against the provided datasource.
   */
  protected void doTestQuery(String queryFilePath, String dataSource)
  {
    try {
      String query = getStringFromFileAndReplaceDatasource(queryFilePath, dataSource);
      queryHelper.testQueriesFromString(query);
    }
    catch (Exception e) {
      LOG.error(e, "Error while running test query");
      throw new RuntimeException(e);
    }
  }

  /**
   * Sumits a sqlTask, waits for task completion and then runs test queries on ingested datasource.
   */
  protected void submitTaskAnddoTestQuery(String sqlTask, String queryFilePath, String datasource,
                                          Map<String, Object> msqContext) throws Exception
  {
    LOG.info("SqlTask - \n %s", sqlTask);

    // Submit the tasks and wait for the datasource to get loaded
    msqHelper.submitMsqTaskAndWaitForCompletion(
        sqlTask,
        msqContext
    );

    dataLoaderHelper.waitUntilDatasourceIsReady(datasource);
    doTestQuery(queryFilePath, datasource);
  }

  /**
   * Runs a MSQ ingest sql test.
   *
   * @param  sqlFilePath path of file containing the sql query.
   * @param  queryFilePath path of file containing the native test queries to be run on the ingested datasource.
   * @param  datasource name of the datasource. %%DATASOURCE%% in the sql and queries will be replaced with this value.
   * @param  msqContext context parameters to be passed with MSQ API call.
   */
  protected void runMSQTaskandTestQueries(String sqlFilePath, String queryFilePath, String datasource,
                                          Map<String, Object> msqContext) throws Exception
  {
    LOG.info("Starting MSQ test for [%s, %s]", sqlFilePath, queryFilePath);

    String sqlTask = getStringFromFileAndReplaceDatasource(sqlFilePath, datasource);
    submitTaskAnddoTestQuery(sqlTask, queryFilePath, datasource, msqContext);
  }
}
