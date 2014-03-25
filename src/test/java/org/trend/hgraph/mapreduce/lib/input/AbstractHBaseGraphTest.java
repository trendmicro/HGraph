/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trend.hgraph.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

/**
 * @author scott_miao
 *
 */
public class AbstractHBaseGraphTest extends AbstractHBaseMiniClusterTest {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  protected static void createTestTable(String tableName, String... splits) throws Exception,
      IOException {
    importData(new String[] {
        "-Dimporttsv.columns=HBASE_ROW_KEY,property:name@String,property:lang@String",
        "-Dimporttsv.separator=|" }, tableName, new String[] { "property" },
      "org/trend/hgraph/mapreduce/lib/input/vertex-test.data");
    printTable(tableName);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, tableName);

    splitTable(tableName, splits);
  }
}
