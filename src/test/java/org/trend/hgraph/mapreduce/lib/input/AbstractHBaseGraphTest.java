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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
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

    // split table
    if (null != splits && splits.length > 0) {
      // wait for the table settle down
      Thread.sleep(6000);
      MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      byte[] table = Bytes.toBytes(tableName);
      List<HRegion> regions = null;
      for (int a = 0; a < splits.length; a++) {
        try {
          // split
          admin.split(table, Bytes.toBytes(splits[a]));
        } catch (InterruptedException e) {
          System.err.println("split table failed");
          e.printStackTrace(System.err);
          throw e;
        }
        Thread.sleep(6000);

        regions = cluster.getRegions(table);
        for (HRegion region : regions) {
          while (!region.isAvailable()) {
            // wait region online
          }
          System.out.println("region:" + region);
          System.out.println("startKey:" + Bytes.toString(region.getStartKey()));
          System.out.println("endKey:" + Bytes.toString(region.getEndKey()));
        }
      }
    }
  }
}
