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
package org.trend.hgraph.mapreduce.pagerank;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.AbstractHBaseGraphTest;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

/**
 * @author scott_miao
 *
 */
public class DriverTest extends AbstractHBaseGraphTest {

  private static final String CF_PROPERTY =
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME;
  private static final String CQ_DEL =
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractHBaseGraphTest.tearDownAfterClass();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testPageRank_default() throws Exception {
    createGraphTables("test.vertex-01", "test.edge-01",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "test.vertex-01", "test.edge-01", "/pagerank-test-01" });
    assertEquals(0, retCode);

    // get content, for manual test purpose
    // FileSystem fs = TEST_UTIL.getTestFileSystem();
    // Path path = fs.getHomeDirectory();
    // String finalOutputPath = driver.getFinalOutputPath();
    // path = new Path(path, finalOutputPath + "/part-r-00000");
    // InputStream is = fs.open(path);
    // System.out.println("result.content=\n" + IOUtils.toString(is));
    // IOUtils.closeQuietly(is);
  }
  
  @Test
  public void testPageRank_import() throws Exception {
    createGraphTables("test.vertex-02", "test.edge-02",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "-i", "test.vertex-02", "test.edge-02", "/pagerank-test-02" });
    assertEquals(0, retCode);
    printVertexPageRank("test.vertex-02");
  }
  
  @Test
  public void testPageRank_import_totalCount() throws Exception {
    createGraphTables("test.vertex-03", "test.edge-03",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver
            .run(new String[] { "-i", "-c", "test.vertex-03", "test.edge-03",
            "/pagerank-test-03" });
    assertEquals(0, retCode);
    printVertexPageRank("test.vertex-03");
  }

  @Test
  public void testPageRank_import_threshold_2() throws Exception {
    createGraphTables("test.vertex-04", "test.edge-04",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "-i", "-t", "2", "test.vertex-04", "test.edge-04",
            "/pagerank-test-04" });
    assertEquals(0, retCode);
    printVertexPageRank("test.vertex-04");
  }

  private static void printVertexPageRank(String tableName) throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    HTable table = null;
    ResultScanner rs = null;
    try {
      table = new HTable(conf, tableName);
      rs = table.getScanner(new Scan());
      for (Result r : rs) {
        System.out.println("rowkey=" + Bytes.toString(r.getRow()) + ", ");
        System.out.println("pageRank="
            + Bytes.toDouble(r.getValue(Bytes.toBytes(CF_PROPERTY),
              Bytes.toBytes("pageRank" + CQ_DEL + "Double"))));
      }
    } catch (IOException e) {
      e.printStackTrace(System.err);
      throw e;
    } finally {
      rs.close();
      table.close();
    }
  }

  private static void createGraphTables(String vertexTableName, String edgeTableName, String cf)
      throws IOException {
    createGraphTable(vertexTableName, cf);
    createGraphTable(edgeTableName, cf);

    generateGraphDataTest("org/trend/hgraph/mapreduce/pagerank/vertex-test-01.data",
      vertexTableName, Constants.PAGE_RANK_CQ_NAME, true);
    generateGraphDataTest("org/trend/hgraph/mapreduce/pagerank/edge-test-01.data", edgeTableName,
      "dummy", false);

    printTable(vertexTableName);
    printTable(edgeTableName);
  }

  private static void createGraphTable(String tableName, String cf) throws IOException {
    HBaseAdmin admin = null;
    HTableDescriptor htd = null;
    HColumnDescriptor hcd = null;
    
    try {
      admin = TEST_UTIL.getHBaseAdmin();

      htd = new HTableDescriptor(tableName);
      hcd = new HColumnDescriptor(cf);
      htd.addFamily(hcd);
      admin.createTable(htd);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      throw e;
    }
  }

  private static void generateGraphDataTest(String dataPath, String tableName, String cq,
      boolean isDouble) throws IOException {
    InputStream data = DriverTest.class.getClassLoader().getResourceAsStream(dataPath);
    LineIterator it = IOUtils.lineIterator(new InputStreamReader(data));
    String record = null;
    Assert.assertEquals(true, it.hasNext());

    HTable table = null;
    try {
      table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      table.setAutoFlush(false);
      Put put = null;
      String[] values = null;
      while (it.hasNext()) {
        record = it.next();
        values = record.split("\\|");
        Assert.assertNotNull(values);
        Assert.assertEquals(2, values.length);
        put = new Put(Bytes.toBytes(values[0]));
        if (isDouble) {
          put.add(
            Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
            Bytes.toBytes(cq + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER
                + "Double"), Bytes.toBytes(Double.parseDouble(values[1])));
        } else {
          put.add(
            Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
            Bytes.toBytes(cq + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER
                + "String"), Bytes.toBytes(values[1]));
        }
        table.put(put);
      }
      table.flushCommits();
    } catch (IOException e) {
      System.err.println("generate data for table:" + tableName + " failed");
      e.printStackTrace(System.err);
      throw e;
    } finally {
      table.close();
      LineIterator.closeQuietly(it);
      IOUtils.closeQuietly(data);
    }
  }

}
