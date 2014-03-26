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
import static org.junit.Assert.assertTrue;

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

  private static final String TEST_DATA_EDGE_01 = "org/trend/hgraph/mapreduce/pagerank/edge-test-01.data";
  private static final String TEST_DATA_VERTEX_01 = "org/trend/hgraph/mapreduce/pagerank/vertex-test-01.data";
  
  private static final String TEST_DATA_EDGE_02 = "org/trend/hgraph/mapreduce/pagerank/edge-test-02.data";
  private static final String TEST_DATA_VERTEX_02 = "org/trend/hgraph/mapreduce/pagerank/vertex-test-02.data";
  
  private static final String CF_PROPERTY =
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME;
  private static final String CQ_DEL =
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER;

  private static final String CF_PR = Constants.PAGE_RANK_CQ_NAME;
  private static final String CF_PRT = Constants.PAGE_RANK_CQ_TMP_NAME;
  private static final String CF_PRU = Constants.PAGE_RANK_CQ_UPDATED_NAME;

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
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_01,
      TEST_DATA_EDGE_01);

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
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_01,
      TEST_DATA_EDGE_01);

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
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_01,
      TEST_DATA_EDGE_01);

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
  public void testPageRank_import_totalCount_manually() throws Exception {
    createGraphTables("test.vertex-05", "test.edge-05",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_01,
      TEST_DATA_EDGE_01);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "-i", "-g", "3", "test.vertex-05", "test.edge-05",
            "/pagerank-test-05" });
    assertEquals(0, retCode);
    printVertexPageRank("test.vertex-05");
  }

  @Test
  public void testPageRank_import_totalCount_manually_fail() throws Exception {
    createGraphTables("test.vertex-06", "test.edge-06",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_01,
      TEST_DATA_EDGE_01);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "-i", "-g", "3", "-c", "test.vertex-06", "test.edge-06",
            "/pagerank-test-06" });
    assertTrue(0 != retCode);
  }

  @Test
  public void testPageRank_import_threshold_2() throws Exception {
    createGraphTables("test.vertex-04", "test.edge-04",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_01,
      TEST_DATA_EDGE_01);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "-i", "-t", "2", "test.vertex-04", "test.edge-04",
            "/pagerank-test-04" });
    assertEquals(0, retCode);
    printVertexPageRank("test.vertex-04");
  }

  @Test
  public void testPageRank_import_iteration_5() throws Exception {
    createGraphTables("test.vertex-07", "test.edge-07",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_01,
      TEST_DATA_EDGE_01);

    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "-i", "-e", "5", "test.vertex-07", "test.edge-07",
            "/pagerank-test-07" });
    assertEquals(0, retCode);
    printVertexPageRank("test.vertex-07");
  }

  @Test
  public void testPageRank_import_totalCount_manual_inputsplits_1() throws Exception {
    createGraphTables("test.vertex-08", "test.edge-08",
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME, TEST_DATA_VERTEX_02,
      TEST_DATA_EDGE_02);

    splitTable("test.vertex-08", "n08", "n17");

    Configuration conf = TEST_UTIL.getConfiguration();

    // prepare splitsPath
    String splitsPath = "/splitsinput-test-08";
    org.trend.hgraph.mapreduce.lib.input.Driver driver_1 =
        new org.trend.hgraph.mapreduce.lib.input.Driver(conf);
    int code =
 driver_1.run(new String[] { "-b", "1", "test.vertex-08", splitsPath });
    Assert.assertEquals(0, code);

    // run test
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { "-c", "-i", "-p", splitsPath + "/part-r-00000", "test.vertex-08",
            "test.edge-08", "/pagerank-test-08" });
    assertEquals(0, retCode);
    printVertexPageRank("test.vertex-08");
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
            + Bytes.toString(r.getValue(Bytes.toBytes(CF_PROPERTY),
              Bytes.toBytes(CF_PR + CQ_DEL + "String"))));
        System.out.println("tmpPageRank="
            + Bytes.toString(r.getValue(Bytes.toBytes(CF_PROPERTY),
              Bytes.toBytes(CF_PRT + CQ_DEL + "String"))));
        System.out.println("pageRankUpdated="
            + Bytes.toString(r.getValue(Bytes.toBytes(CF_PROPERTY),
              Bytes.toBytes(CF_PRU + CQ_DEL + "String"))));

      }
    } catch (IOException e) {
      e.printStackTrace(System.err);
      throw e;
    } finally {
      rs.close();
      table.close();
    }
  }

  private static void createGraphTables(String vertexTableName, String edgeTableName, String cf,
      String vertexDataPath, String edgeDataPath)
      throws IOException {
    createGraphTable(vertexTableName, cf);
    createGraphTable(edgeTableName, cf);

    generateGraphDataTest(vertexDataPath, vertexTableName, Constants.PAGE_RANK_CQ_NAME, false);
    generateGraphDataTest(edgeDataPath, edgeTableName, "dummy", false);

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
