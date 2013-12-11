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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  private static final String TEST_EDGE_01 = "test.edge-01";
  private static final String TEST_VERTEX_01 = "test.vertex-01";
  private static final String CF_PROPERTY =
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME;
  private static final String CQ_DEL =
      HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER;
  private static final String EDGE_DEL_1 = HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1;
  private static final String EDGE_DEL_2 = HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_2;
  private static final String EDGE_LABEL = "link";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();

    createGraphTables();
    generateGraphData();

    printTable(TEST_VERTEX_01);
    printTable(TEST_EDGE_01);
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
    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] { TEST_VERTEX_01, TEST_EDGE_01, "/pagerank-test-01" });
    assertEquals(0, retCode);

    // get content
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path path = fs.getHomeDirectory();
    String finalOutputPath = driver.getFinalOutputPath();
    path = new Path(path, finalOutputPath + "/part-r-00000");
    InputStream is = fs.open(path);
    System.out.println("result.content=\n" + IOUtils.toString(is));
    IOUtils.closeQuietly(is);
  }
  
  @Test
  public void testPageRank_import() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int retCode =
        driver.run(new String[] {"-i", TEST_VERTEX_01, TEST_EDGE_01, "/pagerank-test-02" });
    assertEquals(0, retCode);
    printVertexPageRank();
  }
  
  private static void printVertexPageRank() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    HTable table = null;
    ResultScanner rs = null;
    try {
      table = new HTable(conf, TEST_VERTEX_01);
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

  private static void createGraphTables() throws IOException {
    HBaseAdmin admin = null;
    HTableDescriptor htd = null;
    HColumnDescriptor hcd = null;
    
    try {
      admin = TEST_UTIL.getHBaseAdmin();

      htd = new HTableDescriptor(TEST_VERTEX_01);
      hcd = new HColumnDescriptor(CF_PROPERTY);
      htd.addFamily(hcd);
      admin.createTable(htd);

      htd = new HTableDescriptor(TEST_EDGE_01);
      hcd = new HColumnDescriptor(CF_PROPERTY);
      htd.addFamily(hcd);
      admin.createTable(htd);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      throw e;
    }
  }

  private static void generateGraphData() throws IOException {
    HTable table = null;
    Put put = null;
    Configuration conf = TEST_UTIL.getConfiguration();
    // generate Vertex data
    try {
      table = new HTable(conf, TEST_VERTEX_01);
      table.setAutoFlush(false);
      for (int a = 1; a <= 5; a++) {
        put = new Put(Bytes.toBytes(("n" + a)));
        put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("pageRank" + CQ_DEL + "Double")),
          Bytes.toBytes((0.2D)));
        table.put(put);
      }
      table.flushCommits();
    } catch (IOException e) {
      System.err.println("generate vertex data failed");
      e.printStackTrace(System.err);
    } finally {
      table.close();
    }

    // generate Edge data
    try {
      table = new HTable(conf, TEST_EDGE_01);
      table.setAutoFlush(false);

      put = new Put(Bytes.toBytes(("n1" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n2")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n1" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n4")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n2" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n5")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n2" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n3")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n3" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n4")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n4" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n5")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n5" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n1")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n5" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n2")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      put = new Put(Bytes.toBytes(("n5" + EDGE_DEL_1 + EDGE_LABEL + EDGE_DEL_2 + "n3")));
      put.add(Bytes.toBytes(CF_PROPERTY), Bytes.toBytes(("dummy" + CQ_DEL + "String")),
        Bytes.toBytes("dummy"));
      table.put(put);

      table.flushCommits();
    } catch (IOException e) {
      System.err.println("generate edge data failed");
      e.printStackTrace(System.err);
    } finally {
      table.close();
    }
  }

}
