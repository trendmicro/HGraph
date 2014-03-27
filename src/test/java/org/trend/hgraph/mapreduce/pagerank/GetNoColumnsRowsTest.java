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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

/**
 * @author scott_miao
 *
 */
public class GetNoColumnsRowsTest extends AbstractHBaseMiniClusterTest {

  private static final String TABLE_NAME = "getNoColumnsRowsTest";
  private static final String[] CFS = { "cf_1", "cf_2", "cf_3" };
  private static final String[] CQS = {"c_1", "c_2", "c_3"};
  
  private static final byte[] B_TABLE_NAME = Bytes.toBytes(TABLE_NAME);
  private static final byte[][] B_CFS;
  private static final byte[][] B_CQS;
  private static final byte[] B_V = Bytes.toBytes("v");
  
  static {
    B_CFS = transfer2BytesArray(CFS);
    B_CQS = transfer2BytesArray(CQS);
  }
  
  

// | row | cf_1:c_1 | cf_1:c_2 | cf_2:c_1 | cf_2:c_3 | cf_3:c_1 |
// | n1  | v        |          | v        |          |          | 
// | n2  | empty    |          |          | v        |          |
//

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    createTestData();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractHBaseMiniClusterTest.tearDownAfterClass();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testAnd_cf1c2() throws Exception {
    String outputPath = "/testAnd_cf1c2";
    GetNoColumnsRows tool = new GetNoColumnsRows(TEST_UTIL.getConfiguration());
    int status = tool.run(new String[] { TABLE_NAME, outputPath, CFS[0] + ":" + CQS[1] });
    Assert.assertEquals(0, status);
    Assert.assertEquals(2, tool.getCollectedRow());
  }

  @Test
  public void testAnd_cf1c1() throws Exception {
    String outputPath = "/testAnd_cf1c1";
    GetNoColumnsRows tool = new GetNoColumnsRows(TEST_UTIL.getConfiguration());
    int status = tool.run(new String[] { TABLE_NAME, outputPath, CFS[0] + ":" + CQS[0] });
    Assert.assertEquals(0, status);
    Assert.assertEquals(0, tool.getCollectedRow());
  }

  @Test
  public void testAnd_cf2c1() throws Exception {
    String outputPath = "/testAnd_cf2c1";
    GetNoColumnsRows tool = new GetNoColumnsRows(TEST_UTIL.getConfiguration());
    int status = tool.run(new String[] { TABLE_NAME, outputPath, CFS[1] + ":" + CQS[0] });
    Assert.assertEquals(0, status);
    Assert.assertEquals(1, tool.getCollectedRow());
  }

  @Test
  public void testAnd_cf1c2_cf2c3_cf3c1() throws Exception {
    String outputPath = "/testAnd_cf1c1_cf2c3_cf3c1";
    GetNoColumnsRows tool = new GetNoColumnsRows(TEST_UTIL.getConfiguration());
    int status =
        tool.run(new String[] { TABLE_NAME, outputPath, CFS[0] + ":" + CQS[1],
            CFS[1] + ":" + CQS[2], CFS[2] + ":" + CQS[0] });
    Assert.assertEquals(0, status);
    Assert.assertEquals(1, tool.getCollectedRow());
  }
  
  @Test
  public void testOr_cf1c1_cf2c3_cf3c1() throws Exception {
    String outputPath = "/testOr_cf1c1_cf2c3_cf3c1";
    GetNoColumnsRows tool = new GetNoColumnsRows(TEST_UTIL.getConfiguration());
    int status =
        tool.run(new String[] { "-o", TABLE_NAME, outputPath, CFS[0] + ":" + CQS[0],
            CFS[1] + ":" + CQS[2], CFS[2] + ":" + CQS[0] });
    Assert.assertEquals(0, status);
    Assert.assertEquals(2, tool.getCollectedRow());
  }

  @Test
  public void testOr_cf1c1_cf2c1() throws Exception {
    String outputPath = "/testOr_cf1c1_cf2c1";
    GetNoColumnsRows tool = new GetNoColumnsRows(TEST_UTIL.getConfiguration());
    int status =
        tool.run(new String[] { "-o", TABLE_NAME, outputPath, CFS[0] + ":" + CQS[0],
            CFS[1] + ":" + CQS[0] });
    Assert.assertEquals(0, status);
    Assert.assertEquals(1, tool.getCollectedRow());
  }

  private static void createTestData() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    createTable(conf, B_TABLE_NAME, B_CFS);
    HTable table = null;
    Put put = null;
    try {
      table = new HTable(conf, B_TABLE_NAME);

      put = new Put(Bytes.toBytes("n1"));
      put.add(B_CFS[0], B_CQS[0], B_V);
      put.add(B_CFS[1], B_CQS[0], B_V);
      table.put(put);

      put = new Put(Bytes.toBytes("n2"));
      put.add(B_CFS[0], B_CQS[0], null);
      put.add(B_CFS[1], B_CQS[2], B_V);
      table.put(put);
    } catch (IOException e) {
      System.err.println("createTestData failed !!");
      e.printStackTrace(System.err);
      throw e;
    } finally {
      table.close();
    }
  }

}
