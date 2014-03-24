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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author scott_miao
 *
 */
public class TableInputFormatTest extends AbstractHBaseGraphTest {

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseGraphTest.setUpBeforeClass();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractHBaseGraphTest.tearDownAfterClass();
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
  public void testGetSplits_b4_m3() throws IOException, Exception {
    // init test table
    String tableName = "test.vertex-01";
    String outputPath = "/test-01";
    createTestTable(tableName, "00030", "00060");

    // init inputSplits MR
    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int code = driver.run(new String[] { "-b", "4", tableName, outputPath });
    Assert.assertEquals(0, code);

    // print prepared inputsplit file
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path path = fs.getHomeDirectory();
    path = new Path(path, outputPath + "/part-r-00000");
    InputStream is = fs.open(path);
    System.out.println("result.content=\n" + IOUtils.toString(is));
    IOUtils.closeQuietly(is);

    // start to test
    conf.set(TableInputFormat.INPUT_SPLIT_PAIRS_INPUT_PATH, outputPath + "/part-r-00000");
    conf.set(TableInputFormat.INPUT_TABLE, tableName);
    Job job = new Job();
    TableInputFormat tif = new TableInputFormat();
    tif.setConf(conf);
    List<InputSplit> splits = tif.getSplits(job);
    System.out.println("splits content");
    TableSplit split = null;
    for (int a = 0; a < splits.size(); a++) {
      split = (TableSplit) splits.get(a);
      System.out.println("splits[" + a + "].location=" + split.getRegionLocation());
      System.out.println("splits[" + a + "].startKey=" + Bytes.toString(split.getStartRow()));
      System.out.println("splits[" + a + "].endKey=" + Bytes.toString(split.getEndRow()));
    }
  }

  @Test
  public void testSplitKeys() {
    // compare empty key
    byte[] key_1 = { 0 };
    byte[] key_2 = { 0, 0, 0, 0, 0, 0 };
    assertEquals(-5, Bytes.compareTo(key_1, key_2));
    assertEquals(5, Bytes.compareTo(key_2, key_1));

    // compare same content but length
    key_1 = new byte[] { 0, 0, 0, 0, 1 };
    key_2 = new byte[] { 0, 0, 0, 0, 1, 0 };
    assertEquals(-1, Bytes.compareTo(key_1, key_2));
    assertEquals(1, Bytes.compareTo(key_2, key_1));

    // compare diff value
    key_1 = new byte[] { 0, 0, 0, 1, 0 };
    assertEquals(1, Bytes.compareTo(key_1, key_2));
    assertEquals(-1, Bytes.compareTo(key_2, key_1));

    key_1 = new byte[] { 1 };
    key_2 = new byte[] { 0, 0, 0, 0, 0, 0 };
    assertEquals(1, Bytes.compareTo(key_1, key_2));
    assertEquals(-1, Bytes.compareTo(key_2, key_1));
  }

}
