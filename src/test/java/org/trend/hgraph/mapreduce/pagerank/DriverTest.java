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

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.AbstractHBaseGraphTest;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

/**
 * @author scott_miao
 *
 */
public class DriverTest extends AbstractHBaseGraphTest {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    importData(
      new String[] {
          "-Dimporttsv.columns=HBASE_ROW_KEY,property:pageRank@Double",
        "-Dimporttsv.separator=|" }, "test.vertex-01", new String[] { "property" },
      "org/trend/hgraph/mapreduce/pagerank/vertex-test-01.data");

    importData(new String[] { "-Dimporttsv.columns=HBASE_ROW_KEY,property:dummy@String",
        "-Dimporttsv.separator=|" },
      "test.edge-01", new String[] { "property" },
      "org/trend/hgraph/mapreduce/pagerank/edge-test-01.data");

    printTable("test.vertex-01");
    printTable("test.edge-01");
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
        driver.run(new String[] { "test.vertex-01", "test.edge-01", "/pagerank-test-01" });
    assertEquals(0, retCode);
  }

}
