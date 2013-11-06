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
package org.trend.hgraph.util.test;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;
import org.trend.hgraph.util.test.GenerateTestData;
import org.trend.hgraph.util.test.GetGeneratedGraphData;

public class GetGeneratedGraphDataTest extends AbstractHBaseMiniClusterTest {
  
  
  private static final String TEST_EDGE_1 = "test.edge.1";
  private static final String TEST_VERTEX_1 = "test.vertex.1";
  
  private static final String TEST_EDGE_2 = "test.edge.2";
  private static final String TEST_VERTEX_2 = "test.vertex.2";
  
  private static final String TEST_EDGE_3 = "test.edge.3";
  private static final String TEST_VERTEX_3 = "test.vertex.3";
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    Configuration conf = TEST_UTIL.getConfiguration();
    createTable(conf, Bytes.toBytes(TEST_VERTEX_1), 
        transfer2BytesArray(new String[] {"property"}));
    createTable(conf, Bytes.toBytes(TEST_EDGE_1), 
        transfer2BytesArray(new String[] {"property"}));
    
    createTable(conf, Bytes.toBytes(TEST_VERTEX_2), 
        transfer2BytesArray(new String[] {"property"}));
    createTable(conf, Bytes.toBytes(TEST_EDGE_2), 
        transfer2BytesArray(new String[] {"property"}));
    
    createTable(conf, Bytes.toBytes(TEST_VERTEX_3), 
        transfer2BytesArray(new String[] {"property"}));
    createTable(conf, Bytes.toBytes(TEST_EDGE_3), 
        transfer2BytesArray(new String[] {"property"}));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractHBaseMiniClusterTest.tearDownAfterClass();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testRun_1() throws Exception {
    // initial test data
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, TEST_VERTEX_1);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, TEST_EDGE_1);
    
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-v", "500", TEST_VERTEX_1, TEST_EDGE_1});
    
    List<String> firstVertices = genTestData.getFirstVertices();
    assertEquals(1, firstVertices.size());
    
    GetGeneratedGraphData getData = new GetGeneratedGraphData();
    getData.setConf(conf);
    getData.run(new String[] {"-i", firstVertices.get(0), TEST_VERTEX_1, TEST_EDGE_1});
    
  }
  
  @Test
  public void testRun_2() throws Exception {
    // initial test data
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, TEST_VERTEX_2);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, TEST_EDGE_2);
    
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-v", "100", "-d", "-ev", "1,2,3,4,5", 
        "-p", "0.2,0.2,0.2,0.2,0.2", TEST_VERTEX_2, TEST_EDGE_2});
    
    List<String> firstVertices = genTestData.getFirstVertices();
    assertEquals(5, firstVertices.size());
    
    StringBuffer sb = new StringBuffer();
    for(String rowKey : firstVertices) {
      if(sb.length() == 0) {
        sb.append(rowKey);
      } else {
        sb.append("," + rowKey);
      }
    }
    
    GetGeneratedGraphData getData = new GetGeneratedGraphData();
    getData.setConf(conf);
    getData.run(new String[] {"-i", sb.toString(), TEST_VERTEX_2, TEST_EDGE_2});
    
  }
  
  @Test
  public void testRun_3() throws Exception {
    // initial test data
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, TEST_VERTEX_3);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, TEST_EDGE_3);
    
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-v", "50", TEST_VERTEX_3, TEST_EDGE_3});
    
    GetGeneratedGraphData getData = new GetGeneratedGraphData();
    getData.setConf(conf);
    getData.run(new String[] {TEST_VERTEX_3, TEST_EDGE_3});
    
  }
}
