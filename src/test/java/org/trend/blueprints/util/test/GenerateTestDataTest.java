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
package org.trend.blueprints.util.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.trend.blueprints.Graph;
import org.trend.blueprints.HBaseGraphConstants;
import org.trend.blueprints.HBaseGraphFactory;
import org.trend.blueprints.Vertex;
import org.trend.blueprints.test.AbstractHBaseMiniClusterTest;

import com.tinkerpop.blueprints.Direction;

public class GenerateTestDataTest extends AbstractHBaseMiniClusterTest {
  
  private static final String TEST_EDGE_500 = "test.edge.500";
  private static final String TEST_VERTEX_500 = "test.vertex.500";
  
  private static final String TEST_EDGE_100 = "test.edge.100";
  private static final String TEST_VERTEX_100 = "test.vertex.100";
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    Configuration conf = TEST_UTIL.getConfiguration();
    
    createTable(conf, Bytes.toBytes(TEST_VERTEX_500), 
        transfer2BytesArray(new String[] {"property"}));
    createTable(conf, Bytes.toBytes(TEST_EDGE_500), 
        transfer2BytesArray(new String[] {"property"}));
    
    createTable(conf, Bytes.toBytes(TEST_VERTEX_100), 
        transfer2BytesArray(new String[] {"property"}));
    createTable(conf, Bytes.toBytes(TEST_EDGE_100), 
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

  @Test @Ignore // enable it if you want to see usage print
  public void testMain_printUsage() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, TEST_VERTEX_500);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, TEST_EDGE_500);
    
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-h"});
  }
  
  @Test
  public void testRun_vertexCount_500() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, TEST_VERTEX_500);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, TEST_EDGE_500);
    
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-v", "500", TEST_VERTEX_500, TEST_EDGE_500});
    
    assertCount(conf, TEST_VERTEX_500, 500);
    assertCount(conf, TEST_EDGE_500, 499);
    
    List<String> firstVertex = genTestData.getFirstVertices();
    assertEquals(1, firstVertex.size());
    testGraphIntegrity_500(conf, firstVertex.get(0));
  }
  
  private static void testGraphIntegrity_500(Configuration conf, String firstVertex) {
    Graph graph = null;
    Vertex vertex = null;
    try {
      graph = HBaseGraphFactory.open(conf);
      vertex = graph.getVertex(firstVertex);
    
      assertNotNull(vertex);
      assertEquals(10, vertex.getEdgeCount());
    
      Iterable<com.tinkerpop.blueprints.Edge> edges = vertex.getEdges();
      for(com.tinkerpop.blueprints.Edge edge : edges) {
        vertex = (Vertex) edge.getVertex(Direction.OUT);
        assertNotNull(vertex);
        assertEquals(10, vertex.getEdgeCount());
      }
    
    } finally {
      if(null != graph) graph.shutdown();
    }
  }
  
  @Test
  public void testRun_vertexCount_100() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, TEST_VERTEX_100);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, TEST_EDGE_100);
    
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {
        "-v", "100", "-d", "-ev", "1,2,3,4,5", "-p", "0.2,0.2,0.2,0.2,0.2", 
        TEST_VERTEX_100, TEST_EDGE_100});
    
    assertCount(conf, TEST_VERTEX_100, 100);
    assertCount(conf, TEST_EDGE_100, 99);
    
    List<String> firstVertices = genTestData.getFirstVertices();
    assertEquals(5, firstVertices.size());
    testGraphIntegrity_100(conf, firstVertices);
  }
  
  private static void testGraphIntegrity_100(Configuration conf, List<String> firstVertices) {
    Graph graph = null;
    Vertex vertex = null;
    String vertexId = null;
    try {
      graph = HBaseGraphFactory.open(conf);
      for(int a = 0; a < firstVertices.size(); a++) {
        vertexId = firstVertices.get(a);
        vertex = graph.getVertex(vertexId);
      
        assertNotNull(vertex);
        assertEquals((a + 1), vertex.getEdgeCount());
      }
    } finally {
      if(null != graph) graph.shutdown();
    }
  }

  private void assertCount(Configuration conf, String tableName, int assertCount) 
      throws IOException {
    HTable table = null;
    ResultScanner rs = null;
    try {
      table = new HTable(conf, tableName);
      Scan scan = new Scan();
      scan.setFilter(new FirstKeyOnlyFilter());
      rs = table.getScanner(scan);
      int count = 0;
      for(@SuppressWarnings("unused") Result r : rs) {
        count++;
      }
      assertEquals(assertCount, count);
    } finally {
      if(null != rs) rs.close();
      if(null != table) table.close();
    }
  }

}
