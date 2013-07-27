package com.trend.blueprints.util.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

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

import com.tinkerpop.blueprints.Direction;
import com.trend.blueprints.Graph;
import com.trend.blueprints.HBaseGraphConstants;
import com.trend.blueprints.HBaseGraphFactory;
import com.trend.blueprints.Vertex;
import com.trend.blueprints.test.AbstractHBaseMiniClusterTest;

public class GenerateTestDataTest extends AbstractHBaseMiniClusterTest {
  
  private static String firstVertex;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    Configuration conf = TEST_UTIL.getConfiguration();
    createTable(conf, Bytes.toBytes("test.vertex"), 
        transfer2BytesArray(new String[] {"property"}));
    createTable(conf, Bytes.toBytes("test.edge"), 
        transfer2BytesArray(new String[] {"property"}));
    
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, "test.vertex");
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, "test.edge");
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
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-h"});
  }
  
  @Test
  public void testRun_vertexCount_500() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-v", "500", "test.vertex", "test.edge"});
    
    assertCount(conf, "test.vertex", 500);
    assertCount(conf, "test.edge", 499);
    
    firstVertex = genTestData.getFirstVertex();
  }
  
  @Test
  public void testGraphIntegrity() {
    assertNotNull(firstVertex);
    Configuration conf = TEST_UTIL.getConfiguration();
    Graph graph = HBaseGraphFactory.open(conf);
    Vertex vertex = graph.getVertex(firstVertex);
    assertNotNull(vertex);
    assertEquals(10, vertex.getEdgeCount());
    
    Iterable<com.tinkerpop.blueprints.Edge> edges = vertex.getEdges();
    for(com.tinkerpop.blueprints.Edge edge : edges) {
      vertex = (Vertex) edge.getVertex(Direction.OUT);
      assertNotNull(vertex);
      assertEquals(10, vertex.getEdgeCount());
    }
    
    try{} finally {
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
