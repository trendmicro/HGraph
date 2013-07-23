package com.trend.blueprints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.trend.blueprints.test.AbstractHBaseMiniClusterTest;

public class GraphTest extends AbstractHBaseMiniClusterTest {
  
  private static Graph graph = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    importData(
        new String[] {
            "-Dimporttsv.columns=HBASE_ROW_KEY,property:name,property:lang,property:age", 
            "-Dimporttsv.separator=|"}, 
        "test.vertex", 
        new String[] {"property"}, 
        "com/trend/blueprints/vertex-test.data");
    
    importData(
        new String[] {
            "-Dimporttsv.columns=HBASE_ROW_KEY,property:weight", 
            "-Dimporttsv.separator=|"}, 
        "test.edge", 
        new String[] {"property"}, 
        "com/trend/blueprints/edge-test.data");
    
    printTable("test.vertex");
    printTable("test.edge");
    
    Configuration config = TEST_UTIL.getConfiguration();
    config.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, "test.vertex");
    config.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, "test.edge");
    graph = HBaseGraphFactory.open(config);
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
  public void testGetEdge() {
    Edge edge = null;
    String id = "40012-created->40004";
    edge = graph.getEdge(id);
    assertNotNull(edge);
    assertEquals(id, edge.getId());
  }

  @Test
  public void testGetEdges() {
    Iterable<com.tinkerpop.blueprints.Edge> edges = null;
    edges = graph.getEdges();
    assertNotNull(edges);
    int count = 0;
    
    for(com.tinkerpop.blueprints.Edge edge : edges) {
      assertNotNull(edge);
      count++;
    }
    assertEquals(6, count);
  }

  @Test @Ignore
  public void testGetEdgesStringObject() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetVertex() {
    Vertex vertex = null;
    String id = "40004";
    vertex = graph.getVertex(id);
    assertNotNull(vertex);
    assertEquals(id, vertex.getId());
  }

  @Test
  public void testGetVertices() {
    Iterable<com.tinkerpop.blueprints.Vertex> vertices = null;
    vertices = graph.getVertices();
    assertNotNull(vertices);
    int count = 0;
    for(com.tinkerpop.blueprints.Vertex vertex : vertices) {
      assertNotNull(vertex);
      count++;
    }
    assertEquals(6, count);
  }

  @Test
  public void testGetVerticesStringObject() {
    Iterable<com.tinkerpop.blueprints.Vertex> vertices = null;
    vertices = graph.getVertices("lang", "java");
    assertNotNull(vertices);
    int count = 0;
    for(com.tinkerpop.blueprints.Vertex vertex : vertices) {
      assertNotNull(vertex);
      count++;
    }
    assertEquals(2, count);
  }

}
