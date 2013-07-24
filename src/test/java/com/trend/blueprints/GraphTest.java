package com.trend.blueprints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphTest extends AbstractHBaseGraphTest {
  
  private Graph graph = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseGraphTest.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractHBaseGraphTest.tearDownAfterClass();
  }

  @Before
  public void setUp() throws Exception {
    this.graph = HBaseGraphFactory.open(TEST_UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    this.graph.shutdown();
  }

  @Test
  public void testGetEdge() {
    Edge edge = null;
    String id = "40012-->created-->40004";
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

  @Test
  public void testGetEdgesStringObject() {
    Iterable<com.tinkerpop.blueprints.Edge> edges = null;
    edges = graph.getEdges("weight", "0.4");
    assertNotNull(edges);
    int count = 0;
    for(com.tinkerpop.blueprints.Edge vertex : edges) {
      assertNotNull(vertex);
      count++;
    }
    assertEquals(2, count);
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
