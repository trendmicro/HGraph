package com.trend.blueprints;

import static org.junit.Assert.*;

import org.apache.commons.lang.time.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Direction;

public class VertexTest extends AbstractHBaseGraphTest {
  
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
  public void testGetEdgesDirectionOutStringArray() {
    Vertex vertex = this.graph.getVertex("40012");
    System.out.println("vertex=" + vertex);
    assertNotNull(vertex);
    Iterable<com.tinkerpop.blueprints.Edge> edges = 
        vertex.getEdges(Direction.OUT, "knows", "foo", "bar");
    assertNotNull(edges);
    int count = 0;
    for(com.tinkerpop.blueprints.Edge edge : edges) {
      assertNotNull(edge);
      count++;
    }
    assertEquals(2, count);
  }
  
  @Test(expected=RuntimeException.class)
  public void testGetEdgesDirectionInStringArray() {
    Vertex vertex = this.graph.getVertex("40004");
    System.out.println("vertex=" + vertex);
    assertNotNull(vertex);
    @SuppressWarnings("unused")
    Iterable<com.tinkerpop.blueprints.Edge> edges = 
        vertex.getEdges(Direction.IN, "created");
  }
  @Test
  public void testGetEdges() {
    Vertex vertex = this.graph.getVertex("40012");
    assertNotNull(vertex);
    Iterable<com.tinkerpop.blueprints.Edge> edges = vertex.getEdges();
    int count = 0;
    for(com.tinkerpop.blueprints.Edge edge : edges) {
      assertNotNull(edge);
      count++;
    }
    assertEquals(3, count);
  }
  
  @Test
  public void testGetEdgeCount() {
    Vertex vertex = this.graph.getVertex("40012");
    assertNotNull(vertex);
    assertEquals(3, vertex.getEdgeCount());
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testGetVerticesDirectionOutStringArray() {
    Vertex vertex = this.graph.getVertex("40012");
    System.out.println("vertex=" + vertex);
    assertNotNull(vertex);
    Iterable<com.tinkerpop.blueprints.Vertex> vertices = 
        vertex.getVertices(Direction.OUT, "knows", "foo", "bar");
    assertNotNull(vertices);
    int count = 0;
    for(com.tinkerpop.blueprints.Vertex v : vertices) {
      assertNotNull(v);
      count++;
    }
    assertEquals(2, count);
  }
  
  @Test(expected=RuntimeException.class)
  public void testGetVerticesDirectionInStringArray() {
    Vertex vertex = this.graph.getVertex("40004");
    System.out.println("vertex=" + vertex);
    assertNotNull(vertex);
    @SuppressWarnings("unused")
    Iterable<com.tinkerpop.blueprints.Vertex> vertices = 
        vertex.getVertices(Direction.IN, "created");
  }

}
