package com.trend.blueprints;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Direction;

public class EdgeTest extends AbstractHBaseGraphTest {
  private Graph graph = null;
  private Edge edge = null;

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
    this.edge = this.graph.getEdge("40012-created->40004");
    assertNotNull(edge);
  }

  @After
  public void tearDown() throws Exception {
    this.graph.shutdown();
  }
  
  @Test
  public void testGetProperty() {
    Object value = edge.getProperty("weight");
    assertNotNull(value);
    assertEquals("0.4", value);
  }
  
  @Test
  public void testGetPropertyKeys() {
    Set<String> keys = edge.getPropertyKeys();
    assertNotNull(keys);
    assertEquals(1, keys.size());
  }
  
  @Test
  public void testGetLabel() {
    String label = edge.getLabel();
    assertEquals("created", label);
  }
  @Test
  public void testGetVertex_IN() {
    Vertex vertex = null;
    vertex = (Vertex) edge.getVertex(Direction.IN);
    assertNotNull(vertex);
    assertEquals("40012", vertex.getId());
  }
  
  @Test
  public void testGetVertex_OUT() {
    Vertex vertex = null;
    vertex = (Vertex) edge.getVertex(Direction.OUT);
    assertNotNull(vertex);
    assertEquals("40004", vertex.getId());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testGetVertex_BOTH() {
    @SuppressWarnings("unused")
    Vertex vertex = null;
    vertex = (Vertex) edge.getVertex(Direction.BOTH);
  }
  
  
}
