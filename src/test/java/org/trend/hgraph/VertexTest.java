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
package org.trend.hgraph;

import static org.junit.Assert.*;

import org.apache.commons.lang.time.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.Graph;
import org.trend.hgraph.HBaseGraphFactory;
import org.trend.hgraph.Vertex;

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
