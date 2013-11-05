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
package org.trend.blueprints;

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
    this.edge = this.graph.getEdge("40012-->created-->40004");
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
  public void testGetPropertyCount() {
    assertEquals(1, edge.getPropertyCount());
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
