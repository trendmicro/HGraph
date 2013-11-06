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
package org.trend.hgraph.thinker;

import java.lang.reflect.Method;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.IndexTestSuite;
import com.tinkerpop.blueprints.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

public class ThinkerGraphTest extends GraphTest {

  public void testVertexTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new VertexTestSuite(this));
    printTestPerformance("VertexTestSuite", this.stopWatch());
  }

  public void testEdgeTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new EdgeTestSuite(this));
    printTestPerformance("EdgeTestSuite", this.stopWatch());
  }

  public void testGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphTestSuite(this));
    printTestPerformance("GraphTestSuite", this.stopWatch());
  }

  public void testKeyIndexableGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new KeyIndexableGraphTestSuite(this));
    printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
  }

  public void testIndexableGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new IndexableGraphTestSuite(this));
    printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
  }

  public void testIndexTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new IndexTestSuite(this));
    printTestPerformance("IndexTestSuite", this.stopWatch());
  }

  public void testGraphMLReaderTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphMLReaderTestSuite(this));
    printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
  }

  public void testGMLReaderTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GMLReaderTestSuite(this));
    printTestPerformance("GMLReaderTestSuite", this.stopWatch());
  }

  public void testGraphSONReaderTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphSONReaderTestSuite(this));
    printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
  }

  public Graph generateGraph() {
    return new TinkerGraph();
  }

  @Override
  public void doTestSuite(final TestSuite testSuite) throws Exception {
    String doTest = System.getProperty("testTinkerGraph");
    if (doTest == null || doTest.equals("true")) {
      for (Method method : testSuite.getClass().getDeclaredMethods()) {
        if (method.getName().startsWith("test")) {
          System.out.println("Testing " + method.getName() + "...");
          method.invoke(testSuite);
        }
      }
    }
  }

  @Override
  public Graph generateGraph(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

}
