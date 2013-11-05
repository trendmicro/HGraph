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

import org.apache.hadoop.hbase.client.Result;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * The <code>Vertex</code> impl.
 * @author scott_miao
 *
 */
public class Vertex extends AbstractElement implements com.tinkerpop.blueprints.Vertex {
  
  /**
   * @param result
   * @param graph
   */
  protected Vertex(Result result, Graph graph) {
    super(result, graph);
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#addEdge(java.lang.String, com.tinkerpop.blueprints.Vertex)
   */
  @Override
  public Edge addEdge(String arg0, com.tinkerpop.blueprints.Vertex arg1) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /**
   * get <code>Edge</code>s.
   * @return the edges
   */
  public Iterable<com.tinkerpop.blueprints.Edge> getEdges() {
    return this.getGraph().getEdges(this);
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#getEdges(com.tinkerpop.blueprints.Direction, java.lang.String[])
   */
  @Override
  public Iterable<com.tinkerpop.blueprints.Edge> getEdges(Direction direction, String... labels) {
    if(null == direction || null == labels || labels.length == 0) return null;
    Iterable<com.tinkerpop.blueprints.Edge> edges = null;
    switch(direction) {
    case OUT:
      edges = this.getGraph().getEdges(this, labels);
      break;
    default:
      throw new RuntimeException("direction:" + direction + " is not supported");
    }
    
    return edges;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#getVertices(com.tinkerpop.blueprints.Direction, java.lang.String[])
   */
  @Override
  public Iterable<com.tinkerpop.blueprints.Vertex> getVertices(Direction direction,
      String... labels) {
    throw new UnsupportedOperationException("Due to memory usage consideration pls use getEdges instead");
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#query()
   */
  @Override
  public VertexQuery query() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  /**
   * get number of egdes for this <code>Vertex</code>.
   * @return
   */
  public long getEdgeCount() {
    return this.getGraph().getEdgeCount(this);
  }
  

}
