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
import com.tinkerpop.blueprints.Vertex;

/**
 * An <code>Edge</code> impl.
 * @author scott_miao
 *
 */
public class Edge extends AbstractElement implements com.tinkerpop.blueprints.Edge {
  
  /**
   * @param result
   * @param graph
   */
  protected Edge(Result result, Graph graph) {
    super(result, graph);
  }


  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Edge#getLabel()
   */
  @Override
  public String getLabel() {
    String label = null;
    String id = (String)this.getId();
    int idx1 = id.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1);
    int idx2 = id.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_2, idx1 + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1.length());
    label = id.substring(idx1 + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1.length(), idx2);
    return label;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Edge#getVertex(com.tinkerpop.blueprints.Direction)
   */
  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    if(null == direction) return null;
    int idx = 0;
    String id = (String)this.getId();
    String vertexId = null;
    switch(direction) {
    
    case IN:
      idx = id.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1);
      vertexId = id.substring(0, idx);
      break;
    case OUT:
      idx = id.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1);
      idx = id.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_2, idx + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1.length());
      vertexId = id.substring(idx + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_2.length(), id.length());
      break;
     default:
       throw new IllegalArgumentException(
           "direction:" + direction + " is not supported");
    }
    return this.getGraph().getVertex(vertexId);
  }

}
