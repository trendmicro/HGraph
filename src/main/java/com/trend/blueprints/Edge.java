/**
 * 
 */
package com.trend.blueprints;

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
