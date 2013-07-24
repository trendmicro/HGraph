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
  
  private static final String DELIMITER_1 = "-";
  private static final String DELIMITER_2 = "->";
  
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
  public String getLabel() {
    String label = null;
    String id = (String)this.getId();
    int idx1 = id.indexOf(DELIMITER_1);
    int idx2 = id.indexOf(DELIMITER_2);
    label = id.substring(idx1 + DELIMITER_1.length(), idx2);
    return label;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Edge#getVertex(com.tinkerpop.blueprints.Direction)
   */
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    if(null == direction) return null;
    int idx = 0;
    String id = (String)this.getId();
    String vertexId = null;
    switch(direction) {
    
    case IN:
      idx = id.indexOf(DELIMITER_1);
      vertexId = id.substring(0, idx);
      break;
    case OUT:
      idx = id.indexOf(DELIMITER_2);
      vertexId = id.substring(idx + DELIMITER_2.length(), id.length());
      break;
     default:
       throw new IllegalArgumentException(
           "direction:" + direction + " is not supported");
    }
    return this.getGraph().getVertex(vertexId);
  }

}
