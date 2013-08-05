/**
 * 
 */
package com.trend.blueprints;

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
