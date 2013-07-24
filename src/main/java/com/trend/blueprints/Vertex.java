/**
 * 
 */
package com.trend.blueprints;

import java.util.Set;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * The <code>Vertex</code> object.
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
   * @see com.tinkerpop.blueprints.Element#getProperty(java.lang.String)
   */
  public <T> T getProperty(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#getPropertyKeys()
   */
  public Set<String> getPropertyKeys() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#remove()
   */
  public void remove() {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#removeProperty(java.lang.String)
   */
  public <T> T removeProperty(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#setProperty(java.lang.String, java.lang.Object)
   */
  public void setProperty(String arg0, Object arg1) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#addEdge(java.lang.String, com.tinkerpop.blueprints.Vertex)
   */
  public Edge addEdge(String arg0, com.tinkerpop.blueprints.Vertex arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#getEdges(com.tinkerpop.blueprints.Direction, java.lang.String[])
   */
  public Iterable<Edge> getEdges(Direction arg0, String... arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#getVertices(com.tinkerpop.blueprints.Direction, java.lang.String[])
   */
  public Iterable<com.tinkerpop.blueprints.Vertex> getVertices(Direction arg0,
      String... arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#query()
   */
  public VertexQuery query() {
    // TODO Auto-generated method stub
    return null;
  }

}
