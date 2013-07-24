/**
 * 
 */
package com.trend.blueprints;

import java.util.Set;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

/**
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
   * @see com.tinkerpop.blueprints.Edge#getLabel()
   */
  public String getLabel() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Edge#getVertex(com.tinkerpop.blueprints.Direction)
   */
  public Vertex getVertex(Direction arg0) throws IllegalArgumentException {
    // TODO Auto-generated method stub
    return null;
  }

}
