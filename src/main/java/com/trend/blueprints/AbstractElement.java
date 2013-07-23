/**
 * 
 */
package com.trend.blueprints;

import java.math.BigDecimal;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.tinkerpop.blueprints.Element;

/**
 * Base class for graph elements. 
 * @author scott_miao
 *
 */
public abstract class AbstractElement implements Element {
  
  private final HTablePool pool;
  private final String vertexTableName;
  private final String edgeTableName;
  
  private Result result;
  
  /**
   * @param result
   * @param pool
   * @param vertexTableName
   * @param edgeTableName
   */
  protected AbstractElement(Result result, HTablePool pool, String vertexTableName,
      String edgeTableName) {
    super();
    this.result = result;
    this.pool = pool;
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;
  }

  /**
   * @return the result
   */
  protected Result getResult() {
    return result;
  }

  /**
   * @return the pool
   */
  protected HTablePool getPool() {
    return pool;
  }

  /**
   * @return the vertexTableName
   */
  protected String getVertexTableName() {
    return vertexTableName;
  }

  /**
   * @return the edgeTableName
   */
  protected String getEdgeTableName() {
    return edgeTableName;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#getId()
   */
  @Override
  public Object getId() {
    return Bytes.toString(this.getResult().getRow());
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#getProperty(java.lang.String)
   */
  @Override
  public <T> T getProperty(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#getPropertyKeys()
   */
  @Override
  public Set<String> getPropertyKeys() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#remove()
   */
  @Override
  public void remove() {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#removeProperty(java.lang.String)
   */
  @Override
  public <T> T removeProperty(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#setProperty(java.lang.String, java.lang.Object)
   */
  @Override
  public void setProperty(String arg0, Object arg1) {
    // TODO Auto-generated method stub

  }
  
}
