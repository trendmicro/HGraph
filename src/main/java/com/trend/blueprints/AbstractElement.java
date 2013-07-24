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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  
  private String id;
  private Properties properties = new Properties();
  
  private static Logger LOG = LoggerFactory.getLogger(AbstractElement.class);
  
  /**
   * @param result
   * @param pool
   * @param vertexTableName
   * @param edgeTableName
   */
  protected AbstractElement(Result result, HTablePool pool, String vertexTableName,
      String edgeTableName) {
    super();
    this.pool = pool;
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;
    this.extractValues(result);
  }
  
  private void extractValues(Result r) {
    this.id = Bytes.toString(r.getRow());
    try {
      this.properties.addProperty(r);
    } catch (UnsupportedDataTypeException e) {
      LOG.error("proerties.addProperty failed", e);
      throw new RuntimeException(e);
    }
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
    return this.id;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#getProperty(java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T getProperty(String key) {
    return (T) this.properties.getProperty(key);
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#getPropertyKeys()
   */
  @Override
  public Set<String> getPropertyKeys() {
    return this.properties.getPropertyKeys();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#remove()
   */
  @Override
  public void remove() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#removeProperty(java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T removeProperty(String key) {
    return (T) this.properties.removeProperty(key);
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Element#setProperty(java.lang.String, java.lang.Object)
   */
  @Override
  public void setProperty(String key, Object value) {
    try {
      this.properties.setProperty(key, value);
    } catch (UnsupportedDataTypeException e) {
      LOG.error("properties.setProperty failed", e);
      throw new RuntimeException(e);
    }
  }
  
}
