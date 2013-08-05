/**
 * 
 */
package com.trend.blueprints;

import java.util.Set;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
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
  
  private final Graph graph;
  
  private String id;
  private Properties properties = new Properties();
  
  private static Logger LOG = LoggerFactory.getLogger(AbstractElement.class);
  
  /**
   * @param result
   * @param graph
   */
  protected AbstractElement(Result result, Graph graph) {
    super();
    this.graph = graph;
    this.extractValues(result);
  }
  
  private void extractValues(Result r) {
    this.id = Bytes.toString(r.getRow());
    if(r.value().length == 0) return;
    try {
      this.properties.addProperty(r);
    } catch (UnsupportedDataTypeException e) {
      LOG.error("proerties.addProperty failed", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the graph
   */
  protected Graph getGraph() {
    return graph;
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
  
  /**
   * Get property count
   * @return
   */
  public long getPropertyCount() {
    return this.properties.getCount();
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

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).
        append(id).
        toHashCode();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    AbstractElement rhs = (AbstractElement) obj;
    return new EqualsBuilder()
                  .append(this.id, rhs.id)
                  .isEquals();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE).
        append("id", id).
        append("properties", properties).
        toString();
  }
  
}
