/**
 * 
 */
package com.trend.blueprints;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author scott_miao
 *
 */
public class VertexIterable implements Iterable<com.tinkerpop.blueprints.Vertex>  {
  
  private static final Logger LOG = LoggerFactory.getLogger(VertexIterable.class);
  
  private Graph graph;
  private HTableInterface table;
  private ResultScanner rs;
  
  /**
   * @param rs
   */
  protected VertexIterable(HTableInterface table, ResultScanner rs, Graph graph) {
    super();
    Validate.notNull(table, "table shall always not be null");
    Validate.notNull(rs, "rs shall always not be null");
    Validate.notNull(graph, "graph shall always not be null");
    this.table = table;
    this.rs = rs;
    this.graph = graph;
  }


  @Override
  public Iterator<com.tinkerpop.blueprints.Vertex> iterator() {
    final Iterator<Result> r = this.rs.iterator();
    return new Iterator<com.tinkerpop.blueprints.Vertex>() {
      
      @Override
      public boolean hasNext() {
        return r.hasNext();
      }

      @Override
      public com.tinkerpop.blueprints.Vertex next() {
        Vertex vertex = null;
        try {
          vertex = new Vertex(rs.next(), graph);
        } catch (IOException e) {
          LOG.error("initial vertex failed", e);
          throw new RuntimeException(e);
        }
        return vertex;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#finalize()
   */
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    try {
      this.rs.close();
      this.graph.returnTable(this.table);
    } catch(Exception e) {
      LOG.error("finalize error", e);
      throw e;
    }
  }
  
}
