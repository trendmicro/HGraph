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
public class EdgeIterable implements Iterable<com.tinkerpop.blueprints.Edge>  {
  
  private static final Logger LOG = LoggerFactory.getLogger(EdgeIterable.class);
  
  private Graph graph;
  private HTableInterface table;
  private ResultScanner rs;
  
  /**
   * @param rs
   */
  protected EdgeIterable(HTableInterface table, ResultScanner rs, Graph graph) {
    super();
    Validate.notNull(table, "table shall always not be null");
    Validate.notNull(rs, "rs shall always not be null");
    Validate.notNull(graph, "graph shall always not be null");
    this.table = table;
    this.rs = rs;
    this.graph = graph;
  }


  @Override
  public Iterator<com.tinkerpop.blueprints.Edge> iterator() {
    final Iterator<Result> r = this.rs.iterator();
    return new Iterator<com.tinkerpop.blueprints.Edge>() {
      
      @Override
      public boolean hasNext() {
        return r.hasNext();
      }

      @Override
      public com.tinkerpop.blueprints.Edge next() {
        Edge edge = null;
        try {
          edge = new Edge(rs.next(), graph);
        } catch (IOException e) {
          LOG.error("initial edge failed", e);
          throw new RuntimeException(e);
        }
        return edge;
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
