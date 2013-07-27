/**
 * 
 */
package com.trend.blueprints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;

/**
 * @author scott_miao
 *
 */
public class Graph implements com.tinkerpop.blueprints.Graph {
  
  private final HTablePool POOL;
  private final Configuration CONF;
  
  private final String VERTEX_TABLE_NAME;
  private final String EDGE_TABLE_NAME;
  
  private static final Logger LOG = LoggerFactory.getLogger(Graph.class);
  
  /**
   * @param pool
   * @param conf
   */
  protected Graph(HTablePool pool, Configuration conf) {
    super();
    this.POOL = pool;
    this.CONF = conf;
    
    String vertexTableName = this.CONF.get(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY);
    Validate.notEmpty(vertexTableName, HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY + " shall not be null or empty");
    this.VERTEX_TABLE_NAME = vertexTableName;
    
    String edgeTableName = this.CONF.get(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY);
    Validate.notEmpty(edgeTableName, HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY + " shall not be null or empty");
    this.EDGE_TABLE_NAME = edgeTableName;
    
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#addEdge(java.lang.Object, com.tinkerpop.blueprints.Vertex, com.tinkerpop.blueprints.Vertex, java.lang.String)
   */
  @Override
  public Edge addEdge(Object arg0, com.tinkerpop.blueprints.Vertex arg1, 
      com.tinkerpop.blueprints.Vertex arg2, String arg3) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#addVertex(java.lang.Object)
   */
  @Override
  public Vertex addVertex(Object arg0) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#getEdge(java.lang.Object)
   */
  @Override
  public Edge getEdge(Object key) {
    if(null == key) return null;
    
    Result r = getResult(key, this.EDGE_TABLE_NAME);
    if(r.isEmpty()) return null;
    
    Edge edge = new Edge(r, this);
    return edge;
  }

  private Result getResult(Object key, String tableName) {
    HTableInterface table = this.POOL.getTable(tableName);
    Get get = new Get(Bytes.toBytes(key.toString()));
    Result r;
    try {
      r = table.get(get);
    } catch (IOException e) {
      LOG.error("getEdge failed", e);
      throw new RuntimeException(e);
    } finally {
      this.POOL.putTable(table);
    }
    return r;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#getEdges()
   */
  @Override
  public Iterable<com.tinkerpop.blueprints.Edge> getEdges() {
    HTableInterface table = this.POOL.getTable(EDGE_TABLE_NAME);
    Scan scan = new Scan();
    ResultScanner rs = null;
    List<com.tinkerpop.blueprints.Edge> edges = new ArrayList<com.tinkerpop.blueprints.Edge>();
    
    try {
      rs = table.getScanner(scan);
      for(Result r : rs) {
        edges.add(new Edge(r, this));
      }
    } catch (IOException e) {
      LOG.error("getEdges failed", e);
      throw new RuntimeException(e);
    } finally {
      this.POOL.putTable(table);
    }
    return edges;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#getEdges(java.lang.String, java.lang.Object)
   */
  @Override
  public Iterable<com.tinkerpop.blueprints.Edge> getEdges(String key, Object value) {
    List<com.tinkerpop.blueprints.Edge> edges = new ArrayList<com.tinkerpop.blueprints.Edge>();
    
    collectElements(key, value, 
        new CollectElementStrategy<com.tinkerpop.blueprints.Edge>(this.EDGE_TABLE_NAME, edges) {
          @Override
          com.tinkerpop.blueprints.Edge newElement(Result r, Graph graph) {
            return new Edge(r, graph);
          }
    });
    return edges;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#getFeatures()
   */
  @Override
  public Features getFeatures() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#getVertex(java.lang.Object)
   */
  @Override
  public Vertex getVertex(Object id) {
    if(null == id) return null;
    
    Result r = getResult(id, this.VERTEX_TABLE_NAME);
    if(r.isEmpty()) return null;
    
    Vertex vertex = this.getVertex(r, this);
    return vertex;
  }
  
  private Vertex getVertex(Result r, Graph graph) {
    Vertex vertex = new Vertex(r, graph);
    Set<Edge> edges = new HashSet<Edge>();
    HTableInterface table = this.POOL.getTable(EDGE_TABLE_NAME);
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(vertex.getId() + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1));
    scan.setStopRow(Bytes.toBytes(vertex.getId() + "~"));
    ResultScanner rs;
    try {
      rs = table.getScanner(scan);
      for(Result r1 : rs) {
        edges.add(new Edge(r1, graph));
      }
    } catch (IOException e) {
      LOG.error("egdse.add failed", e);
      throw new RuntimeException(e);
    } finally {
      this.POOL.putTable(table);
    }
    vertex.setEdges(edges);
    
    return vertex;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#getVertices()
   */
  @Override
  public Iterable<com.tinkerpop.blueprints.Vertex> getVertices() {
    HTableInterface table = this.POOL.getTable(VERTEX_TABLE_NAME);
    Scan scan = new Scan();
    ResultScanner rs = null;
    List<com.tinkerpop.blueprints.Vertex> vertices = new ArrayList<com.tinkerpop.blueprints.Vertex>();
    
    try {
      rs = table.getScanner(scan);
      for(Result r : rs) {
        vertices.add(this.getVertex(r, this));
      }
    } catch (IOException e) {
      LOG.error("getVertices failed", e);
      throw new RuntimeException(e);
    } finally {
      this.POOL.putTable(table);
    }
    return vertices;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#getVertices(java.lang.String, java.lang.Object)
   */
  @Override
  public Iterable<com.tinkerpop.blueprints.Vertex> getVertices(String key, Object value) {
    List<com.tinkerpop.blueprints.Vertex> vertices = new ArrayList<com.tinkerpop.blueprints.Vertex>();
    
    collectElements(key, value, 
        new CollectElementStrategy<com.tinkerpop.blueprints.Vertex>(this.VERTEX_TABLE_NAME, vertices) {
          @Override
          com.tinkerpop.blueprints.Vertex newElement(Result r, Graph graph) {
            return getVertex(r, graph);
          }
    });
    return vertices;
  }
  
  private static abstract class CollectElementStrategy <T extends com.tinkerpop.blueprints.Element> {
    private String tableName;
    private List<T> elements;
    /**
     * @param tableName
     * @param elements
     */
    CollectElementStrategy(String tableName, List<T> elements) {
      super();
      this.tableName = tableName;
      this.elements = elements;
    }
    
    abstract T newElement(Result r, Graph graph);
    
    String getTableName() {
      return this.tableName;
    }
    
    void addElement(T element) {
      this.elements.add(element);
    }
    
  }
  
  private <T extends com.tinkerpop.blueprints.Element> void collectElements(
      String key, Object value, CollectElementStrategy<T> strategy) {
    if(null == key || "".equals(key) || null == value) return ;
    Validate.notNull(strategy, "strategy shall always not be null");
    
    HTableInterface table = this.POOL.getTable(strategy.getTableName());
    Properties.Pair<byte[], byte[]> pair = null;
    Scan scan = new Scan();
    try {
      pair = Properties.keyValueToBytes(key, value);
    } catch (UnsupportedDataTypeException e) {
      LOG.error("valueToBytes failed", e);
      throw new RuntimeException(e);
    }
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME), pair.key, CompareOp.EQUAL, pair.value);
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    try {
      ResultScanner rs = table.getScanner(scan);
      for(Result r : rs) {
        strategy.addElement(strategy.newElement(r, this));
      }
    } catch (IOException e) {
      LOG.error("getScanner failed", e);
      throw new RuntimeException(e);
    }
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#query()
   */
  @Override
  public GraphQuery query() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#removeEdge(com.tinkerpop.blueprints.Edge)
   */
  @Override
  public void removeEdge(com.tinkerpop.blueprints.Edge arg0) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#removeVertex(com.tinkerpop.blueprints.Vertex)
   */
  @Override
  public void removeVertex(com.tinkerpop.blueprints.Vertex arg0) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Graph#shutdown()
   */
  @Override
  public void shutdown() {
    try {
      this.POOL.close();
    } catch (IOException e) {
      LOG.error("close POOL failed", e);
      throw new RuntimeException(e);
    }
  }

}
