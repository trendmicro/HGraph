/**
 * 
 */
package com.trend.blueprints;

import java.util.Set;
import java.util.HashSet;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.hbase.client.Result;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * The <code>Vertex</code> impl.
 * @author scott_miao
 *
 */
public class Vertex extends AbstractElement implements com.tinkerpop.blueprints.Vertex {
  
  private Set<Edge> edges = null;
  
  /**
   * @param result
   * @param graph
   */
  protected Vertex(Result result, Graph graph) {
    super(result, graph);
  }

  /**
   * @return the edges
   */
  protected Set<Edge> getEdges() {
    return edges;
  }

  /**
   * @param edges the edges to set
   */
  protected void setEdges(Set<Edge> edges) {
    Validate.notNull(edges, "edges shall always not be null");
    this.edges = edges;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#addEdge(java.lang.String, com.tinkerpop.blueprints.Vertex)
   */
  public Edge addEdge(String arg0, com.tinkerpop.blueprints.Vertex arg1) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#getEdges(com.tinkerpop.blueprints.Direction, java.lang.String[])
   */
  public Iterable<com.tinkerpop.blueprints.Edge> getEdges(Direction direction, String... labels) {
    if(null == direction || null == labels || labels.length == 0) return null;
    Set<com.tinkerpop.blueprints.Edge> edges = new HashSet<com.tinkerpop.blueprints.Edge>();
    switch(direction) {
    case OUT:
      for(Edge edge : this.edges) {
        for(String label : labels) {
          if(edge.getLabel().equals(label)) {
            edges.add(edge);
            break;
          }
        }
      }
      break;
    default:
      throw new RuntimeException("direction:" + direction + " is not supported");
    }
    
    return edges;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#getVertices(com.tinkerpop.blueprints.Direction, java.lang.String[])
   */
  public Iterable<com.tinkerpop.blueprints.Vertex> getVertices(Direction direction,
      String... labels) {
    Iterable<com.tinkerpop.blueprints.Edge> edges = this.getEdges(direction, labels);
    if(null == edges) return null;
    Set<com.tinkerpop.blueprints.Vertex> vertices = 
        new HashSet<com.tinkerpop.blueprints.Vertex>();
    
    for(com.tinkerpop.blueprints.Edge edge : edges) {
      vertices.add(edge.getVertex(direction));
    }
    return vertices;
  }

  /* (non-Javadoc)
   * @see com.tinkerpop.blueprints.Vertex#query()
   */
  public VertexQuery query() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.trend.blueprints.AbstractElement#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE).
        appendSuper(super.toString()).
        append("edges", edges).
        toString();
  }

}
