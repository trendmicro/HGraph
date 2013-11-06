/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trend.hgraph;

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
        return new Vertex(r.next(), graph);
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
