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
package org.trend.hgraph.mapreduce.pagerank;

import static org.trend.hgraph.mapreduce.pagerank.CalculateInitPageRankMapper.dispatchPageRank;
import static org.trend.hgraph.mapreduce.pagerank.CalculateInitPageRankMapper.getOutgoingRowKeys;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.mapreduce.pagerank.CalculateInitPageRankMapper.ContextWriterStrategy;

/**
 * A <code>Mapper</code> for calculating intermediate pagerank value from HDFS.
 * @author scott_miao
 */
public class CalculateIntermediatePageRankMapper extends
    Mapper<BytesWritable, DoubleWritable, BytesWritable, DoubleWritable> {

  private HTable edgeTable = null;
  private HTable vertexTable = null;
  
  private String tmpPageRankCq = Constants.PAGE_RANK_CQ_TMP_NAME;

  enum Counters {
    VERTEX_COUNT, GET_OUTGOING_VERTICES_TIME_CONSUMED, DISPATCH_PR_TIME_CONSUMED
  }
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, Context)
   */
  @Override
  protected void map(final BytesWritable key, final DoubleWritable value, final Context context)
      throws IOException, InterruptedException {
    String rowKey = Bytes.toString(key.getBytes()).trim();
    double pageRank = value.get();
    // write current pageRank to tmp
    Utils.writePageRank(vertexTable, rowKey, tmpPageRankCq, pageRank);
    
    Configuration conf = context.getConfiguration();
    List<String> outgoingRowKeys = null;

    context.getCounter(Counters.VERTEX_COUNT).increment(1);
    outgoingRowKeys =
        getOutgoingRowKeys(conf, edgeTable, rowKey,
          context.getCounter(Counters.GET_OUTGOING_VERTICES_TIME_CONSUMED));
    dispatchPageRank(outgoingRowKeys, pageRank, conf, edgeTable,
      context.getCounter(Counters.DISPATCH_PR_TIME_CONSUMED),
      new ContextWriterStrategy() {
        @Override
        public void write(String key, double value) throws IOException, InterruptedException {
          context.write(new BytesWritable(Bytes.toBytes(key)), new DoubleWritable(value));
        }
      });
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#setup(Context)
   */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    vertexTable =
        Utils.initTable(conf, HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY,
          this.getClass());
    edgeTable =
        Utils.initTable(conf, HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, this.getClass());
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
   */
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    vertexTable.close();
    edgeTable.close();
  }

}
