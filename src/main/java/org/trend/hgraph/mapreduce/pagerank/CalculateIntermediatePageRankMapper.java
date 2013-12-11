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

import static org.trend.hgraph.mapreduce.pagerank.CalculateInitPageRankMapper.collectOutgoingRowKeys;
import static org.trend.hgraph.mapreduce.pagerank.CalculateInitPageRankMapper.dispatchPageRank;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.mapreduce.pagerank.CalculateInitPageRankMapper.ContextWriterStrategy;
import org.trend.hgraph.mapreduce.pagerank.CalculateInitPageRankMapper.Counters;

/**
 * A <code>Mapper</code> for calculating intermediate pagerank value from HDFS.
 * @author scott_miao
 */
public class CalculateIntermediatePageRankMapper extends
    Mapper<BytesWritable, DoubleWritable, BytesWritable, DoubleWritable> {

  private String edgeTableName = null;

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, Context)
   */
  @Override
  protected void map(final BytesWritable key, final DoubleWritable value, final Context context)
      throws IOException, InterruptedException {
    String rowKey = Bytes.toString(key.getBytes()).trim();
    double pageRank = value.get();
    List<String> outgoingRowKeys = null;

    context.getCounter(Counters.VERTEX_COUNT).increment(1);
    outgoingRowKeys = collectOutgoingRowKeys(context.getConfiguration(), edgeTableName, rowKey);
    dispatchPageRank(outgoingRowKeys, pageRank,
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
    edgeTableName =
        context.getConfiguration().get(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY);
    Validate.notEmpty(edgeTableName, HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY
        + " shall be set before running this Mapper:" + this.getClass().getName());
  }

}
