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

import java.io.IOException;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * A <code>Mapper</code> for calculating initial pagerank value from HBase.
 * @author scott_miao
 */
public class CalculateInitPageRankMapper extends TableMapper<BytesWritable, DoubleWritable> {

  private HTable edgeTable = null;
  private HTable vertexTable = null;
  
  private String tmpPageRankCq = Constants.PAGE_RANK_CQ_TMP_NAME;

  enum Counters {
    VERTEX_COUNT, GET_OUTGOING_VERTICES_TIME_CONSUMED, DISPATCH_PR_TIME_CONSUMED
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object,
   * org.apache.hadoop.mapreduce.Mapper.Context)
   */
  @Override
  protected void map(final ImmutableBytesWritable key, final Result value, final Context context)
      throws IOException, InterruptedException {
    String rowKey = Bytes.toString(key.get());
    double pageRank = Utils.getPageRank(value, Constants.PAGE_RANK_CQ_NAME);
    // write current pageRank to tmp
    Utils.writePageRank(vertexTable, rowKey, tmpPageRankCq, pageRank);
    long outgoingRowKeyCount = 0L;
    Configuration conf = context.getConfiguration();

    context.getCounter(Counters.VERTEX_COUNT).increment(1);
    outgoingRowKeyCount =
        getOutgoingRowKeysCount(conf, edgeTable, rowKey,
          context.getCounter(Counters.GET_OUTGOING_VERTICES_TIME_CONSUMED));
    dispatchPageRank(outgoingRowKeyCount, pageRank, conf, edgeTable, rowKey,
      context.getCounter(Counters.DISPATCH_PR_TIME_CONSUMED),
      new ContextWriterStrategy() {
      @Override
      public void write(String key, double value) throws IOException, InterruptedException {
        context.write(new BytesWritable(Bytes.toBytes(key)), new DoubleWritable(value));
      }
    });
  }

  static void dispatchPageRank(long outgoingRowKeyCount, double pageRank, Configuration conf,
      HTable edgeTable, String rowKey, Counter counter, ContextWriterStrategy strategy)
      throws IOException,
      InterruptedException {
    double pageRankForEachOutgoing = pageRank / outgoingRowKeyCount;
    long count = 0L;
    String outgoingRowKey = null;
    ResultScanner rs = null;
    StopWatch sw = null;
    try {
      Scan scan = getRowKeyOnlyScan(rowKey);
      sw = new StopWatch();
      sw.start();
      rs = edgeTable.getScanner(scan);
      for (Result r : rs) {
        outgoingRowKey = getOutgoingRowKey(r);
        strategy.write(outgoingRowKey, pageRankForEachOutgoing);
        count++;
      }
      sw.stop();
      counter.increment(sw.getTime());
    } catch (IOException e) {
      System.err.println("access htable:" + Bytes.toString(edgeTable.getTableName()) + " failed");
      e.printStackTrace(System.err);
      throw e;
    } finally {
      rs.close();
    }
    // check count status whether correct
    if (count != outgoingRowKeyCount) {
      String msg =
          "the count size not match for rowkey:" + rowKey + ", outgoingRowKeyCount:"
              + outgoingRowKeyCount + ", count:" + count;
      System.err.println(msg);
      throw new IllegalStateException(msg);
    }
  }

  private static Scan getRowKeyOnlyScan(String rowKey) {
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(rowKey
        + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1));
    scan.setStopRow(Bytes.toBytes(rowKey + "~"));
    scan.setFilter(new FirstKeyOnlyFilter());
    return scan;
  }

  interface ContextWriterStrategy {
    void write(String key, double value) throws IOException, InterruptedException;
  }

  static long getOutgoingRowKeysCount(Configuration conf, HTable edgeTable, String rowKey,
      Counter counter)
      throws IOException {
    long count = 0L;
    ResultScanner rs = null;
    StopWatch sw = null;
    try {
      Scan scan = getRowKeyOnlyScan(rowKey);
      sw = new StopWatch();
      sw.start();
      rs = edgeTable.getScanner(scan);
      for (@SuppressWarnings("unused") Result r : rs) {
        count++;
      }
      sw.stop();
      counter.increment(sw.getTime());
    } catch (IOException e) {
      System.err.println("access htable:" + Bytes.toString(edgeTable.getTableName()) + " failed");
      e.printStackTrace(System.err);
      throw e;
    } finally {
      rs.close();
    }
    return count;
  }

  private static String getOutgoingRowKey(Result r) {
    String rowKey;
    String outgoingRowKey;
    int idx;
    rowKey = Bytes.toString(r.getRow());
    idx = rowKey.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1);
    idx =
        rowKey.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_2, idx
            + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1.length());
    outgoingRowKey =
        rowKey.substring(idx + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_2.length(),
          rowKey.length());
    return outgoingRowKey;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
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
