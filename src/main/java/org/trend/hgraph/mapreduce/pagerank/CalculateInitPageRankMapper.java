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
import java.util.LinkedList;
import java.util.List;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * A <code>Mapper</code> for calculating initial pagerank value from HBase.
 * @author scott_miao
 */
public class CalculateInitPageRankMapper extends TableMapper<Text, DoubleWritable> {

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
    List<String> outgoingRowKeys = null;
    Configuration conf = context.getConfiguration();

    context.getCounter(Counters.VERTEX_COUNT).increment(1);
    outgoingRowKeys =
        getOutgoingRowKeys(conf, vertexTable, edgeTable, rowKey,
          context.getCounter(Counters.GET_OUTGOING_VERTICES_TIME_CONSUMED));
    dispatchPageRank(outgoingRowKeys, pageRank, conf, edgeTable,
      context.getCounter(Counters.DISPATCH_PR_TIME_CONSUMED),
      new ContextWriterStrategy() {
      @Override
      public void write(String key, double value) throws IOException, InterruptedException {
          context.write(new Text(key), new DoubleWritable(value));
      }
    });
  }

  static void dispatchPageRank(List<String> outgoingRowKeys, double pageRank, Configuration conf,
      HTable edgeTable, Counter counter, ContextWriterStrategy strategy)
      throws IOException,
      InterruptedException {
    double pageRankForEachOutgoing = pageRank / (double) outgoingRowKeys.size();
    StopWatch sw = null;
    String outgoingRowKey = null;
    try {
      sw = new StopWatch();
      sw.start();
      for (int a = 0; a < outgoingRowKeys.size(); a++) {
        outgoingRowKey = outgoingRowKeys.get(a);
        strategy.write(outgoingRowKey, pageRankForEachOutgoing);
      }
      sw.stop();
      counter.increment(sw.getTime());
    } catch (IOException e) {
      System.err.println("failed while writing outgoingRowKey:" + outgoingRowKey);
      e.printStackTrace(System.err);
      throw e;
    } catch (InterruptedException e) {
      System.err.println("failed while writing outgoingRowKey:" + outgoingRowKey);
      e.printStackTrace(System.err);
      throw e;
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

  static List<String> getOutgoingRowKeys(Configuration conf, HTable vertexTable, HTable edgeTable,
      String rowKey, Counter counter) throws IOException {
    ResultScanner rs = null;
    String key = null;
    LinkedList<String> rowKeys = new LinkedList<String>();
    StopWatch sw = null;
    // Put put = null;
    try {
      Scan scan = getRowKeyOnlyScan(rowKey);
      sw = new StopWatch();
      sw.start();
      rs = edgeTable.getScanner(scan);
      for (Result r : rs) {
        key = getOutgoingRowKey(r);
        // collect outgoing rowkeys
        rowKeys.add(key);
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
    return rowKeys;
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
