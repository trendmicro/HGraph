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
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * A <code>Reducer</code> for calculating new pageranks by its upstream <code>Mapper</code>s.
 * @author scott_miao
 * @see CalculateInitPageRankMapper
 * @see CalculateIntermediatePageRankMapper
 */
public class CalculatePageRankReducer extends
    Reducer<BytesWritable, DoubleWritable, BytesWritable, DoubleWritable> {
  
  public static enum Counters {
    CHANGED_PAGE_RANK_COUNT
  }
  
  private double verticesTotalCnt = 1.0D;
  private double dampingFactor = Constants.PAGE_RANK_DAMPING_FACTOR_DEFAULT_VALUE;
  private String vertexTableName;
  private int pageRankCompareScale = 3;

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, Context)
   */
  @Override
  protected void reduce(BytesWritable key, Iterable<DoubleWritable> incomingPageRanks,
      Context context) throws IOException, InterruptedException {

    double incomingPageRankSum = 0.0D;
    for (DoubleWritable incomingPageRank : incomingPageRanks) {
      incomingPageRankSum = incomingPageRankSum + incomingPageRank.get();
    }
    // calculate new pageRank here
    double newPageRank =
        (dampingFactor * incomingPageRankSum) + ((1.0D - dampingFactor) / verticesTotalCnt);

    double oldPageRank = getPageRank(context.getConfiguration(), key.getBytes());
    if (!pageRankEquals(oldPageRank, newPageRank)) {
      // collect pageRank changing count with counter
      context.getCounter(Counters.CHANGED_PAGE_RANK_COUNT).increment(1);
    }
    context.write(key, new DoubleWritable(newPageRank));
  }

  private boolean pageRankEquals(double src, double dest) {
    BigDecimal a = new BigDecimal(src);
    BigDecimal b = new BigDecimal(dest);
    a = a.setScale(pageRankCompareScale, RoundingMode.DOWN);
    b = b.setScale(pageRankCompareScale, RoundingMode.DOWN);
    return a.compareTo(b) == 0 ? true : false;
  }

  private double getPageRank(Configuration conf, byte[] key) throws IOException {
    double pageRank = 0D;
    HTable table = null;
    Get get = null;
    Result r = null;
    try {
      table = new HTable(conf, vertexTableName);
      get = new Get(key);
      r = table.get(get);
      if (!r.isEmpty()) {
        byte[] colValue =
            r.getValue(Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
              Bytes
                  .toBytes("pageRank"
                      + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER
                      + "Double"));
        if (null != colValue) pageRank = Bytes.toDouble(colValue);
      }
    } catch (IOException e) {
      System.err.println("access HTable failed");
      e.printStackTrace(System.err);
      throw e;
    } finally {
      table.close();
    }
    return pageRank;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
   */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    String value = context.getConfiguration().get(Constants.PAGE_RANK_VERTICES_TOTAL_COUNT_KEY);
    if (null != value) verticesTotalCnt = Double.parseDouble(value);

    value = context.getConfiguration().get(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY);
    Validate.notEmpty(value, HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY
        + " shall be set before running this reducer:" + this.getClass().getName());
    vertexTableName = value;

    value = context.getConfiguration().get(Constants.PAGE_RANK_DAMPING_FACTOR_KEY);
    if (null != value) dampingFactor = Double.parseDouble(value);
  }

}
