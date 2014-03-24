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
package org.trend.hgraph.mapreduce.lib.input;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;


/**
 * A <code>Mapper</code> for calculating and preparing the <code>InputSplit</code>s file on HDFS.
 * For later pagerank jobs use.
 * @author scott_miao
 */
public class CalculateInputSplitMapper extends TableMapper<Text, Text> {

  public static final String BY_PASS_KEYS = "hgraph.mapreduce.lib.input.mappers.bypass.keys";
  public static final int BY_PASS_KEYS_DEFAULT_VALUE = 1000;

  enum Counters {
    ROW_COUNT, COLLECT_ROW_COUNT
  }

  private int bypassKeys = BY_PASS_KEYS_DEFAULT_VALUE;

  private String regionEncodedName = null;
  private HTable vertexTable = null;

  long totalCount = 0L;

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
      InterruptedException {
    context.getCounter(Counters.ROW_COUNT).increment(1L);
    totalCount++;
    boolean toWrite = true;

    // for debugging
    // String rowKey = Bytes.toString(key.get());
    // System.out.println("processing rowKey:" + rowKey);

    HRegionInfo info = vertexTable.getRegionLocation(key.get(), false).getRegionInfo();
    String encodedName = info.getEncodedName();
    if (null == regionEncodedName || "".equals(regionEncodedName)) {
      // first run
      // store regionEncodedName for subsequent use
      regionEncodedName = encodedName;

      // collect the start rowKey as well
      byte[] startKey = info.getStartKey();
      if (!Arrays.equals(HConstants.EMPTY_START_ROW, startKey)) {
        context.write(new Text(regionEncodedName), new Text(startKey));
        context.getCounter(Counters.COLLECT_ROW_COUNT).increment(1L);
        toWrite = false;
      }
    } else {
      toWrite = false;
      if (!regionEncodedName.equals(encodedName)) {
        throw new IllegalStateException(
            "This mapper span multiple regions, it is not the expected status !!\n"
                + " previous regionEncodedName:" + regionEncodedName
                + ", current regionEncodedName:" + encodedName);
      }
    }

    byte[] endKey = info.getEndKey();
    // hit the rowkey we want to collect
    if (toWrite || (totalCount % bypassKeys == 0 && !Arrays.equals(endKey, key.get()))) {
      context.write(new Text(regionEncodedName), new Text(key.get()));
      context.getCounter(Counters.COLLECT_ROW_COUNT).increment(1L);
    }

    // hit the end rowKey
    if (Arrays.equals(endKey, key.get()) && !Arrays.equals(HConstants.EMPTY_END_ROW, key.get())) {
      context.write(new Text(regionEncodedName), new Text(key.get()));
      context.getCounter(Counters.COLLECT_ROW_COUNT).increment(1L);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    vertexTable.close();
  }

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    bypassKeys = conf.getInt(BY_PASS_KEYS, bypassKeys);

    String vertexTableName = conf.get(TableInputFormat.INPUT_TABLE);
    if (null == vertexTableName || "".equals(vertexTableName)) {
      throw new IllegalArgumentException(TableInputFormat.INPUT_TABLE
          + " shall not be empty or null");
    }
    vertexTable = new HTable(conf, vertexTableName);
  }

}
