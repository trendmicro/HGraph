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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A <code>Reducer</code> for calculating and preparing the <code>InputSplit</code>s file on HDFS.
 * For later pagerank jobs use.
 * @author scott_miao
 */
public class CalculateInputSplitReducer extends Reducer<Text, Text, Text, NullWritable> {

  private static final String DELIMITER = "\t";

  public static String MAPPERS_FOR_ONE_REGION = "hgraph.mapreduce.pagerank.mappers.one.region";
  public static int MAPPERS_FOR_ONE_REGION_DEFAULT_VALUE = 3;
  private int mappersForOneRegion = MAPPERS_FOR_ONE_REGION_DEFAULT_VALUE;

  @Override
  protected void reduce(Text regionKey, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    String regionName = Bytes.toString(regionKey.getBytes()).trim();
    List<String> rowKeys = new ArrayList<String>();
    int count = 0;
    for (Text rowKey : values) {
      rowKeys.add(Bytes.toString(rowKey.getBytes()).trim());
      count++;
    }

    if (mappersForOneRegion > count) {
      throw new IllegalArgumentException(MAPPERS_FOR_ONE_REGION
          + " shall not bigger than total count:" + count + " for region:" + regionName);
    }

    int baseBuckets = count / mappersForOneRegion;
    int extraBuckets = count % mappersForOneRegion;
    if (baseBuckets < 2) {
      throw new IllegalStateException(
          "baseBuckets:" + baseBuckets
          + " shall bigger than 2, otherwise it will make one or more pairs with duplicate of start/end rowKeys");
    }

    int[] bucketsForEachMapper = new int[mappersForOneRegion];
    for (int a = bucketsForEachMapper.length - 1; a >= 0; a--) {
      bucketsForEachMapper[a] = baseBuckets;
      if (extraBuckets > 0) {
        bucketsForEachMapper[a] = bucketsForEachMapper[a] + 1;
        extraBuckets--;
      }
    }

    System.out.println("bucketsForEachMapper=" + Arrays.toString(bucketsForEachMapper));
    int buckets = 0;
    int idx = 0;
    String startRowKey = null;
    String endRowKey = null;
    for (int a = 0; a < bucketsForEachMapper.length; a++) {
      buckets = bucketsForEachMapper[a];
      startRowKey = rowKeys.get(idx);
      if ((a + 1) == bucketsForEachMapper.length) {
        buckets -= 1;
      }
      idx = idx + buckets;
      endRowKey = rowKeys.get(idx);
      // write one regionName, startRowKey and endRowKey pair
      context.write(new Text(regionName + DELIMITER + startRowKey + DELIMITER + endRowKey),
        NullWritable.get());
    }

    // do housekeeping
    rowKeys.clear();
    rowKeys = null;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    mappersForOneRegion = conf.getInt(MAPPERS_FOR_ONE_REGION, mappersForOneRegion);
  }

}
