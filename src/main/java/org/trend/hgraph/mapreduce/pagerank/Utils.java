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

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * A Utility class
 * @author scott_miao
 */
class Utils {

  static void writePageRank(HTable table, String rowkey, String columnQualifer, double pageRank) {
    Put put = null;

    put = new Put(Bytes.toBytes(rowkey));
    put.add(
      Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
      Bytes.toBytes(columnQualifer
          + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + "Double"),
      Bytes.toBytes(pageRank));
    try {
      table.put(put);
    } catch (IOException e) {
      System.err.println("writePageRank failed");
      e.printStackTrace(System.err);
    }
  }

  static double getPageRank(HTable table, String rowkey, String columnQualifer) throws IOException {
    Get get = null;
    Result r = null;
    try {
      get = new Get(Bytes.toBytes(rowkey));
      r = table.get(get);
    } catch (IOException e) {
      System.err.println("access HTable failed");
      e.printStackTrace(System.err);
      throw e;
    }
    return getPageRank(r, columnQualifer);
  }

  static double getPageRank(Result value, String columnQualifer) {
    byte[] colValue =
        value.getValue(
          Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
          Bytes.toBytes(columnQualifer
              + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + "Double"));
    double pageRank = 0.0D;
    if (null != colValue) {
      pageRank = Bytes.toDouble(colValue);
    }
    return pageRank;
  }

  static HTable initTable(Configuration conf, String tableKey,
      @SuppressWarnings("rawtypes") Class clazz) throws IOException {
    HTable table = null;
    String tableName = conf.get(tableKey);
    Validate.notEmpty(tableName, tableKey + " shall be set before running this task:"
        + clazz.getClass().getName());
    try {
      table = new HTable(conf, tableName);
    } catch (IOException e) {
      System.err.println("initial table:" + tableName + " failed");
      e.printStackTrace(System.err);
      throw e;
    }
    return table;
  }

}
