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
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
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
          + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + "String"),
      Bytes.toBytes(("" + pageRank)));
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
              + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + "String"));
    double pageRank = 0.0D;
    if (null != colValue) {
      try {
        pageRank = Double.parseDouble(Bytes.toString(colValue));
      } catch (NumberFormatException e) {
        String rowkey = Bytes.toString(value.getRow());
        System.err.println("parse pageRank failed for row:" + rowkey);
      }
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

  /**
   * Set token if authentication enabled.
   * @param job
   */
  static void setAuthenticationToken(Job job, Logger logger) {
    if (User.isSecurityEnabled()) {
      try {
        User.getCurrent().obtainAuthTokenForJob(job.getConfiguration(), job);
      } catch (IOException e) {
        String msg = job.getJobName() + ": Failed to obtain current user.";
        logger.error(msg);
        throw new IllegalStateException(msg, e);
      } catch (InterruptedException ie) {
        logger.warn(job.getJobName() + ": Interrupted obtaining user authentication token");
      }
    }
  }

}
