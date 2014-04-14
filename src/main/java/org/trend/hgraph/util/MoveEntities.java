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
package org.trend.hgraph.util;

import static org.trend.hgraph.util.FindCandidateEntities.isFileExists;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author scott_miao
 *
 */
public class MoveEntities extends Configured implements Tool {

  protected MoveEntities(Configuration conf) {
    super(conf);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length != 6) {
      System.err.println("not all 6 options given !!");
      printUsage();
      return -1;
    }

    String vFile = args[0];
    String eFile = args[1];
    String srcVt = args[2];
    String srcEt = args[3];
    String destVt = args[4];
    String destEt = args[5];

    if (!isFileExists(vFile)) {
      System.err.println("vertex-input-file does not exist !!");
      printUsage();
      return -1;
    }

    if (!isFileExists(eFile)) {
      System.err.println("edge-input-file does not exist !!");
      printUsage();
      return -1;
    }

    // move vertex data
    moveEntities(vFile, getConf(), srcVt, destVt);
    // move edge data
    moveEntities(eFile, getConf(), srcEt, destEt);

    return 0;
  }

  private static void moveEntities(String f, Configuration conf, String st, String dt)
      throws IOException {
    Reader fr = null;
    LineIterator it = null;
    
    String key = null;
    HTable sTable = null;
    HTable dTable = null;

    try {
      fr = new FileReader(f);
    } catch (FileNotFoundException e) {
      System.err.println("file:" + f + " does not exist");
      e.printStackTrace(System.err);
      throw e;
    }
    try {
    it = IOUtils.lineIterator(fr);
    if (it.hasNext()) {
      sTable = new HTable(conf, st);
      dTable = new HTable(conf, dt);
      Get get = null;
      Put put = null;
      Result r = null;
      byte[] bKey = null;
      byte[] cf = null;
      byte[] cq = null;
      byte[] v = null;
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfMap = null;
      NavigableMap<byte[], byte[]> cqMap = null;
      Entry<byte[], NavigableMap<byte[], byte[]>> cfEntry = null;
      Entry<byte[], byte[]> cqEntry = null;
        long cnt = 0L;
      while (it.hasNext()) {
        key = it.next();
        bKey = Bytes.toBytes(key);
        get = new Get(bKey);
        r = sTable.get(get);
        if (r.isEmpty()) {
          String msg = "result is empty for key:" + key + " from table:" + sTable;
          System.err.println(msg);
          throw new IllegalStateException(msg);
        }
        // dump the values from r to p
        put = new Put(bKey);
        cfMap = r.getNoVersionMap();
        for (Iterator<Entry<byte[], NavigableMap<byte[], byte[]>>> cfIt =
            cfMap.entrySet().iterator(); cfIt.hasNext();) {
          cfEntry = cfIt.next();
          cf = cfEntry.getKey();
          cqMap = cfEntry.getValue();
          for (Iterator<Entry<byte[], byte[]>> cqIt = cqMap.entrySet().iterator(); cqIt.hasNext();) {
            cqEntry = cqIt.next();
            cq = cqEntry.getKey();
            v = cqEntry.getValue();
            put.add(cf, cq, v);
          }
        }
        dTable.put(put);
          cnt++;
      }
        System.out.println("put " + cnt + " row(s) into table:" + dt);
    }
    } catch (IOException e) {
      System.err.println("error occurs while doing HBase manipultations !!");
      e.printStackTrace(System.err);
      throw e;
    } finally {
      LineIterator.closeQuietly(it);
      IOUtils.closeQuietly(fr);
      if (null != sTable) {
        sTable.close();
      }
      if (null != dTable) {
        dTable.close();
      }
    }
  }

  private static void printUsage() {
    System.err
        .println(MoveEntities.class.getSimpleName() + " Usage:"
            + " <vertex-input-file> <edge-input-file> <src-vertex-table> <src-edge-table> <dest-vertex-table> <dest-edge-table>");
    System.err.println("Move the keyvalues by given rowkeys stored in <vertex-input-file> and <edgge-input-file>,");
    System.err.println("from src tables to dest tables. Usually companion with " + FindCandidateEntities.class.getSimpleName());
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Tool tool = new MoveEntities(conf);
    int code = ToolRunner.run(tool, args);
    System.exit(code);
  }

}
