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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trend.hgraph.HBaseGraphConstants;


/**
 * A <code>Mapper</code> for calculating and preparing the <code>InputSplit</code>s file on HDFS.
 * For later pagerank jobs use.
 * @author scott_miao
 */
public class CalculateInputSplitMapper extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(CalculateInputSplitMapper.class);

  protected CalculateInputSplitMapper(Configuration conf) {
    super(conf);
  }

  static class Mapper extends TableMapper<Text, Text> {

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
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
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
    protected void setup(Context context) throws IOException, InterruptedException {
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
  private static Job createInputSplitMapperJob(Configuration conf, String outputPath)
      throws IOException {
    String tableName = conf.get(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY);
    long timestamp = System.currentTimeMillis();
    Job job = null;
    String jobName = null;
    try {
      jobName = "InputSplitMapper_" + timestamp;
      LOGGER.info("start to run job:" + jobName);

      job = new Job(conf, jobName);
      job.setJarByClass(CalculateInputSplitMapper.class);
      Scan scan = new Scan();

      TableMapReduceUtil.initTableMapperJob(tableName, scan, Mapper.class,
        Text.class, Text.class, job);
      job.setOutputFormatClass(TextOutputFormat.class);
      LOGGER.info("outputPath=" + outputPath);
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length == 0) {
      System.err.println("no any option specified !!");
      printUsage();
      return -1;
    }

    int bypassRowKeys = Mapper.BY_PASS_KEYS_DEFAULT_VALUE;
    String cmd = null;
    int mustStartIdx = -1;
    for (int a = 0; a < args.length; a++) {
      cmd = args[a];
      if (cmd.startsWith("-")) {
        if (mustStartIdx != -1) {
          System.err.println("option order is incorrect !!");
          printUsage();
          return -1;
        }

        if ("-b".equals(cmd) || "--bypass-rowkeys".equals(cmd)) {
          a++;
          cmd = args[a];
          try {
            bypassRowKeys = Integer.parseInt(cmd);
          } catch (NumberFormatException e) {
            System.err.println("-b shall be numeric value, b:" + cmd);
            printUsage();
            return -1;
          }
        } else {
          System.err.println("option:" + cmd + " is undefined !!");
          printUsage();
          return -1;
        }

      } else {
        if (mustStartIdx == -1) {
          mustStartIdx = a;
          break;
        }
      }
    }

    if (mustStartIdx + 2 != args.length) {
      System.err.println("must options is unsatisfied !!");
      printUsage();
      return -1;
    }

    Configuration conf = this.getConf();
    String tableName = args[mustStartIdx];
    String outputPath = args[mustStartIdx + 1];

    LOGGER.info("tableName=" + tableName);
    LOGGER.info("outputPath=" + outputPath);

    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, tableName);
    conf.setInt(Mapper.BY_PASS_KEYS, bypassRowKeys);

    Job job = createInputSplitMapperJob(conf, outputPath);
    boolean success = job.waitForCompletion(true);

    return success ? 0 : -1;
  }

  private static void printUsage() {
    System.err.print(CalculateInputSplitMapper.class.getSimpleName() + "Usage: ");
    System.err.println("[-b <n>] <table-name> <output-path>");
    System.err
        .println("  -b, --bypass-rowkeys: speficy how many rowkeys to bypass for one region, default is 1000");
  }

  public static final void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Tool tool = new CalculateInputSplitMapper(conf);
    int status = ToolRunner.run(tool, args);
    System.exit(status);
  }

}
