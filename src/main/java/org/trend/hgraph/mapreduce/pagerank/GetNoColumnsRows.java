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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author scott_miao
 *
 */
public class GetNoColumnsRows extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(GetNoColumnsRows.class);

  /**
   * only for test
   */
  private long collectedRow;

  protected GetNoColumnsRows(Configuration conf) {
    super(conf);
  }

  private static class Mapper extends TableMapper<Text, NullWritable> {

    public static final String AND_OR = "hgraph.mapreduce.nocolumns.and";
    public static final String NO_COLUMNS = "hgraph.mapreduce.nocolumns";

    private boolean and = true;
    private Pair<byte[][], byte[][]> pair = null;

    enum Counters {
      ROWS, COLLECTED_ROWS
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException,
        InterruptedException {
      context.getCounter(Counters.ROWS).increment(1L);

      List<Boolean> founds = null;
      Entry<byte[], NavigableMap<byte[], byte[]>> cfEntry = null;
      NavigableMap<byte[], byte[]> cqMap = null;
      Entry<byte[], byte[]> cqEntry = null;
      byte[] cf = null;
      byte[] cq = null;

      founds = new ArrayList<Boolean>(pair.getFirst().length);
      for (int a = 0; a < pair.getFirst().length; a++) {
        founds.add(Boolean.FALSE);
      }

      NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfMap = value.getNoVersionMap();
      for (Iterator<Entry<byte[], NavigableMap<byte[], byte[]>>> cfIt = cfMap.entrySet().iterator(); cfIt
          .hasNext();) {
        cfEntry = cfIt.next();
        cf = cfEntry.getKey();
        cqMap = cfEntry.getValue();
        for (Iterator<Entry<byte[], byte[]>> cqIt = cqMap.entrySet().iterator(); cqIt.hasNext();) {
          cqEntry = cqIt.next();
          cq = cqEntry.getKey();
          // whether hit target ?
          for (int a = 0; a < pair.getFirst().length; a++) {
            if (Arrays.equals(pair.getFirst()[a], cf) && Arrays.equals(pair.getSecond()[a], cq)) {
              founds.set(a, Boolean.TRUE);
              break;
            }
          }
        }
      }

      boolean write = false;
      if (and) {
        write = true;
        for (Boolean found : founds) {
          if (Boolean.TRUE.equals(found)) {
            write = false;
            break;
          }
        }
      } else {
        for (Boolean found : founds) {
          if (Boolean.FALSE.equals(found)) {
            write = true;
            break;
          }
        }
      }
      if (write) {
        context.write(new Text(key.get()), NullWritable.get());
        context.getCounter(Counters.COLLECTED_ROWS).increment(1L);
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      and = conf.getBoolean(AND_OR, true);
      pair = parseColumns(conf.getStrings(NO_COLUMNS));
    }

    private static Pair<byte[][], byte[][]> parseColumns(String[] columns) {
      Validate.notEmpty(columns, "columns shall not be empty or null");
      List<byte[]> first = new ArrayList<byte[]>();
      List<byte[]> second = new ArrayList<byte[]>();
      String[] cfcq = null;
      for (String column : columns) {
        cfcq = StringUtils.split(column, ":");
        Validate.notEmpty(cfcq, "split failed for column:" + column);
        Validate.isTrue(cfcq.length == 2, "parsed failed due to length is not 2, current length:"
            + cfcq.length);
        Validate.notEmpty(cfcq[0], "family is empty or null for column:" + column);
        Validate.notEmpty(cfcq[1], "qualifier is empty or null for column:" + column);

        first.add(Bytes.toBytes(cfcq[0]));
        second.add(Bytes.toBytes(cfcq[1]));
      }

      Validate.isTrue(first.size() == second.size(), "the parsed size is not equal, family.size:"
          + first.size() + ", qualifier.size:" + second.size());

      return new Pair<byte[][], byte[][]>(first.toArray(new byte[][] {}),
          second.toArray(new byte[][] {}));
    }

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length == 0) {
      System.err.println("no any option given !!");
      printUsage();
      return -1;
    }

    System.out.println("options:" + Arrays.toString(args));
    boolean and = true;
    String cmd = null;
    int mustStartIdx = -1;
    for (int a = 0; a < args.length; a++) {
      cmd = args[a];
      if (cmd.startsWith("-")) {
        if (mustStartIdx > -1) {
          System.err.println("option order is incorrect !!");
          printUsage();
          return -1;
        }

        if ("-a".equals(cmd)) {
          and = true;
        } else if ("-o".equals(cmd)) {
          and = false;
        } else {
          System.err.println("option is not defined !!");
          printUsage();
          return -1;
        }
      } else {
        if (mustStartIdx == -1) {
          mustStartIdx = a;
        }
      }
    }

    String tableName = args[mustStartIdx];
    String outputPath = args[mustStartIdx + 1];
    List<String> columns = new ArrayList<String>();
    for (int a = mustStartIdx + 2; a < args.length; a++) {
      columns.add(args[a]);
    }

    LOGGER.info("tableName=" + tableName);
    LOGGER.info("outputPath=" + outputPath);
    LOGGER.info("columns=" + columns);
    
    Configuration conf = this.getConf();
    conf.setBoolean(Mapper.AND_OR, and);
    conf.setStrings(Mapper.NO_COLUMNS, columns.toArray(new String[] {}));
    
    Job job = createSubmittableJob(conf, tableName, outputPath);
    boolean success = job.waitForCompletion(true);
    if (!success) {
      System.err.println("run job:" + job.getJobName() + " failed");
      return -1;
    }

    // for test
    Counter counter =
        job.getCounters().findCounter(
          "org.trend.hgraph.mapreduce.pagerank.GetNoColumnsRows$Mapper$Counters", "COLLECTED_ROWS");
    if (null != counter) {
      collectedRow = counter.getValue();
    }

    return 0;
  }

  public static Job createSubmittableJob(Configuration conf, String tableName, String outputPath)
      throws IOException {
    Validate.notEmpty(tableName, "tableName shall always not be empty");
    Validate.notEmpty(outputPath, "outputPath shall always not be empty");

    long timestamp = System.currentTimeMillis();
    Job job = null;
    String jobName = null;
    try {
      jobName = "GetNoCoumnsRows_" + timestamp;
      LOGGER.info("start to run job:" + jobName);
      job = new Job(conf, jobName);
      job.setJarByClass(GetNoColumnsRows.class);

      LOGGER.info("tableName=" + tableName);
      LOGGER.info("outputPath=" + outputPath);

      Scan scan = new Scan();
      TableMapReduceUtil.initTableMapperJob(tableName, scan, Mapper.class, Text.class,
        NullWritable.class, job, true, TableInputFormat.class);

      // only mapper
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setNumReduceTasks(0);

      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }
    return job;
  }

  private static void printUsage() {
    System.err.println(GetNoColumnsRows.class.getSimpleName()
        + " Usage: [-a | -o] <table> <output-path> <family:qualifier> [<family:qualifier> [...]]");
    System.err.println("  -a: AND (default), -o : OR");
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Tool tool = new GetNoColumnsRows(conf);
    int status = ToolRunner.run(tool, args);
    System.exit(status);
  }

  protected long getCollectedRow() {
    return collectedRow;
  }

}
