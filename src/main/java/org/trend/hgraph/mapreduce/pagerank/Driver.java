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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * A Drvier for running the pageRank program.
 * @author scott_miao
 */
public class Driver extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);

  private boolean includeVeticesTotalCount;
  private boolean importResults;
  private long pageRankThreshold = 0;

  /**
   * Default constructor
   */
  public Driver() {
    super();
  }

  /**
   * Constructor for test
   * @param conf
   */
  protected Driver(Configuration conf) {
    super(conf);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length == 0) {
      System.err.println("args is 0");
      printUsage();
      return 1;
    }

    String arg = null;
    int startMustIdx = -1;
    for (int a = 0; a < args.length; a++) {
      arg = args[a];
      if (arg.startsWith("-")) {
        if (startMustIdx >= 0) {
          System.err.println("options order incorrect !!");
          printUsage();
          return 1;
        }

        // optional
        if ("-c".equals(arg) || "--vertices-total-count".equals(arg)) {
          includeVeticesTotalCount = true;
        } else if ("-i".equals(arg) || "--import-result".equals(arg)) {
          importResults = true;
        } else if ("-h".equals(arg) || "--help".equals(arg)) {
          printUsage();
          return 0;
        } else if ("-t".equals(arg) || "--threshold".equals(arg)) {
          a++;
          String tmpArg = args[a];
          try {
            pageRankThreshold = Long.parseLong(tmpArg);
          } catch (NumberFormatException e) {
            System.err.println("parsing pageRank threshold failed, value:" + tmpArg);
            printUsage();
            return 1;
          }
        } else {
          System.err.println("Not a defined option:" + arg);
          printUsage();
          return 1;
        }
      } else {
        // must
        if (startMustIdx < 0) startMustIdx = a;
      }
    }

    if (startMustIdx + 3 != args.length) {
      System.err.println("The must options not satified !!");
      printUsage();
      return 1;
    }

    LOGGER.info("start to run " + this.getClass().getName() + " with options:" + args);

    Configuration conf = getConf();
    String vertexTableName = args[startMustIdx];
    String edgeTableName = args[startMustIdx + 1];
    String outputBasePath = args[startMustIdx + 2];

    LOGGER.info(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY + "=" + vertexTableName);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, vertexTableName);

    LOGGER.info(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY + "=" + edgeTableName);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, edgeTableName);

    LOGGER.info("outputBasePath=" + outputBasePath);

    // collect total vertices count
    if (includeVeticesTotalCount) {
      int retCode = 0;
      retCode = collectVeticesTotalCount(conf);
      if (retCode != 0) {
        System.err.println("run vertices total count job failed, with retCode:" + retCode);
        return retCode;
      }
    }

    // run pageRank
    boolean thresholdReached = false;
    boolean firstRun = true;
    Job job = null;
    boolean jobSucceed = false;
    long pageRankChangedCount = 0L;
    while (!thresholdReached) {
      if (firstRun) {
        firstRun = false;
        job = createInitialPageRankJob(conf, outputBasePath);
      } else {
        job = createInterMediatePageRankJob(conf, outputBasePath);
      }
      jobSucceed = job.waitForCompletion(true);
      if (!jobSucceed) {
        LOGGER.error("run job:" + job.getJobName() + " failed");
        return 1;
      }
      pageRankChangedCount = getPageRankChangedCount(job);
      if (pageRankChangedCount <= pageRankThreshold) {
        thresholdReached = true;
        LOGGER.info("threshold reached, pageRankThreshold:" + pageRankThreshold,
          ", pageRankChangedCount:" + pageRankChangedCount);
      }
    }

    if (importResults) {
      job = ImportPageRanks.createSubmittableJob(conf);
      jobSucceed = job.waitForCompletion(true);
      if (!jobSucceed) {
        LOGGER.error("run job:" + job.getJobName() + " failed !!");
        return 1;
      }
    }
    // run all jobs successful !!
    return 0;
  }

  private static long getPageRankChangedCount(Job job) throws IOException {
    long value = 0L;
    try {
      value =
          job.getCounters().findCounter(CalculatePageRankReducer.Counters.CHANGED_PAGE_RANK_COUNT)
              .getValue();
    } catch (IOException e) {
      LOGGER.error("get pageRankChangedCount failed", e);
      throw e;
    }
    LOGGER.info("pageRankChangedCount=" + value);
    return value;
  }

  private static Job createInterMediatePageRankJob(Configuration conf, String outputBasePath)
      throws IOException {
    long timestamp = System.currentTimeMillis();
    Job job = null;
    String jobName = null;
    try {
      jobName = "CalculateImtermediatePageRank_" + timestamp;
      LOGGER.info("start to run job:" + jobName);
      job = new Job(conf, jobName);
      job.setJarByClass(Driver.class);

      String inputPath = conf.get("mapred.output.dir");
      Validate.notEmpty(inputPath, "inputPath shall always not be empty");
      LOGGER.info("inputPath=" + inputPath);

      FileInputFormat.setInputPaths(job, new Path(inputPath));
      job.setMapperClass(CalculateIntermediatePageRankMapper.class);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setMapOutputKeyClass(BytesWritable.class);
      job.setMapOutputValueClass(DoubleWritable.class);

      job.setReducerClass(CalculatePageRankReducer.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      String outputPath = outputBasePath + "/" + timestamp;
      LOGGER.info("outputPath=" + outputPath);
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }
    return job;
  }

  private static Job createInitialPageRankJob(Configuration conf, String outputBasePath)
      throws IOException {
    String tableName = conf.get(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY);
    long timestamp = System.currentTimeMillis();
    Job job = null;
    String jobName = null;
    try {
      jobName = "CalculateInitPageRank_" + timestamp;
      LOGGER.info("start to run job:" + jobName);

      job = new Job(conf, jobName);
      job.setJarByClass(Driver.class);
      Scan scan = new Scan();

      TableMapReduceUtil.initTableMapperJob(tableName, scan, CalculateInitPageRankMapper.class,
        BytesWritable.class, DoubleWritable.class, job);
      job.setReducerClass(CalculatePageRankReducer.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      String outputPath = outputBasePath + "/" + timestamp;
      LOGGER.info("outputPath=" + outputPath);
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }
    return job;
  }

  private static int collectVeticesTotalCount(Configuration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    long totalCount = 1L;
    boolean success = false;
    Counter counter = null;
    String jobName = null;
    try {
      Job job = RowCounter.createSubmittableJob(conf, null);
      if (job == null) {
        System.err.println("job is null");
        return 1;
      }

      success = job.waitForCompletion(true);
      counter =
          job.getCounters().findCounter(
            "org.apache.hadoop.hbase.mapreduce.RowCounter.RowCounterMapper.Counters", "ROWS");
      jobName = job.getJobName();
      if (null != counter) {
        totalCount = counter.getValue();
        conf.set(Constants.PAGE_RANK_VERTICES_TOTAL_COUNT_KEY, totalCount + "");
      }
      LOGGER.info(Constants.PAGE_RANK_VERTICES_TOTAL_COUNT_KEY + "=" + totalCount);

    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    } catch (InterruptedException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    } catch (ClassNotFoundException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }

    return success ? 0 : -1;
  }

  private static final void printUsage() {
    System.err.println(Driver.class.getSimpleName()
            + " Usage: [-c] [-i] [-t numeric] <vertex-table-name> <edge-table-name> <output-base-path>");
    System.err.println("Run pageRank on the HBase for pre-defined HGraph schema");
    System.err.println("  -h, --help: print usage");
    System.err.println("  -t, --threshold: pageRank threshold, default is 0");
    System.err.println("  -c, --vertices-total-count: include the all vertices total count");
    System.err.println("  -i, --import-result: import pageRank results to <vertex-table-name>, default is false");
  }

  /**
   * Entry point.
   * @param args
   * @throws Exception
   * @see #printUsage()
   */
  public static final void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int retCode = ToolRunner.run(conf, new Driver(), args);
    System.exit(retCode);
  }

}
