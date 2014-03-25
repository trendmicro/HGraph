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
import java.util.Arrays;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
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

  static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);

  private boolean includeVeticesTotalCount;
  private boolean importResults;
  private long pageRankThreshold = 0L;
  private long pageRankIterations = -1L;
  private long verticesTotalCount = -1L;
  private String inputSplitsPath = null;

  /** for test usage */
  private String finalOutputPath;

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

  /**
   * @return the finalOutputPath
   */
  protected String getFinalOutputPath() {
    return finalOutputPath;
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
        } else if ("-e".equals(arg) || "".equals("--iteration")) {
          a++;
          String tmpArg = args[a];
          try {
            pageRankIterations = Long.parseLong(tmpArg);
          } catch (NumberFormatException e) {
            System.err.println("parsing pageRank threshold failed, value:" + tmpArg);
            printUsage();
            return 1;
          }
        } else if ("-g".equals(arg) || "--give-vertices-total-count".equals(arg)) {
          a++;
          String tmpArg = args[a];
          try {
            verticesTotalCount = Long.parseLong(tmpArg);
          } catch (NumberFormatException e) {
            System.err.println("parsing pageRank threshold failed, value:" + tmpArg);
            printUsage();
            return 1;
          }
        } else if ("-p".equals(arg) || "--input-splits-path".equals(arg)) {
          a++;
          inputSplitsPath = args[a];
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
      System.err.println("The must options not satisfied !!");
      printUsage();
      return 1;
    }

    LOGGER.info("start to run " + this.getClass().getName() + " with options:"
        + Arrays.toString(args));

    Configuration conf = getConf();
    Class<? extends TableInputFormat> tableInputFormat = TableInputFormat.class;
    String vertexTableName = args[startMustIdx];
    String edgeTableName = args[startMustIdx + 1];
    String outputBasePath = args[startMustIdx + 2];

    LOGGER.info(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY + "=" + vertexTableName);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, vertexTableName);

    LOGGER.info(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY + "=" + edgeTableName);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, edgeTableName);

    LOGGER.info("outputBasePath=" + outputBasePath);

    // collect total vertices count
    if (includeVeticesTotalCount && verticesTotalCount == -1L) {
      LOGGER.info("start to collect vertices total count");
      int retCode = 0;
      retCode = collectVeticesTotalCount(conf, vertexTableName);
      if (retCode != 0) {
        System.err.println("run vertices total count job failed, with retCode:" + retCode);
        return retCode;
      }
    } else if (includeVeticesTotalCount && verticesTotalCount != -1L) {
      System.err.println("can not use two options '-c' and '-g' in the same time");
      printUsage();
      return 1;
    }

    // user give the total count manually
    if (verticesTotalCount > 0) {
      conf.set(Constants.PAGE_RANK_VERTICES_TOTAL_COUNT_KEY, verticesTotalCount + "");
      LOGGER.info(Constants.PAGE_RANK_VERTICES_TOTAL_COUNT_KEY + "=" + verticesTotalCount);
    }

    // user give a inputSplitPath for customized TableInputFormat
    if (null != inputSplitsPath && !"".equals(inputSplitsPath)) {
      tableInputFormat = org.trend.hgraph.mapreduce.lib.input.TableInputFormat.class;
      conf.set(org.trend.hgraph.mapreduce.lib.input.TableInputFormat.INPUT_SPLIT_PAIRS_INPUT_PATH,
        inputSplitsPath);
    }

    // run pageRank
    boolean exit = false;
    boolean firstRun = true;
    Job job = null;
    boolean jobSucceed = false;
    long pageRankChangedCount = 0L;
    String inputPath = null;
    long iterations = 0L;
    while (!exit) {
      iterations++;
      LOGGER.info("start to run interation:" + iterations);
      if (firstRun) {
        firstRun = false;
        job = createInitialPageRankJob(conf, outputBasePath, tableInputFormat);
        inputPath = job.getConfiguration().get("mapred.output.dir");
      } else {
        job = createInterMediatePageRankJob(conf, inputPath, outputBasePath);
        inputPath = job.getConfiguration().get("mapred.output.dir");
      }
      jobSucceed = job.waitForCompletion(true);
      if (!jobSucceed) {
        LOGGER.error("run job:" + job.getJobName() + " failed at iteration(s):" + iterations);
        return 1;
      }
      pageRankChangedCount = getPageRankChangedCount(job);
      if (pageRankChangedCount <= pageRankThreshold) {
        exit = true;
        LOGGER.info("threshold reached, pageRankThreshold:" + pageRankThreshold +
          ", pageRankChangedCount:" + pageRankChangedCount + ", iteration(s):" + iterations);
      }

      if (pageRankIterations == iterations) {
        exit = true;
        LOGGER.info("iterations reached, iteration(s):" + iterations +
          ", pageRankChangedCount:" + pageRankChangedCount);
      }
    }
    // for test usage
    this.finalOutputPath = inputPath;

    if (importResults) {
      job = ImportPageRanks.createSubmittableJob(conf, inputPath);
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

  private static Job createInterMediatePageRankJob(Configuration conf, String inputPath, String outputBasePath)
      throws IOException {
    long timestamp = System.currentTimeMillis();
    Job job = null;
    String jobName = null;
    try {
      jobName = "CalculateIntermediatePageRank_" + timestamp;
      LOGGER.info("start to run job:" + jobName);
      job = new Job(conf, jobName);
      job.setJarByClass(Driver.class);

      Validate.notEmpty(inputPath, "inputPath shall always not be empty");
      LOGGER.info("inputPath=" + inputPath);

      // HBaseConfiguration.merge(job.getConfiguration(),
      // HBaseConfiguration.create(job.getConfiguration()));

      FileInputFormat.setInputPaths(job, new Path(inputPath));
      job.setMapperClass(CalculateIntermediatePageRankMapper.class);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setMapOutputKeyClass(BytesWritable.class);
      job.setMapOutputValueClass(DoubleWritable.class);

      job.setReducerClass(CalculatePageRankReducer.class);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(DoubleWritable.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      String outputPath = outputBasePath + "/" + timestamp;
      LOGGER.info("outputPath=" + outputPath);
      FileOutputFormat.setOutputPath(job, new Path(outputPath));

      Utils.setAuthenticationToken(job, LOGGER);

    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }
    return job;
  }

  private static Job createInitialPageRankJob(Configuration conf, String outputBasePath,
      Class<? extends TableInputFormat> tableInputFormat)
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
        BytesWritable.class, DoubleWritable.class, job, true, tableInputFormat);

      job.setReducerClass(CalculatePageRankReducer.class);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(DoubleWritable.class);
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

  private static int collectVeticesTotalCount(Configuration conf, String vertexTableName)
      throws IOException,
      InterruptedException, ClassNotFoundException {
    long totalCount = 1L;
    boolean success = false;
    Counter counter = null;
    String jobName = null;
    try {
      Job job = RowCounter.createSubmittableJob(conf, new String[] { vertexTableName });
      if (job == null) {
        System.err.println("job is null");
        return 1;
      }

      success = job.waitForCompletion(true);
      counter =
          job.getCounters().findCounter(
            "org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters", "ROWS");
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
            + " Usage: [-c | -g <total-count>] [-p <input-splits-path>] [-i] [-t <threshold>] [-e <iteration>] <vertex-table-name> <edge-table-name> <output-base-path>");
    System.err.println("Run pageRank on the HBase for pre-defined HGraph schema");
    System.err.println("  -h, --help: print usage");
    System.err.println("  -t, --threshold: pageRank threshold, default is 0");
    System.err.println("  -e, --iteration: pageRank iterations, default is unlimited");
    System.err.println("  -c, --vertices-total-count: include the all vertices total count");
    System.err.println("  -g, --give-vertices-total-count: user gives the all vertices total count manually");
    System.err.println("  -i, --import-result: import pageRank results to <vertex-table-name>, default is false");
    System.err.println("  -p, --input-splits-path: enabled customized TableInputFormat by given file path");
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
