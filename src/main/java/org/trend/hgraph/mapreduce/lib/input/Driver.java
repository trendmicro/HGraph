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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
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
 * A Drvier for running the InputSplit program.
 * @author scott_miao
 */
public class Driver extends Configured implements Tool {

  public Driver(Configuration conf) {
    super(conf);
  }

  private static Logger LOGGER = LoggerFactory.getLogger(Driver.class);

  private int mappersForOneRegion = CalculateInputSplitReducer.MAPPERS_FOR_ONE_REGION_DEFAULT_VALUE;
  private int bypassRowKeys = CalculateInputSplitMapper.Mapper.BY_PASS_KEYS_DEFAULT_VALUE;

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length == 0) {
      System.err.println("no any option specified !!");
      printUsage();
      return -1;
    }

    int mustStartIdx = -1;
    String cmd = null;
    for (int idx = 0; idx < args.length; idx++) {
      cmd = args[idx];
      if (cmd.startsWith("-")) {
        if (mustStartIdx > -1) {
          System.err.println("option order is incorrect !!");
          printUsage();
          return -1;
        }

        if (cmd.equals("-h") || cmd.equals("--help")) {
          printUsage();
          return 0;
        } else if (cmd.equals("-m") || cmd.equals("--mappers-for-one-region")) {
          idx++;
          cmd = args[idx];
          try {
            mappersForOneRegion = Integer.parseInt(cmd);
          } catch (NumberFormatException e) {
            System.err.println("-m shall be a numeric value, m:" + cmd);
            printUsage();
            return -1;
          }

        } else if (cmd.equals("-b") || cmd.equals("--bypass-rowkeys")) {
          idx++;
          cmd = args[idx];
          try {
            bypassRowKeys = Integer.parseInt(cmd);
          } catch (NumberFormatException e) {
            System.err.println("-b shall be a numeric value, b:" + cmd);
            printUsage();
            return -1;
          }

        } else {
          System.err.println("Not a defined option:" + cmd);
          printUsage();
          return 1;
        }
      } else {
        if (mustStartIdx < 0) {
          mustStartIdx = idx;
        }
      }
    }

    if (mustStartIdx + 2 != args.length) {
      System.err.println("The must options not satisfied !!");
      printUsage();
      return 1;
    }

    LOGGER.info("start to run " + this.getClass().getName() + " with options:"
        + Arrays.toString(args));

    String tableName = args[mustStartIdx];
    String outputPath = args[mustStartIdx + 1];
    Configuration conf = this.getConf();

    LOGGER.info(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY + "=" + tableName);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, tableName);

    LOGGER.info("outputPath=" + outputPath);

    conf.set(CalculateInputSplitMapper.Mapper.BY_PASS_KEYS, bypassRowKeys + "");
    conf.set(CalculateInputSplitReducer.MAPPERS_FOR_ONE_REGION, mappersForOneRegion + "");

    Job job = createInputSplitJob(conf, outputPath);
    boolean jobSucceed = job.waitForCompletion(true);

    String jobName = job.getJobName();
    if (jobSucceed) {
      LOGGER.info("job:" + jobName + " ran successfully !!");
    } else {
      LOGGER.error("job:" + jobName + " ran failed !!");
      return -1;
    }
    return 0;
  }

  private static Job createInputSplitJob(Configuration conf, String outputPath) throws IOException {
    String tableName = conf.get(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY);
    long timestamp = System.currentTimeMillis();
    Job job = null;
    String jobName = null;
    try {
      jobName = "InputSplit_" + timestamp;
      LOGGER.info("start to run job:" + jobName);

      job = new Job(conf, jobName);
      job.setJarByClass(Driver.class);
      Scan scan = new Scan();

      TableMapReduceUtil.initTableMapperJob(tableName, scan,
        CalculateInputSplitMapper.Mapper.class,
        Text.class, Text.class, job);
      job.setReducerClass(CalculateInputSplitReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      LOGGER.info("outputPath=" + outputPath);
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }
    return job;
  }

  private static void printUsage() {
    System.err.println(Driver.class.getSimpleName() + "Usage: [-m <n>] [-b <n>] <table-name> <output-path>");
    System.err.println("prepare the start/end rowkeys for HTable InputSplits for MR processes");
    System.err.println("  -h, --help: print usage");
    System.err.println("  -m,  --mappers-for-one-region: specify how many mappers for one region, default is 3");
    System.err.println("  -b,  --bypass-rowkeys: speficy how many rowkeys to bypass for one region, default is 1000");

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Driver tool = new Driver(conf);
    int code = ToolRunner.run(tool, args);
    System.exit(code);
  }

}
