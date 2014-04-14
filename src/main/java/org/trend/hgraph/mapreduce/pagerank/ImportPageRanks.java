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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * Import pageRanks from <code>SeuquenceFile</code> into HBase. 
 * By key => rowKey, value => pageRank.
 * @author scott_miao
 */
public class ImportPageRanks extends Configured implements Tool {

  private static Logger LOGGER = LoggerFactory.getLogger(ImportPageRanks.class);

  private static class ImportPageRanksMapper extends
      Mapper<Text, DoubleWritable, Text, DoubleWritable> {

    private HTable vertexTable;

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object,
     * org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(Text key, DoubleWritable value, Context context)
        throws IOException, InterruptedException {
      Put put = null;
      String rowKey = Bytes.toString(key.getBytes()).trim();
      try {
        put = new Put(Bytes.toBytes(rowKey));
        // set rank value
        put.add(
          Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
          Bytes.toBytes(Constants.PAGE_RANK_CQ_NAME
              + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + "String"),
          Bytes.toBytes("" + value.get()));
        // set update flag to 1 (true)
        put.add(
          Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
          Bytes.toBytes(Constants.PAGE_RANK_CQ_UPDATED_NAME
              + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + "String"),
          Bytes.toBytes("1"));
        vertexTable.put(put);
      } catch (IOException e) {
        System.err.println("import pageRank failed !!");
        e.printStackTrace(System.err);
        throw e;
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      vertexTable.close();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      String vertexTableName =
          context.getConfiguration().get(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY);
      Validate.notEmpty(vertexTableName, "vertexTableName shall always not be empty");
      vertexTable = new HTable(context.getConfiguration(), vertexTableName);
    }

  }

  /**
   * Default constructor.
   */
  public ImportPageRanks() {
    super();
  }

  /**
   * Constructor for test.
   * @param conf
   */
  protected ImportPageRanks(Configuration conf) {
    super(conf);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length != 2) {
      System.err.println("shall pass only 2 options");
      printUsage();
      return 1;
    }

    String inputPath = args[0];
    String vertexTableName = args[1];
    LOGGER.info("pass two options:" + inputPath + ", " + vertexTableName);

    Configuration conf = getConf();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, vertexTableName);

    Job job = createSubmittableJob(conf, inputPath);
    String jobName = job.getJobName();
    LOGGER.info("start to run job:" + jobName);
    boolean succeed = job.waitForCompletion(true);
    if (!succeed) return 1;
    LOGGER.info("run job:" + jobName + " finished");
    return 0;
  }

  public static Job createSubmittableJob(Configuration conf, String inputPath) throws IOException {
    String vertexTableName = conf.get(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY);
    Validate.notEmpty(vertexTableName, "vertexTableName shall always not be empty");
    Validate.notEmpty(inputPath, "inputPath shall always not be empty");
    
    long timestamp = System.currentTimeMillis();
    Job job = null;
    String jobName = null;
    try {
      jobName = "ImportPageRanks_" + timestamp;
      LOGGER.info("start to run job:" + jobName);
      job = new Job(conf, jobName);
      job.setJarByClass(ImportPageRanks.class);

      LOGGER.info("inputPath=" + inputPath);
      LOGGER.info("vertexTableName=" + vertexTableName);

      FileInputFormat.setInputPaths(job, new Path(inputPath));
      job.setMapperClass(ImportPageRanksMapper.class);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(DoubleWritable.class);

      // only mapper
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setNumReduceTasks(0);

      Utils.setAuthenticationToken(job, LOGGER);
    } catch (IOException e) {
      LOGGER.error("run " + jobName + " failed", e);
      throw e;
    }
    return job;
  }

  private static void printUsage() {
    System.err.println(ImportPageRanks.class.getName() + " Usage: <input-path> <hbase-vertex-table>");
    System.err.println("Import the pageRank intermediate data into <hbase-vertex-table>");
  }

  /**
   * entry point.
   * @throws Exception
   * @see #printUsage()
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int retCode = ToolRunner.run(conf, new ImportPageRanks(), args);
    System.exit(retCode);
  }

}
