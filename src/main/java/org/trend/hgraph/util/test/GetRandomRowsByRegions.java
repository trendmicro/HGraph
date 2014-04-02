/**
 * 
 */
package org.trend.hgraph.util.test;

import java.io.IOException;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple MR job for getting sample data for performance test. Get sample data from every region
 * by a given <code>HTable</code>.
 * @author scott_miao
 */
public class GetRandomRowsByRegions extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(GetRandomRowsByRegions.class);

  protected GetRandomRowsByRegions(Configuration conf) {
    super(conf);
  }

  public static class Mapper extends TableMapper<Text, NullWritable> {


    public static final String BYPASS_ROW_SIZE_NAME = "hgraph.util.bypass.row.size";
    public static final String TARGET_SAMPLE_SIZE_NAME = "hgraph.util.target.sample.size";

    public static final int BYPASS_ROW_SIZE_DEFAULT = 1000;
    public static final int TARGET_SAMPLE_SIZE_DEFAULT = 1000;

    enum Counters {
      ROWS, COLLECTEED_ROWS
    }

    private long count = 0L;

    private int bypassRowSize = 0;
    private int tarSampleSize = 0;
    private int curSampleSize = 0;

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      count++;
      context.getCounter(Counters.ROWS).increment(1L);

      if (count % bypassRowSize == 0 && curSampleSize <= tarSampleSize) {
        context.write(new Text(key.get()), NullWritable.get());
        curSampleSize++;
        context.getCounter(Counters.COLLECTEED_ROWS).increment(1L);
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      bypassRowSize = conf.getInt(BYPASS_ROW_SIZE_NAME, BYPASS_ROW_SIZE_DEFAULT);
      tarSampleSize = conf.getInt(TARGET_SAMPLE_SIZE_NAME, TARGET_SAMPLE_SIZE_DEFAULT);
    }

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length < 2) {
      System.err.println("options shall not be empty or not equal to 2");
      printUsage();
      return -1;
    }
    String cmd = null;
    int mustStartIdx = -1;
    int bypassRows = Mapper.BYPASS_ROW_SIZE_DEFAULT;
    int tarSampleSize = Mapper.TARGET_SAMPLE_SIZE_DEFAULT;
    for (int a = 0; a < args.length; a++) {
      cmd = args[a];
      if (cmd.startsWith("-")) {
        if (mustStartIdx > -1) {
          System.err.println("The option order is incorrect !!");
          printUsage();
          return -1;
        }
        
        if ("-b".equals(cmd) || "-bypass-row-size".equals(cmd)) {
          a++;
          cmd = args[a];
          try {
            bypassRows = Integer.parseInt(cmd);
          } catch (NumberFormatException e) {
            System.err.println("parsing bypass rows failed, value:" + cmd);
            printUsage();
            return 1;
          }
        } else if ("-t".equals(cmd) || "-target-sample-size".equals(cmd)) {
          a++;
          cmd = args[a];
          try {
            tarSampleSize = Integer.parseInt(cmd);
          } catch (NumberFormatException e) {
            System.err.println("parsing target sample size failed, value:" + tarSampleSize);
            printUsage();
            return 1;
          }
        } else {
          System.err.println("option:" + cmd + " is undefined !!");
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

    LOGGER.info("tableName=" + tableName);
    LOGGER.info("outputPath=" + outputPath);

    Configuration conf = this.getConf();
    conf.setInt(Mapper.BYPASS_ROW_SIZE_NAME, bypassRows);
    conf.setInt(Mapper.TARGET_SAMPLE_SIZE_NAME, tarSampleSize);
    Job job = createSubmittableJob(conf, tableName, outputPath);
    Boolean success = job.waitForCompletion(true);
    if (!success) {
      System.err.println("run job:" + job.getJobName() + " failed");
      return -1;
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
      jobName = GetRandomRowsByRegions.class.getSimpleName() + "_" + timestamp;
      LOGGER.info("start to run job:" + jobName);
      job = new Job(conf, jobName);
      job.setJarByClass(GetRandomRowsByRegions.class);

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
    System.err.println(GetRandomRowsByRegions.class.getSimpleName()
        + " Usage: [-b <numeric>] [-t <numeric>] <table-name> <output-path>");
    System.err.println(
      "  -b, -bypass-row-size: bypass how many row(s) to pick one sample, default:"
            + Mapper.BYPASS_ROW_SIZE_DEFAULT);
    System.err.println(
      "  -t, -target-sample-size: how many target sample(s) to collect, default:"
        + Mapper.TARGET_SAMPLE_SIZE_DEFAULT);
  }

  public static void main(String[] agrs) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Tool tool = new GetRandomRowsByRegions(conf);
    int status = ToolRunner.run(tool, agrs);
    System.exit(status);
  }

}
