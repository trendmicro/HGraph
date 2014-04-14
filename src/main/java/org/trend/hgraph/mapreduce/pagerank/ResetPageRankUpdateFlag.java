/**
 * 
 */
package org.trend.hgraph.mapreduce.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * A MR to set {@link Constants#PAGE_RANK_CQ_UPDATED_NAME} flag to 0
 * @author scott_miao
 */
public class ResetPageRankUpdateFlag extends Configured implements Tool {

  protected ResetPageRankUpdateFlag(Configuration conf) {
    super(conf);
  }

  private static class Mapper extends TableMapper<ImmutableBytesWritable, Put> {

    enum Counters {
      RESET_ROW_COUNT
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      byte[] rowkey = key.get();
      Put put = null;
      put = new Put(rowkey);
      put.add(Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME),
        Bytes.toBytes(Constants.PAGE_RANK_CQ_UPDATED_NAME
            + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + "String"),
        Bytes.toBytes("0"));
      context.write(key, put);
      context.getCounter(Counters.RESET_ROW_COUNT).increment(1L);
    }

  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length != 1) {
      System.err.println("option shall be 1 !!");
      printUsage();
      return -1;
    }

    String tableName = args[0];
    Job job = createSubmittableJob(this.getConf(), tableName);
    boolean success = job.waitForCompletion(true);
    return success ? 0 : -1;
  }

  public static Job createSubmittableJob(Configuration conf, String tableName) throws IOException {
    Job job = new Job(conf, "resetPageRankUpdateFlag_" + tableName);
    job.setJarByClass(ResetPageRankUpdateFlag.class);
    Scan scan = new Scan();
    TableMapReduceUtil.initTableMapperJob(tableName, scan, Mapper.class, null, null, job);
    TableMapReduceUtil.initTableReducerJob(tableName, null, job);
    job.setNumReduceTasks(0);
    return job;
  }

  private static void printUsage() {
    System.err.println(ResetPageRankUpdateFlag.class.getSimpleName() + "Usage: ");
    System.err.println("<vertex-table-name>");
  }

  public static final void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Tool tool = new ResetPageRankUpdateFlag(conf);
    int status = ToolRunner.run(tool, args);
    System.exit(status);
  }

}
